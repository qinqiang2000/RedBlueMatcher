use bigdecimal::{BigDecimal, Zero};
use crate::db::{queries, queries_invoice_centric};
use futures::{stream, StreamExt};
use crate::models::{
    InvoiceScoringContext, MatchingRequirements, MatchResult1201, MatchStats,
    MatchBillItem1201,
};
use chrono::Utc;
use sqlx::PgPool;
use std::collections::HashMap;

/// Invoice-Centric匹配服务
/// 核心改进：以发票为中心，优先选择覆盖多SKU的发票，减少已用发票数量
pub struct InvoiceCentricMatcher {
    pool: PgPool,
}

impl InvoiceCentricMatcher {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// 批量匹配入口
    pub async fn batch_match(&self, bill_ids: &[i64]) -> Result<Vec<MatchStats>, Box<dyn std::error::Error>> {
        self.batch_match_with_limit(bill_ids, None).await
    }

    /// 批量匹配入口（带SKU数量限制，用于测试）
    pub async fn batch_match_with_limit(&self, bill_ids: &[i64], max_skus: Option<usize>) -> Result<Vec<MatchStats>, Box<dyn std::error::Error>> {
        let mut all_stats = Vec::new();

        for &bill_id in bill_ids {
            match self.match_single_bill(bill_id, max_skus).await {
                Ok(stats) => {
                    all_stats.push(stats);
                }
                Err(e) => {
                    tracing::error!("Bill {} matching failed: {}", bill_id, e);
                    return Err(e);
                }
            }
        }

        Ok(all_stats)
    }

    /// 单个单据匹配 - Invoice-Centric算法核心
    async fn match_single_bill(&self, bill_id: i64, max_skus: Option<usize>) -> Result<MatchStats, Box<dyn std::error::Error>> {
        // Phase 1: 获取单据信息
        let bill = queries::get_bill(&self.pool, bill_id).await?;
        let Some(bill) = bill else {
            return Err(format!("Bill {} not found", bill_id).into());
        };

        let mut bill_items = queries::list_bill_items(&self.pool, bill_id).await?;
        if bill_items.is_empty() {
            return Ok(MatchStats {
                bill_id,
                total_skus: 0,
                matched_skus: 0,
                invoices_used: 0,
                total_matched_amount: BigDecimal::zero(),
                total_candidate_invoices: 0,
                output_file: None,
            });
        }

        // 应用 max_skus 限制（用于测试）
        if let Some(limit) = max_skus {
            if bill_items.len() > limit {
                bill_items.truncate(limit);
                tracing::warn!("[Invoice-Centric] Bill {}: 限制到前 {} 个SKU (测试模式)", bill_id, limit);
            }
        }

        // Phase 2: 构建需求
        let mut requirements = MatchingRequirements::from_bill_items(&bill_items);
        let sku_list = requirements.get_required_skus();
        let total_skus = sku_list.len();

        tracing::info!(
            "[Invoice-Centric] Bill {}: 开始匹配, {} 个SKU{}",
            bill_id, total_skus,
            if max_skus.is_some() { " (测试模式)" } else { "" }
        );

        // Phase 3: 分步分批查询候选发票明细 (优化版)
        // 3.1 获取所有候选发票ID
        let all_fids = queries_invoice_centric::query_candidate_invoice_ids(
            &self.pool,
            &bill.fbuyertaxno,
            &bill.fsalertaxno,
        )
        .await?;
        
        // 3.2 并发分批拉取明细
        let mut all_items = Vec::new();
const BATCH_SIZE: usize = 500;
const CONCURRENCY: usize = 10;

// Create owned chunks to avoid lifetime issues with async stream
let chunks: Vec<Vec<i64>> = all_fids.chunks(BATCH_SIZE).map(|c| c.to_vec()).collect();

let mut stream = stream::iter(chunks)
    .map(|chunk_vec| {
        let pool = self.pool.clone();
        let sku_list = sku_list.clone();
        async move {
            queries_invoice_centric::query_items_by_fids_and_skus(
                &pool,
                &chunk_vec,
                &sku_list,
            )
            .await
        }
    })
    .buffer_unordered(CONCURRENCY);

while let Some(result) = stream.next().await {
    let batch_items = result?;
    all_items.extend(batch_items);
}

        let total_candidate_invoices = all_fids.len();

        tracing::info!(
            "[Invoice-Centric] Bill {}: 查询完成, {} 张候选发票, {} 条明细",
            bill_id, total_candidate_invoices, all_items.len()
        );

        // Phase 4: 构建评分上下文
        let mut scoring_context = InvoiceScoringContext::from_items(all_items);

        // Phase 5: 贪心选择 - 迭代选择最优发票
        let mut results: Vec<MatchResult1201> = Vec::new();
        let mut total_matched_amount = BigDecimal::zero();

        // 构建bill_item的快速查找表
        let bill_item_map: HashMap<String, &MatchBillItem1201> = bill_items
            .iter()
            .map(|bi| (bi.fspbm.clone(), bi))
            .collect();

        let mut iteration = 0;
        
        // 5.0 初始化惰性堆 (只需做一次)
        scoring_context.init_heap(&requirements);
        tracing::info!("[Invoice-Centric] Bill {}: 惰性堆初始化完成", bill_id);

        while !requirements.is_satisfied() {
            iteration += 1;

            // 找当前最优发票 (Lazy Greedy)
            let best_invoice_id = scoring_context.find_best_invoice_lazy(&requirements);

            let Some(invoice_id) = best_invoice_id else {
                tracing::warn!(
                    "[Invoice-Centric] Bill {}: 没有更多可用发票, 剩余 {} 个SKU未满足",
                    bill_id, requirements.remaining_sku_count()
                );
                break;
            };

            // 获取该发票当前可用的明细（剩余金额 > 0）
            let available_items = scoring_context.get_available_items(invoice_id);

            // 匹配该发票上所有可用的SKU
            let items_count = available_items.len();
            let mut matched_in_invoice = 0;

            for item in available_items {
                let required = match requirements.get_remaining(&item.product_code) {
                    Some(r) if *r > BigDecimal::zero() => r.clone(),
                    _ => continue,
                };

                let match_amount = if item.remaining_amount < required {
                    item.remaining_amount.clone()
                } else {
                    required.clone()
                };

                if match_amount <= BigDecimal::zero() {
                    continue;
                }

                // 消费明细（更新 remaining_amount）
                scoring_context.consume_item(invoice_id, &item.product_code, &match_amount);

                // 查找对应的bill_item以获取额外信息
                let bi = bill_item_map.get(&item.product_code);

                let rec = MatchResult1201 {
                    fbillid: bill_id,
                    fbuyertaxno: bill.fbuyertaxno.clone(),
                    fsalertaxno: bill.fsalertaxno.clone(),
                    fspbm: item.product_code.clone(),
                    finvoiceid: item.invoice_id,
                    finvoiceitemid: item.item_id,
                    fnum: item.quantity.clone(),
                    fbillamount: bi.map(|b| b.famount.clone()).unwrap_or_else(BigDecimal::zero),
                    finvoiceamount: item.original_amount.clone(),
                    fmatchamount: match_amount.clone(),
                    fbillunitprice: bi.and_then(|b| b.funitprice.clone()),
                    fbillqty: bi.and_then(|b| b.fnum.clone()),
                    finvoiceunitprice: item.unit_price.clone(),
                    finvoiceqty: Some(item.quantity.clone()),
                    fmatchtime: Utc::now(),
                };

                results.push(rec);
                matched_in_invoice += 1;
                total_matched_amount += &match_amount;
                requirements.reduce(&item.product_code, &match_amount);
            }

            if iteration == 1 || iteration % 100 == 0 {
                tracing::debug!("[Invoice-Centric] Bill {}: 迭代 {}, 发票 {} 有 {} 个可用明细, 匹配了 {} 个, 累计results: {}",
                    bill_id, iteration, invoice_id, items_count, matched_in_invoice, results.len());
            }

            // 注意：不再标记整个发票为已使用，允许后续迭代继续使用该发票的剩余明细

            // 进度日志（每10轮或第一轮）
            if iteration % 10 == 0 || iteration == 1 {
                tracing::info!(
                    "[Invoice-Centric] Bill {}: 迭代 {}, 已用发票: {}, 剩余SKU: {}",
                    bill_id, iteration, scoring_context.used_count(), requirements.remaining_sku_count()
                );
            }
        }

        // Phase 6: 批量插入结果
        let matched_skus = total_skus - requirements.remaining_sku_count();
        let invoices_used = scoring_context.used_count();

        // 记录未匹配的SKU详情
        if requirements.remaining_sku_count() > 0 {
            let remaining_details = requirements.get_remaining_details();
            let mut total_remaining_amount = BigDecimal::zero();
            let mut details_str = String::new();

            for (sku, amount) in remaining_details {
                total_remaining_amount += &amount;
                details_str.push_str(&format!("{} ({}), ", sku, amount));
            }

            tracing::warn!(
                "[Invoice-Centric] Bill {}: ⚠️ 有 {} 个SKU未完全匹配! 总缺口金额: {}. 详情: [{}]",
                bill_id, requirements.remaining_sku_count(), total_remaining_amount, details_str.trim_end_matches(", ")
            );
        }

        tracing::info!("[Invoice-Centric] Bill {}: 准备导出 {} 条匹配结果", bill_id, results.len());

        if !results.is_empty() {
            // 导出到 CSV 文件（绕过数据库插入卡死问题）
            // 确保 logs 目录存在
            let logs_dir = std::path::Path::new("logs");
            if !logs_dir.exists() {
                let _ = std::fs::create_dir_all(logs_dir);
            }

            let csv_filename = format!("logs/match_results_{}.csv", bill_id);
            let csv_path = std::path::Path::new(&csv_filename).to_path_buf();

            tracing::info!("[Invoice-Centric] Bill {}: 导出到 CSV 文件: {} ({} 条记录)",
                bill_id, csv_filename, results.len());

            // 直接同步写入，避免 clone 开销
            match queries::export_to_csv(&results, &csv_path) {
                Ok(()) => {
                    tracing::info!("[Invoice-Centric] Bill {}: ✓ CSV 导出成功: {}", bill_id, csv_filename);
                    tracing::info!("[Invoice-Centric] Bill {}: 请使用导入脚本:", bill_id);
                    tracing::info!("  ./scripts/import_csv_to_db.sh --csv {} --env dev", csv_filename);
                }
                Err(e) => {
                    tracing::error!("[Invoice-Centric] Bill {}: ✗ CSV 导出失败: {:?}", bill_id, e);
                    return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())));
                }
            }
        } else {
            tracing::warn!("[Invoice-Centric] Bill {}: ⚠️ results 为空，没有数据导出!", bill_id);
        }

        let stats = MatchStats {
            bill_id,
            total_skus,
            matched_skus,
            invoices_used,
            total_matched_amount,
            total_candidate_invoices,
            // 记录生成的 CSV 文件名，供外部脚本使用
            output_file: if !results.is_empty() {
                Some(format!("logs/match_results_{}.csv", bill_id))
            } else {
                None
            },
        };

        tracing::info!(
            "[Invoice-Centric] Bill {}: 匹配完成 - SKU: {}/{}, 已用发票: {} (候选: {})",
            bill_id, matched_skus, total_skus, invoices_used, total_candidate_invoices
        );

        Ok(stats)
    }
}
