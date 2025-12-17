use bigdecimal::{BigDecimal, Zero};
use crate::db::queries;
use crate::models::{MatchResult1201, TempSummary};
use chrono::Utc;
use indexmap::IndexSet;
use sqlx::PgPool;
use std::collections::HashMap;

/// 匹配服务 (完全复刻 Java batchMatchTempStrategy)
pub struct MatcherService {
    pool: PgPool,
}

impl MatcherService {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// 批量临时策略匹配 (完全复刻 Java batchMatchTempStrategy)
    pub async fn batch_match_temp_strategy(&self, bill_ids: &[i64]) -> Result<(), Box<dyn std::error::Error>> {
        for &bill_id in bill_ids {
            // 1. 查询单据主表
            let bill = queries::get_bill(&self.pool, bill_id).await?;
            let Some(bill) = bill else {
                tracing::warn!("Bill {} not found, skipping", bill_id);
                continue;
            };

            // 2. 查询单据明细
            let bill_items = queries::list_bill_items(&self.pool, bill_id).await?;
            if bill_items.is_empty() {
                tracing::info!("Bill {} has no items, skipping", bill_id);
                continue;
            }

            // 3. 预统计阶段: 收集每个 SKU 的候选信息
            let mut summaries: Vec<TempSummary> = Vec::with_capacity(bill_items.len());
            for (idx, bi) in bill_items.iter().enumerate() {
                let remaining = bill_items.len() - idx - 1;
                tracing::info!("统计单据 {} 商品编码 {} 剩余未处理 {}", bill_id, bi.fspbm, remaining);

                let stat = queries::stat_for_product(
                    &self.pool,
                    &bill.fbuyertaxno,
                    &bill.fsalertaxno,
                    &bi.fspbm,
                )
                .await?;
                summaries.push(TempSummary {
                    fspbm: bi.fspbm.clone(),
                    item_count: stat.cnt,
                    total_amount: stat.sum_amount,
                });
            }

            // 4. 按稀缺度排序 (item_count ASC, total_amount ASC)
            summaries.sort_by(|a, b| {
                a.item_count
                    .cmp(&b.item_count)
                    .then_with(|| a.total_amount.cmp(&b.total_amount))
            });

            // 5. 重新排列 bill_items 按稀缺度顺序
            let ordered_items: Vec<_> = summaries
                .iter()
                .filter_map(|s| bill_items.iter().find(|bi| bi.fspbm == s.fspbm).cloned())
                .collect();

            // 6. 初始化状态
            let mut preferred_invoices: IndexSet<i64> = IndexSet::new(); // 保序去重
            let mut matched_by_product: HashMap<String, BigDecimal> = HashMap::new();

            // 进度统计
            let total_skus = ordered_items.len();
            let mut matched_count = 0;

            tracing::info!("跳过稀缺度预统计，直接开始按需匹配...");
            tracing::info!("处理销购方组: {} 个SKU", total_skus);

            // 7. 匹配阶段
            for (idx, bi) in ordered_items.iter().enumerate() {
                let code = &bi.fspbm;
                let target_abs = bi.famount.abs();
                let already = matched_by_product.get(code).cloned().unwrap_or_else(BigDecimal::zero);
                let mut remaining = &target_abs - &already;

                if remaining <= BigDecimal::zero() {
                    matched_count += 1; // 跳过时计数
                    continue; // 已匹配足额
                }

                // 7.1 构建候选集合 (去重、保序)
                let mut source = Vec::new();
                let mut seen_item_ids: IndexSet<i64> = IndexSet::new();

                // 第一层: 从 preferred_invoices 查询 (分块处理)
                if !preferred_invoices.is_empty() {
                    let ids: Vec<i64> = preferred_invoices.iter().copied().collect();
                    for chunk in ids.chunks(1000) {
                        let pref = queries::match_on_invoices(
                            &self.pool,
                            &bill.fbuyertaxno,
                            &bill.fsalertaxno,
                            code,
                            chunk,
                        )
                        .await?;
                        for mi in pref {
                            if seen_item_ids.insert(mi.item_id) {
                                source.push(mi);
                            }
                        }
                    }
                }

                // 第二层: 从全量候选查询
                let general = queries::match_by_tax_and_product(
                    &self.pool,
                    &bill.fbuyertaxno,
                    &bill.fsalertaxno,
                    code,
                )
                .await?;
                for mi in general {
                    if seen_item_ids.insert(mi.item_id) {
                        source.push(mi);
                    }
                }

                // 7.2 顺序遍历填充
                let mut batch: Vec<MatchResult1201> = Vec::new();
                remaining = &target_abs - &matched_by_product.get(code).cloned().unwrap_or_else(BigDecimal::zero);

                for mi in &source {
                    if remaining <= BigDecimal::zero() {
                        break;
                    }

                    let use_amount = if &mi.amount >= &remaining {
                        remaining.clone()
                    } else {
                        mi.amount.clone()
                    };

                    if use_amount <= BigDecimal::zero() {
                        continue;
                    }

                    let rec = MatchResult1201 {
                        fbillid: bill_id,
                        fbuyertaxno: bill.fbuyertaxno.clone(),
                        fsalertaxno: bill.fsalertaxno.clone(),
                        fspbm: mi.product_code.clone(),
                        finvoiceid: mi.invoice_id,
                        finvoiceitemid: mi.item_id,
                        fnum: mi.quantity.clone(),
                        fbillamount: bi.famount.clone(),
                        finvoiceamount: mi.amount.clone(),
                        fmatchamount: use_amount.clone(),
                        fbillunitprice: bi.funitprice.clone(),
                        fbillqty: bi.fnum.clone(),
                        finvoiceunitprice: mi.unit_price.clone(),
                        finvoiceqty: Some(mi.quantity.clone()),
                        fmatchtime: Utc::now(),
                    };

                    batch.push(rec);
                    preferred_invoices.insert(mi.invoice_id);
                    let entry = matched_by_product.entry(code.clone()).or_insert_with(BigDecimal::zero);
                    *entry = &*entry + &use_amount;
                    remaining = &remaining - &use_amount;
                }

                // 7.3 批量插入 (每1000条分块)
                if !batch.is_empty() {
                    for chunk in batch.chunks(1000) {
                        queries::insert_batch(&self.pool, chunk).await?;
                    }
                    matched_count += 1; // 匹配成功时计数
                }

                // 7.4 进度日志 (每100个SKU或第一个SKU)
                let current_idx = idx + 1;
                if current_idx % 100 == 0 || current_idx == 1 {
                    let progress_msg = format!(
                        "SKU进度: {}/{}, 已匹配: {}, 已用发票: {}",
                        current_idx, total_skus, matched_count, preferred_invoices.len()
                    );
                    tracing::info!("{}", progress_msg);
                    println!("{}", progress_msg); // 同时输出到控制台
                }
            }

            // 最终统计
            tracing::info!(
                "匹配完成: 总SKU: {}, 已匹配: {}, 已用发票: {}",
                total_skus, matched_count, preferred_invoices.len()
            );
            tracing::info!("Bill {} matched successfully", bill_id);
        }

        Ok(())
    }
}
