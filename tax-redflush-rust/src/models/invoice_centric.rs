use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use std::collections::{HashMap, HashSet};

/// 发票覆盖度统计 - 用于查询结果
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct InvoiceCoverage {
    pub invoice_id: i64,
    pub sku_coverage_count: i64,           // 覆盖的SKU数量
    pub total_coverage_amount: BigDecimal, // 可匹配总金额
}

/// 发票明细项 - 用于Invoice-Centric算法
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct InvoiceItemDetail {
    pub invoice_id: i64,
    pub item_id: i64,
    pub product_code: String,
    pub quantity: BigDecimal,
    pub amount: BigDecimal,
    pub unit_price: Option<BigDecimal>,
}

/// 发票明细状态 - 追踪每个明细的剩余可用金额
#[derive(Debug, Clone)]
pub struct InvoiceItemState {
    pub invoice_id: i64,
    pub item_id: i64,
    pub product_code: String,
    pub quantity: BigDecimal,
    pub original_amount: BigDecimal,
    pub remaining_amount: BigDecimal,  // 剩余可用金额
    pub unit_price: Option<BigDecimal>,
}

/// 发票及其所有明细
#[derive(Debug, Clone)]
pub struct InvoiceWithItems {
    pub invoice_id: i64,
    pub items: Vec<InvoiceItemDetail>,
}

impl InvoiceWithItems {
    pub fn new(invoice_id: i64) -> Self {
        Self {
            invoice_id,
            items: Vec::new(),
        }
    }

    pub fn add_item(&mut self, item: InvoiceItemDetail) {
        self.items.push(item);
    }

    /// 计算该发票对当前需求的覆盖评分
    /// 返回 (覆盖的SKU数量, 可匹配总金额)
    pub fn calculate_coverage(&self, requirements: &MatchingRequirements) -> (i64, BigDecimal) {
        let mut sku_count = 0i64;
        let mut amount_sum = BigDecimal::from(0);

        for item in &self.items {
            if let Some(required) = requirements.get_remaining(&item.product_code) {
                if *required > BigDecimal::from(0) && item.amount > BigDecimal::from(0) {
                    sku_count += 1;
                    let available = if item.amount < *required {
                        item.amount.clone()
                    } else {
                        required.clone()
                    };
                    amount_sum += available;
                }
            }
        }

        (sku_count, amount_sum)
    }
}

/// 需求跟踪器 - 跟踪每个SKU的剩余需求金额
#[derive(Debug, Clone)]
pub struct MatchingRequirements {
    requirements: HashMap<String, BigDecimal>,
}

impl MatchingRequirements {
    pub fn new() -> Self {
        Self {
            requirements: HashMap::new(),
        }
    }

    /// 从单据明细构建需求
    pub fn from_bill_items(bill_items: &[crate::models::MatchBillItem1201]) -> Self {
        let mut requirements = HashMap::new();
        for item in bill_items {
            let amount = item.famount.abs();
            *requirements.entry(item.fspbm.clone()).or_insert_with(|| BigDecimal::from(0)) += amount;
        }
        Self { requirements }
    }

    /// 获取所有需要的SKU列表
    pub fn get_required_skus(&self) -> Vec<String> {
        self.requirements.keys().cloned().collect()
    }

    /// 获取某SKU的剩余需求金额
    pub fn get_remaining(&self, sku: &str) -> Option<&BigDecimal> {
        self.requirements.get(sku)
    }

    /// 扣减某SKU的需求金额
    pub fn reduce(&mut self, sku: &str, amount: &BigDecimal) {
        if let Some(remaining) = self.requirements.get_mut(sku) {
            *remaining = &*remaining - amount;
            if *remaining <= BigDecimal::from(0) {
                self.requirements.remove(sku);
            }
        }
    }

    /// 检查是否所有需求都已满足
    pub fn is_satisfied(&self) -> bool {
        self.requirements.is_empty()
    }

    /// 获取剩余未满足的SKU数量
    pub fn remaining_sku_count(&self) -> usize {
        self.requirements.len()
    }
}

impl Default for MatchingRequirements {
    fn default() -> Self {
        Self::new()
    }
}

/// 发票评分上下文 - 管理所有候选发票并支持明细级复用
#[derive(Debug)]
pub struct InvoiceScoringContext {
    /// 发票ID -> 明细状态列表（可变，用于扣减）
    invoices: HashMap<i64, Vec<InvoiceItemState>>,
    /// 倒排索引：SKU -> 拥有该SKU的发票ID列表
    sku_invoice_index: HashMap<String, HashSet<i64>>,
    /// 已使用过的发票（用于统计，不影响复用）
    used_invoices: HashSet<i64>,
}

impl InvoiceScoringContext {
    pub fn new() -> Self {
        Self {
            invoices: HashMap::new(),
            sku_invoice_index: HashMap::new(),
            used_invoices: HashSet::new(),
        }
    }

    /// 从发票明细列表构建上下文，同时创建倒排索引
    pub fn from_items(items: Vec<InvoiceItemDetail>) -> Self {
        let mut invoices: HashMap<i64, Vec<InvoiceItemState>> = HashMap::new();
        let mut sku_invoice_index: HashMap<String, HashSet<i64>> = HashMap::new();

        for item in items {
            let state = InvoiceItemState {
                invoice_id: item.invoice_id,
                item_id: item.item_id,
                product_code: item.product_code.clone(),
                quantity: item.quantity,
                original_amount: item.amount.clone(),
                remaining_amount: item.amount,  // 初始时剩余金额 = 原始金额
                unit_price: item.unit_price,
            };

            // 更新倒排索引
            sku_invoice_index
                .entry(state.product_code.clone())
                .or_insert_with(HashSet::new)
                .insert(state.invoice_id);

            // 添加到发票明细列表
            invoices
                .entry(state.invoice_id)
                .or_insert_with(Vec::new)
                .push(state);
        }

        Self {
            invoices,
            sku_invoice_index,
            used_invoices: HashSet::new(),
        }
    }

    /// 查找最优发票 - 只搜索有当前需求SKU的发票
    pub fn find_best_invoice(&self, requirements: &MatchingRequirements) -> Option<i64> {
        // 收集候选发票（只查有需求SKU的）
        let mut candidates: HashSet<i64> = HashSet::new();
        for sku in requirements.get_required_skus() {
            if let Some(inv_ids) = self.sku_invoice_index.get(&sku) {
                candidates.extend(inv_ids);
            }
        }

        let mut best: Option<(i64, i64, BigDecimal)> = None;
        for invoice_id in candidates {
            let (sku_count, amount) = self.calculate_coverage(invoice_id, requirements);
            if sku_count == 0 {
                continue;
            }

            let is_better = match &best {
                None => true,
                Some((_, best_sku, best_amt)) => {
                    amount > *best_amt || (amount == *best_amt && sku_count > *best_sku)
                }
            };

            if is_better {
                best = Some((invoice_id, sku_count, amount));
            }
        }

        best.map(|(id, _, _)| id)
    }

    /// 计算覆盖度（基于剩余金额）
    fn calculate_coverage(&self, invoice_id: i64, requirements: &MatchingRequirements) -> (i64, BigDecimal) {
        let items = match self.invoices.get(&invoice_id) {
            Some(i) => i,
            None => return (0, BigDecimal::from(0)),
        };

        let mut sku_count = 0i64;
        let mut amount_sum = BigDecimal::from(0);

        for item in items {
            if item.remaining_amount <= BigDecimal::from(0) {
                continue;
            }

            if let Some(required) = requirements.get_remaining(&item.product_code) {
                if *required > BigDecimal::from(0) {
                    sku_count += 1;
                    let available = if item.remaining_amount < *required {
                        item.remaining_amount.clone()
                    } else {
                        required.clone()
                    };
                    amount_sum += available;
                }
            }
        }

        (sku_count, amount_sum)
    }

    /// 消费明细金额（不标记整个发票为已使用）
    pub fn consume_item(&mut self, invoice_id: i64, product_code: &str, amount: &BigDecimal) -> Option<InvoiceItemState> {
        self.used_invoices.insert(invoice_id);  // 记录使用过

        if let Some(items) = self.invoices.get_mut(&invoice_id) {
            for item in items.iter_mut() {
                if item.product_code == product_code && item.remaining_amount > BigDecimal::from(0) {
                    let consumed = if *amount < item.remaining_amount {
                        amount.clone()
                    } else {
                        item.remaining_amount.clone()
                    };

                    item.remaining_amount -= &consumed;
                    return Some(item.clone());
                }
            }
        }

        None
    }

    /// 获取发票当前可用的明细（remaining > 0）
    pub fn get_available_items(&self, invoice_id: i64) -> Vec<InvoiceItemState> {
        self.invoices
            .get(&invoice_id)
            .map(|items| {
                items
                    .iter()
                    .filter(|i| i.remaining_amount > BigDecimal::from(0))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// 获取已使用的发票数量
    pub fn used_count(&self) -> usize {
        self.used_invoices.len()
    }

    /// 获取总候选发票数量
    pub fn total_count(&self) -> usize {
        self.invoices.len()
    }
}

impl Default for InvoiceScoringContext {
    fn default() -> Self {
        Self::new()
    }
}

/// 匹配统计信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchStats {
    pub bill_id: i64,
    pub total_skus: usize,
    pub matched_skus: usize,
    pub invoices_used: usize,
    pub total_matched_amount: BigDecimal,
    pub total_candidate_invoices: usize,
}
