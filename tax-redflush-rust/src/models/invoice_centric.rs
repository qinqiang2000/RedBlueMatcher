use bigdecimal::{BigDecimal, ToPrimitive};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

/// 发票评分（用于堆排序）
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvoiceScore {
    pub invoice_id: i64,
    pub score: i64,      // 整数化评分 (amount * 100 + bonus)
    pub sku_count: i64,  // 覆盖SKU数量 (第二优先级)
}

impl Ord for InvoiceScore {
    fn cmp(&self, other: &Self) -> Ordering {
        // 先按分数比较，分数相同按 SKU 数量比较
        self.score
            .cmp(&other.score)
            .then_with(|| self.sku_count.cmp(&other.sku_count))
    }
}

impl PartialOrd for InvoiceScore {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

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
            let sku = item.fspbm.trim();
            if sku.is_empty() {
                continue;
            }
            let amount = item.famount.abs();
            *requirements.entry(sku.to_string()).or_insert_with(|| BigDecimal::from(0)) += amount;
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

    /// 获取剩余未满足的SKU详情 (SKU, Amount)
    pub fn get_remaining_details(&self) -> Vec<(String, BigDecimal)> {
        self.requirements
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
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
    /// SKU 全局频率表 (用于计算稀缺性)
    sku_frequency_map: HashMap<String, i64>,
    /// 已使用过的发票（用于统计，不影响复用）
    used_invoices: HashSet<i64>,
    /// 惰性堆 (Lazy Heap) - 缓存发票评分
    heap: BinaryHeap<InvoiceScore>,
    // 对发票评分的缓存检查机制 (Lazy Check 不需要复杂版本号，直接重算对比即可，
    // 但为了极致性能，我们可以记录上次计算时的 remaining_sku_count 或类似标记，
    // 这里简化逻辑：Pop出来 -> Re-calculate -> 比较 -> If dropped, push back)
}

impl InvoiceScoringContext {
    pub fn new() -> Self {
        Self {
            invoices: HashMap::new(),
            sku_invoice_index: HashMap::new(),
            sku_frequency_map: HashMap::new(),
            used_invoices: HashSet::new(),
            heap: BinaryHeap::new(),
        }
    }

    /// 从发票明细列表构建上下文，同时创建倒排索引和频率表
    pub fn from_items(items: Vec<InvoiceItemDetail>) -> Self {
        let mut invoices: HashMap<i64, Vec<InvoiceItemState>> = HashMap::new();
        let mut sku_invoice_index: HashMap<String, HashSet<i64>> = HashMap::new();
        let mut sku_frequency_map: HashMap<String, i64> = HashMap::new();

        for item in items {
            let sku = item.product_code.trim();
            if sku.is_empty() {
                continue;
            }

            let state = InvoiceItemState {
                invoice_id: item.invoice_id,
                item_id: item.item_id,
                product_code: sku.to_string(),
                quantity: item.quantity,
                original_amount: item.amount.clone(),
                remaining_amount: item.amount,  // 初始时剩余金额 = 原始金额
                unit_price: item.unit_price,
            };

            // 更新倒排索引
            if sku_invoice_index
                .entry(state.product_code.clone())
                .or_insert_with(HashSet::new)
                .insert(state.invoice_id) {
                    // 仅当是新发票包含此SKU时，增加频率计数
                    *sku_frequency_map.entry(state.product_code.clone()).or_insert(0) += 1;
                }

            // 添加到发票明细列表
            invoices
                .entry(state.invoice_id)
                .or_insert_with(Vec::new)
                .push(state);
        }

        Self {
            invoices,
            sku_invoice_index,
            sku_frequency_map,
            used_invoices: HashSet::new(),
            heap: BinaryHeap::new(),
        }
    }

    /// 初始化堆（第一轮全量计算）
    pub fn init_heap(&mut self, requirements: &MatchingRequirements) {
        self.heap.clear();
        
        // 收集所有相关候选发票（只查有需求SKU的）
        let mut candidates: HashSet<i64> = HashSet::new();
        for sku in requirements.get_required_skus() {
            if let Some(inv_ids) = self.sku_invoice_index.get(&sku) {
                candidates.extend(inv_ids);
            }
        }

        for invoice_id in candidates {
            let (score, sku_count) = self.calculate_score_int(invoice_id, requirements);
            if score > 0 {
                self.heap.push(InvoiceScore {
                    invoice_id,
                    score,
                    sku_count,
                });
            }
        }
    }

    /// 查找最优发票 - (Lazy Greed Strategy)
    pub fn find_best_invoice_lazy(&mut self, requirements: &MatchingRequirements) -> Option<i64> {
        loop {
            // 1. 取出堆顶（当前认为最好的）
            let best_candidate = match self.heap.pop() {
                Some(c) => c,
                None => return None, // 堆空了，没发票了
            };

            // 2. 惰性检查 (Lazy Check)
            // 重新计算它的真实评分
            let (current_score, current_sku_count) = self.calculate_score_int(best_candidate.invoice_id, requirements);

            // 3. 比较
            // 如果堆已经是空的，或者 当前评分 >= 堆顶评分，说明它就是冠军！
            // (注意：InvoiceScore 实现的是 Max-Heap，pop 出来的是最大的。
            // 只有当重新计算后的分数比堆里第二名还要小的时候，才需要放回去重新排。)
            
            match self.heap.peek() {
                None => {
                    // 堆空了，它就是唯一的王
                    // 但要确保它还有效 (score > 0)
                    if current_score > 0 {
                        return Some(best_candidate.invoice_id);
                    } else {
                        continue; // 废了，丢弃，下一位
                    }
                }
                Some(second_best) => {
                    if current_score >= second_best.score {
                        // 依然比第二名强 (或者相等)，它就是冠军
                        if current_score > 0 {
                             return Some(best_candidate.invoice_id);
                        } else {
                            continue; // 废了
                        }
                    } else {
                        // 4. 它变弱了，退回去重新排队
                         if current_score > 0 {
                            self.heap.push(InvoiceScore {
                                invoice_id: best_candidate.invoice_id,
                                score: current_score,
                                sku_count: current_sku_count,
                            });
                        }
                        // 继续 loop，处理下一个堆顶
                    }
                }
            }
        }
    }
    
    // 保留原方法用于兼容或对比（可选，目前直接替换调用）
    // pub fn find_best_invoice(...) 

    /// 计算整数评分 (Integer Arithmetic Optimization)
    /// 返回 (Score, SkuCount)
    fn calculate_score_int(&self, invoice_id: i64, requirements: &MatchingRequirements) -> (i64, i64) {
         let items = match self.invoices.get(&invoice_id) {
            Some(i) => i,
            None => return (0, 0),
        };

        let mut sku_count = 0i64;
        let mut score: i64 = 0;

        for item in items {
            if item.remaining_amount <= BigDecimal::from(0) {
                continue;
            }

            if let Some(required) = requirements.get_remaining(&item.product_code) {
                if *required > BigDecimal::from(0) {
                    sku_count += 1;
                    let available = if item.remaining_amount < *required {
                        &item.remaining_amount
                    } else {
                        required
                    };
                    
                    // 整数化: available * 100
                    // 注意：这里可能会有精度截断，但作为评分标准通常足够
                    if let Some(cent_val) = (available * BigDecimal::from(100)).to_i64() {
                        score += cent_val;
                    }

                    // 计算稀缺性加分: 1000 / frequency
                    if let Some(&freq) = self.sku_frequency_map.get(&item.product_code) {
                        if freq > 0 {
                            let bonus = 1000 / freq; 
                            score += bonus;
                        }
                    }
                }
            }
        }

        (score, sku_count)
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
