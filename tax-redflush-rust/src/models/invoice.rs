use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

/// 候选发票项 (MatchedInvoiceItem)
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct MatchedInvoiceItem {
    pub invoice_id: i64,
    pub item_id: i64,
    pub product_code: String,
    pub quantity: BigDecimal,
    pub amount: BigDecimal,
    pub unit_price: Option<BigDecimal>,
}

/// 候选发票统计结果
#[derive(Debug, Clone, FromRow)]
pub struct CandidateStat {
    pub cnt: i64,
    pub sum_amount: BigDecimal,
}
