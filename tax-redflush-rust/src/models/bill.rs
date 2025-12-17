use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

/// 单据主表 (MatchBill1201)
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct MatchBill1201 {
    pub fid: i64,
    pub fbuyertaxno: String,
    pub fsalertaxno: String,
}

/// 单据明细表 (MatchBillItem1201)
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct MatchBillItem1201 {
    pub fid: i64,          // 关联单据ID
    pub fentryid: i64,     // 明细行ID
    pub fspbm: String,     // 商品编码/SKU
    pub famount: BigDecimal,  // 金额
    pub fnum: Option<BigDecimal>,      // 数量
    pub funitprice: Option<BigDecimal>, // 单价
}

/// 临时汇总表 (用于SKU稀缺度排序)
#[derive(Debug, Clone)]
pub struct TempSummary {
    pub fspbm: String,
    pub item_count: i64,
    pub total_amount: BigDecimal,
}
