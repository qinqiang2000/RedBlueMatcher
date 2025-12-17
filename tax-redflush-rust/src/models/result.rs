use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// 匹配结果表 (MatchResult1201)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchResult1201 {
    pub fbillid: i64,
    pub fbuyertaxno: String,
    pub fsalertaxno: String,
    pub fspbm: String,
    pub finvoiceid: i64,
    pub finvoiceitemid: i64,
    pub fnum: BigDecimal,
    pub fbillamount: BigDecimal,
    pub finvoiceamount: BigDecimal,
    pub fmatchamount: BigDecimal,
    pub fbillunitprice: Option<BigDecimal>,
    pub fbillqty: Option<BigDecimal>,
    pub finvoiceunitprice: Option<BigDecimal>,
    pub finvoiceqty: Option<BigDecimal>,
    pub fmatchtime: DateTime<Utc>,
}
