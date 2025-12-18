use crate::service::{MatcherService, InvoiceCentricMatcher};
use crate::models::MatchStats;
use axum::{
    extract::{Json, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// 请求体: 单据ID列表
#[derive(Debug, Deserialize)]
pub struct BatchMatchRequest {
    pub bill_ids: Vec<i64>,
}

/// 响应体
#[derive(Debug, Serialize)]
pub struct BatchMatchResponse {
    pub success: bool,
    pub message: String,
}

/// Invoice-Centric响应体（含统计信息）
#[derive(Debug, Serialize)]
pub struct InvoiceCentricResponse {
    pub success: bool,
    pub message: String,
    pub stats: Option<Vec<MatchStats>>,
}

/// 健康检查
pub async fn health_check() -> &'static str {
    "OK"
}

/// 批量匹配接口（原SKU-Centric算法）
pub async fn batch_match(
    State(service): State<Arc<MatcherService>>,
    Json(req): Json<BatchMatchRequest>,
) -> Response {
    match service.batch_match_temp_strategy(&req.bill_ids).await {
        Ok(_) => {
            let response = BatchMatchResponse {
                success: true,
                message: format!("Successfully matched {} bills", req.bill_ids.len()),
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            let response = BatchMatchResponse {
                success: false,
                message: format!("Error: {}", e),
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(response)).into_response()
        }
    }
}

/// Invoice-Centric批量匹配接口（新算法，减少发票使用量）
pub async fn batch_match_invoice_centric(
    State(matcher): State<Arc<InvoiceCentricMatcher>>,
    Json(req): Json<BatchMatchRequest>,
) -> Response {
    match matcher.batch_match(&req.bill_ids).await {
        Ok(stats) => {
            let total_invoices: usize = stats.iter().map(|s| s.invoices_used).sum();
            let total_skus: usize = stats.iter().map(|s| s.matched_skus).sum();

            let response = InvoiceCentricResponse {
                success: true,
                message: format!(
                    "Successfully matched {} bills, {} SKUs, {} invoices used",
                    req.bill_ids.len(), total_skus, total_invoices
                ),
                stats: Some(stats),
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            let response = InvoiceCentricResponse {
                success: false,
                message: format!("Error: {}", e),
                stats: None,
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(response)).into_response()
        }
    }
}
