use crate::service::MatcherService;
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

/// 健康检查
pub async fn health_check() -> &'static str {
    "OK"
}

/// 批量匹配接口
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
