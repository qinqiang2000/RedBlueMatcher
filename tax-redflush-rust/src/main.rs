use axum::{routing::{get, post}, Router};
use std::sync::Arc;
use tax_redflush_rust::{api, create_pool, AppConfig, MatcherService, InvoiceCentricMatcher};
use tower::ServiceBuilder;
use tracing::info;
use tracing_subscriber::fmt::time::ChronoLocal;

/// 共享状态：包含两种匹配服务
#[derive(Clone)]
pub struct AppState {
    pub sku_centric: Arc<MatcherService>,
    pub invoice_centric: Arc<InvoiceCentricMatcher>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志 - 使用本地时间格式 (类似Java格式)
    tracing_subscriber::fmt()
        .with_timer(ChronoLocal::new("%Y-%m-%d %H:%M:%S".to_string()))
        .with_target(true)
        .with_level(true)
        .init();

    // 加载配置
    let config = AppConfig::from_env();
    info!("Starting server with config: {:?}", config);

    // 创建数据库连接池
    let pool = create_pool(&config.database.url).await?;
    info!("Database pool created");

    // 创建两种匹配服务
    let sku_centric_service = Arc::new(MatcherService::new(pool.clone()));
    let invoice_centric_matcher = Arc::new(InvoiceCentricMatcher::new(pool));

    // 构建路由
    // 原SKU-Centric算法路由
    let sku_centric_routes = Router::new()
        .route("/api/match/batch", post(api::batch_match))
        .with_state(sku_centric_service);

    // 新Invoice-Centric算法路由
    let invoice_centric_routes = Router::new()
        .route("/api/match/batch/v2", post(api::batch_match_invoice_centric))
        .with_state(invoice_centric_matcher);

    // 合并路由
    let app = Router::new()
        .route("/health", get(api::health_check))
        .merge(sku_centric_routes)
        .merge(invoice_centric_routes)
        .layer(ServiceBuilder::new());

    // 启动服务器
    let addr = format!("{}:{}", config.server.host, config.server.port);
    info!("Server listening on {}", addr);
    info!("API Endpoints:");
    info!("  POST /api/match/batch     - SKU-Centric (original)");
    info!("  POST /api/match/batch/v2  - Invoice-Centric (optimized)");

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
