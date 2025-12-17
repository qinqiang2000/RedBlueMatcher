use axum::{routing::{get, post}, Router};
use std::sync::Arc;
use tax_redflush_rust::{api, create_pool, AppConfig, MatcherService};
use tower::ServiceBuilder;
use tracing::info;
use tracing_subscriber::fmt::time::ChronoLocal;

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

    // 创建匹配服务
    let service = Arc::new(MatcherService::new(pool));

    // 构建路由
    let app = Router::new()
        .route("/health", get(api::health_check))
        .route("/api/match/batch", post(api::batch_match))
        .with_state(service)
        .layer(ServiceBuilder::new());

    // 启动服务器
    let addr = format!("{}:{}", config.server.host, config.server.port);
    info!("Server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
