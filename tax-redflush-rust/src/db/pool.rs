use sqlx::postgres::{PgPoolOptions, PgConnectOptions};
use sqlx::{PgPool, ConnectOptions};
use std::time::Duration;
use std::str::FromStr;

/// 创建数据库连接池
pub async fn create_pool(database_url: &str) -> Result<PgPool, sqlx::Error> {
    let mut connect_options = PgConnectOptions::from_str(database_url)?;
    
    // 设置慢查询日志阈值为 5秒
    connect_options = connect_options.log_slow_statements(
        tracing::log::LevelFilter::Warn, 
        Duration::from_secs(5)
    );

    PgPoolOptions::new()
        .max_connections(20)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(connect_options)
        .await
}
