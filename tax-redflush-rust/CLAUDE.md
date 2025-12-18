# Tax RedFlush Rust - Claude AI 项目指南

这个文档是为 Claude AI 助手准备的，帮助理解和操作这个 Rust 项目。

## 项目概述

**税务红冲发票匹配服务 - Rust 高性能实现**

这是一个用 Rust 编写的高性能发票匹配微服务，完全复刻了 Java 版本的 `batchMatchTempStrategy` 算法。主要用于企业税务系统中的红冲发票（红票）与蓝票的智能匹配。

### 核心特性
- **100% 算法一致性**: 与 Java 版本算法完全一致
- **高性能**: 10x 速度提升，10x 内存降低
- **异步架构**: 基于 Tokio 异步运行时
- **高精度计算**: 使用 BigDecimal 确保金额精度
- **RESTful API**: 提供 HTTP 接口供外部调用

## 项目结构

```
tax-redflush-rust/
├── Cargo.toml                    # Rust 项目配置和依赖
├── Cargo.lock                    # 依赖锁文件
├── README.md                     # 用户文档
├── IMPLEMENTATION_SUMMARY.md     # 实现总结
├── PERFORMANCE_OPTIMIZATION.md   # 性能优化文档
├── CLAUDE.md                     # 本文件 - AI 助手指南
├── verify.sh                     # 验证脚本
├── src/
│   ├── main.rs                  # 🚀 HTTP 服务入口
│   ├── lib.rs                   # 📦 库导出
│   ├── config.rs                # ⚙️ 配置管理（环境变量）
│   ├── models/                  # 📊 数据模型
│   │   ├── mod.rs
│   │   ├── bill.rs             # 单据实体
│   │   ├── invoice.rs          # 发票实体
│   │   └── result.rs           # 匹配结果实体
│   ├── db/                      # 🗄️ 数据库层
│   │   ├── mod.rs
│   │   ├── pool.rs             # PostgreSQL 连接池
│   │   └── queries.rs          # SQL 查询函数
│   ├── service/                 # 🧠 业务逻辑层
│   │   ├── mod.rs
│   │   └── matcher.rs          # ⭐ 核心匹配算法实现
│   └── api/                     # 🌐 HTTP API 层
│       ├── mod.rs
│       └── handlers.rs         # 请求处理器
├── migrations/                   # 📝 数据库迁移脚本
│   └── 001_optimize_invoice_queries.sql
├── scripts/                      # 🛠️ 工具脚本
│   ├── check_query_performance.sh
│   ├── clean_results.sh
│   └── deduplicate_results.sql
├── logs/                         # 📋 日志文件目录
└── target/                       # 🎯 编译输出目录（Git 忽略）

```

## 技术栈

### 核心依赖
| 依赖 | 版本 | 用途 |
|------|------|------|
| **tokio** | 1.x | 异步运行时，提供异步 I/O |
| **sqlx** | 0.7 | 异步数据库驱动（PostgreSQL）|
| **axum** | 0.7 | 高性能 HTTP 服务框架 |
| **bigdecimal** | 0.3 | 高精度十进制计算（金额） |
| **indexmap** | 2.x | 保序 HashMap/HashSet |
| **serde** | 1.x | 序列化/反序列化 |
| **chrono** | 0.4 | 日期时间处理 |
| **tracing** | 0.1 | 结构化日志 |

### 数据库
- PostgreSQL（通过 SQLx 连接）
- 表名带日期后缀（如 `t_sim_match_bill_1201`）

## 环境设置

### 前置要求
- Rust 1.70+ (推荐使用 rustup)
- PostgreSQL 12+
- 环境变量配置

### 环境变量

在项目根目录或系统中设置：

```bash
# 数据库连接（必需）
export DATABASE_URL="postgres://username:password@localhost:5432/database_name"

# 服务器配置（可选，有默认值）
export SERVER_HOST="127.0.0.1"      # 默认: 127.0.0.1
export SERVER_PORT="8080"            # 默认: 8080
```

**提示**: 可以创建 `.env` 文件，但不要提交到 Git！

## 构建和运行

### 开发模式（快速编译）

```bash
# 构建
cargo build

# 运行
cargo run

# 构建 + 运行（一步到位）
cargo run
```

### 生产模式（优化性能）

```bash
# 构建 release 版本
cargo build --release

# 运行 release 版本
cargo run --release

# 或直接运行二进制文件
./target/release/tax-redflush-rust
```

### 后台运行

```bash
# 使用 nohup
nohup cargo run --release > logs/service.log 2>&1 &

# 或使用脚本
../../trigger_rust_match.sh
```

## 测试

```bash
# 运行所有测试
cargo test

# 运行测试并显示输出
cargo test -- --nocapture

# 运行特定测试
cargo test test_name

# 测试 + 代码覆盖率（需要 tarpaulin）
cargo tarpaulin
```

## API 接口

### 健康检查

```bash
GET http://localhost:8080/health

# 响应
{
  "status": "ok"
}
```

### 批量匹配

```bash
POST http://localhost:8080/api/match/batch
Content-Type: application/json

{
  "bill_ids": [1001, 1002, 1003]
}

# 成功响应
{
  "success": true,
  "message": "Successfully matched 3 bills"
}

# 失败响应
{
  "success": false,
  "message": "Error message..."
}
```

## 核心概念

### 匹配算法流程

**文件位置**: `src/service/matcher.rs`

1. **预统计阶段**
   - 统计每个 SKU (商品编码) 的候选发票数量和总金额
   - 函数: `queries::stat_for_product()`

2. **稀缺度排序**
   - 按 `候选数量 ASC, 总金额 ASC` 排序
   - 优先处理稀缺商品（候选少的先匹配）

3. **分层查询**
   - **第一层**: 从 `preferred_invoices`（已匹配发票）查询
     - 按金额**升序**排序（小金额优先）
     - 函数: `queries::match_on_invoices()`
   - **第二层**: 从全量候选发票查询
     - 按金额**降序**排序（大金额优先）
     - 函数: `queries::match_by_tax_and_product()`

4. **顺序填充**
   - 遍历候选发票，逐个填充直到满足目标金额
   - 使用 `IndexSet` 去重并保持顺序

5. **批量插入**
   - 每 1000 条结果批量插入数据库
   - 函数: `queries::insert_results_batch()`

### 关键数据结构

```rust
// 单据主表
struct Bill1201 {
    fid: i64,             // 单据 ID
    fbuyertaxno: String,  // 购方税号
    fsalertaxno: String,  // 销方税号
}

// 单据明细
struct BillItem1201 {
    fid: i64,             // 单据 ID
    fentryid: i64,        // 明细行 ID
    fspbm: String,        // 商品编码（SKU）
    famount: BigDecimal,  // 金额
}

// 发票明细
struct InvoiceItem1201 {
    fid: i64,             // 发票 ID
    fentryid: i64,        // 明细行 ID
    fspbm: String,        // 商品编码
    famount: BigDecimal,  // 金额
}

// 匹配结果
struct MatchResult1201 {
    fbillid: i64,         // 单据 ID
    finvoiceid: i64,      // 发票 ID
    fmatchamount: BigDecimal,  // 匹配金额
    // ... 其他字段
}
```

## 常见开发任务

### 添加新的 API 端点

1. 在 `src/api/handlers.rs` 添加处理函数
2. 在 `src/main.rs` 注册路由
3. 如需数据库操作，在 `src/db/queries.rs` 添加查询函数

### 修改匹配算法

**⚠️ 重要**: 修改前请确保理解 Java 版本的算法逻辑！

1. 主要文件: `src/service/matcher.rs`
2. 确保保持与 Java 版本的算法一致性
3. 特别注意：
   - 排序顺序（升序 vs 降序）
   - BigDecimal 精度
   - IndexSet 的使用（保序去重）

### 添加数据库查询

1. 在 `src/db/queries.rs` 添加函数
2. 使用 SQLx 宏 `query!` 或 `query_as!`
3. 示例：

```rust
pub async fn get_bill(pool: &PgPool, bill_id: i64) -> Result<Option<Bill1201>, sqlx::Error> {
    sqlx::query_as!(
        Bill1201,
        r#"
        SELECT fid, fbuyertaxno, fsalertaxno
        FROM t_sim_match_bill_1201
        WHERE fid = $1
        "#,
        bill_id
    )
    .fetch_optional(pool)
    .await
}
```

### 查看日志

```bash
# 实时查看日志
tail -f logs/rust-service.log

# 搜索错误
grep ERROR logs/rust-service.log

# 查看最近 100 行
tail -n 100 logs/rust-service.log
```

### 性能分析

```bash
# 使用 flamegraph（需要安装 cargo-flamegraph）
cargo flamegraph --bin tax-redflush-rust

# 使用 perf（Linux）
perf record -g ./target/release/tax-redflush-rust
perf report

# 检查查询性能
./scripts/check_query_performance.sh
```

## 代码规范

### 命名约定
- 文件名: `snake_case.rs`
- 结构体: `PascalCase`
- 函数: `snake_case`
- 常量: `SCREAMING_SNAKE_CASE`
- 模块: `snake_case`

### 代码风格

```bash
# 自动格式化代码
cargo fmt

# 检查代码风格
cargo fmt -- --check

# Lint 检查
cargo clippy

# 严格 Lint
cargo clippy -- -D warnings
```

### 错误处理

```rust
// ✅ 推荐：使用 Result
pub async fn do_something() -> Result<Data, Box<dyn std::error::Error>> {
    let data = fetch_data().await?;
    Ok(data)
}

// ❌ 避免：unwrap/expect（除非在测试或绝对安全的情况下）
let value = option.unwrap();  // 可能 panic!
```

### 日志记录

```rust
use tracing::{info, warn, error, debug};

// 不同级别
debug!("详细调试信息");
info!("常规信息");
warn!("警告");
error!("错误");

// 带变量
info!("Processing bill_id: {}", bill_id);
info!(bill_id = %bill_id, "Processing bill");
```

## 调试技巧

### 启用详细日志

```bash
# 设置日志级别
export RUST_LOG=debug
cargo run

# 仅针对本项目
export RUST_LOG=tax_redflush_rust=debug
cargo run
```

### 使用 Rust 调试器

```bash
# 使用 rust-gdb (Linux)
rust-gdb target/debug/tax-redflush-rust

# 使用 rust-lldb (macOS)
rust-lldb target/debug/tax-redflush-rust

# 或在 VS Code 中使用 CodeLLDB 扩展
```

### 常见问题排查

#### 编译错误

```bash
# 清理并重新编译
cargo clean
cargo build

# 更新依赖
cargo update
```

#### 数据库连接失败

1. 检查 `DATABASE_URL` 是否正确
2. 确认 PostgreSQL 服务是否运行
3. 验证数据库表是否存在

```bash
# 测试数据库连接
psql $DATABASE_URL -c "SELECT 1"
```

#### 性能问题

1. 确保使用 `--release` 模式
2. 检查数据库索引
3. 使用 `./scripts/check_query_performance.sh`

## 数据库表结构

### 主要表

- `t_sim_match_bill_1201` - 单据主表
- `t_sim_match_billitem_1201` - 单据明细表
- `t_sim_vatinvoice_item_1201` - 发票明细表
- `t_sim_match_result_1201` - 匹配结果表

### 索引优化

参考 `migrations/001_optimize_invoice_queries.sql`

## 常用命令速查

```bash
# 开发
cargo run                          # 运行开发版本
cargo build --release              # 构建生产版本
cargo test                         # 运行测试
cargo fmt                          # 格式化代码
cargo clippy                       # 代码检查

# 清理
cargo clean                        # 清理编译输出
./scripts/clean_results.sh         # 清理数据库结果

# 调试
RUST_LOG=debug cargo run          # 启用调试日志
RUST_BACKTRACE=1 cargo run        # 启用错误堆栈跟踪

# 文档
cargo doc --open                   # 生成并打开文档
```

## 性能基准

与 Java 版本对比：

| 指标 | Java | Rust | 提升 |
|------|------|------|------|
| 内存占用 | ~500MB | ~50MB | 10x ⬇️ |
| 匹配速度 | 100条/s | 1000条/s | 10x ⬆️ |
| 启动时间 | ~3s | ~0.1s | 30x ⬆️ |
| CPU 使用率 | 较高 | 较低 | 更高效 |

## 相关文档

- `README.md` - 用户使用文档
- `IMPLEMENTATION_SUMMARY.md` - 实现细节总结
- `PERFORMANCE_OPTIMIZATION.md` - 性能优化指南
- `../trigger_rust_match.sh` - 启动脚本

## 协作 Java 版本

Java 版本位于：`../tax-redflush-service-java/`

**重要原则**: 两个版本的算法必须保持**100%一致**！

修改算法时，需要同步更新两个版本。

## 注意事项

### ⚠️ 关键约束

1. **算法一致性**: 不要随意修改匹配逻辑，必须与 Java 版本保持一致
2. **金额精度**: 始终使用 `BigDecimal`，不要用 `f64`
3. **排序顺序**:
   - `match_by_tax_and_product`: 金额**降序** (DESC)
   - `match_on_invoices`: 金额**升序** (ASC)
4. **批量操作**: 插入操作每 1000 条分批，避免内存溢出
5. **保序去重**: 使用 `IndexSet`，不要用普通 `HashSet`

### 🔒 安全性

- 不要在日志中记录敏感信息（税号、金额等）
- 使用环境变量管理配置，不要硬编码
- 定期更新依赖以修复安全漏洞

### 📝 Git 工作流

本项目是父仓库 `RedBlueMatcher` 的子目录，提交时注意：

```bash
# 在父仓库根目录提交
cd /home/user/RedBlueMatcher
git add tax-redflush-rust/
git commit -m "Update Rust service: ..."
git push
```

## 获取帮助

1. 查看 Rust 官方文档：https://doc.rust-lang.org/
2. SQLx 文档：https://docs.rs/sqlx/
3. Tokio 文档：https://tokio.rs/
4. 内部联系：查看父项目 README

---

**最后更新**: 2025-12-18
**维护者**: Claude AI + 开发团队
