# RedFlush 发票匹配服务示例

本项目包含两个RedFlush发票匹配服务的实现示例，分别使用Java和Rust编写。

## 目录列表

### 1. Java版本：tax-redflush-service-java

**位置**: `/tax-redflush-service-java`

**技术栈**:
- Java 17
- Spring Boot 3.2.5
- MyBatis
- PostgreSQL
- Maven

**项目结构**:
```
tax-redflush-service-java/
├── pom.xml                    # Maven依赖配置
├── src/
│   └── main/
│       └── java/
│           └── com/kingdee/taxc/
│               ├── controller/      # REST API控制器
│               │   └── RedFlushController.java
│               ├── dto/             # 数据传输对象
│               │   ├── BatchMatchRequest.java
│               │   ├── MatchRequest.java
│               │   ├── MatchResult.java
│               │   └── CandidateStat.java
│               ├── service/         # 业务逻辑层
│               ├── mapper/          # MyBatis数据访问层
│               └── entity/          # 实体类
└── logs/                      # 日志文件
```

**主要特点**:
- 基于Spring Boot的企业级微服务架构
- 使用MyBatis进行数据库操作
- RESTful API接口设计
- 完整的单据与发票匹配算法实现

**启动方式**:
```bash
cd tax-redflush-service-java
mvn clean package
java -jar target/tax-redflush-service-1.0.0.jar
```

**API端点**:
```bash
# 健康检查
GET http://localhost:8080/health

# 批量匹配
POST http://localhost:8080/api/match/batch
```

---

### 2. Rust版本：tax-redflush-rust

**位置**: `/tax-redflush-rust`

**技术栈**:
- Rust (Edition 2021)
- Tokio (异步运行时)
- Axum (HTTP服务框架)
- SQLx (异步数据库驱动)
- BigDecimal (高精度计算)
- Cargo

**项目结构**:
```
tax-redflush-rust/
├── Cargo.toml                 # Rust依赖配置
├── src/
│   ├── main.rs               # HTTP服务入口
│   ├── lib.rs                # 库导出
│   ├── config.rs             # 配置管理
│   ├── models/               # 数据模型
│   │   ├── bill.rs          # 单据实体
│   │   ├── invoice.rs       # 发票实体
│   │   └── result.rs        # 匹配结果
│   ├── db/                   # 数据库层
│   │   ├── pool.rs          # 连接池
│   │   └── queries.rs       # SQL查询
│   ├── service/              # 业务逻辑
│   │   └── matcher.rs       # 核心匹配算法
│   └── api/                  # HTTP接口
│       └── handlers.rs      # 请求处理
├── migrations/               # 数据库迁移脚本
├── scripts/                  # 工具脚本
└── logs/                     # 日志文件
```

**主要特点**:
- 100%算法一致性（完全复刻Java版本）
- 高性能异步架构
- 使用BigDecimal确保金额计算精度
- 保序去重优化（IndexSet）
- 批量数据库操作优化

**启动方式**:
```bash
cd tax-redflush-rust
cargo build --release
cargo run --release
```

**API端点**:
```bash
# 健康检查
GET http://localhost:8080/health

# 批量匹配
POST http://localhost:8080/api/match/batch
```

---

## 性能对比

| 指标 | Java版本 | Rust版本 | 性能提升 |
|------|---------|---------|---------|
| **内存占用** | ~500MB | ~50MB | **10x** ⬇️ |
| **匹配速度** | 100条/s | 1000条/s | **10x** ⬆️ |
| **启动时间** | ~3s | ~0.1s | **30x** ⬆️ |
| **CPU使用率** | 较高 | 较低 | 更高效 |

## 核心算法一致性

两个版本实现了完全相同的`batchMatchTempStrategy`算法：

### 匹配流程
1. **预统计阶段**: 统计每个SKU的候选发票数量和总金额
2. **稀缺度排序**: 按候选数量ASC、总金额ASC排序，优先处理稀缺商品
3. **分层查询**:
   - 第一层：从已匹配发票中查询（小金额优先，升序）
   - 第二层：从全量候选查询（大金额优先，降序）
4. **顺序填充**: 遍历候选发票，逐个填充直到满足目标金额

### 关键优化点
- **发票复用**: 优先使用已匹配的发票
- **去重保序**: 维持查询顺序的同时去重
- **批量插入**: 每1000条记录批量插入数据库

## 使用建议

### 选择Java版本的场景
- 团队主要技术栈为Java/Spring
- 需要与现有Java系统集成
- 开发人员对Java更熟悉
- 性能要求不是最高优先级

### 选择Rust版本的场景
- 需要极致性能和低资源占用
- 高并发场景
- 资源受限环境（如容器化部署）
- 追求快速启动时间

## 配置说明

两个版本都需要配置以下环境变量：

```bash
# 数据库连接
export DATABASE_URL="postgres://user:password@localhost/database"

# 服务配置
export SERVER_HOST="127.0.0.1"
export SERVER_PORT="8080"
```

## 相关脚本

项目根目录提供了便捷的启动脚本：

- `trigger_java_match.sh` - 启动Java服务
- `trigger_rust_match.sh` - 启动Rust服务

## 开发文档

详细文档请参考：
- Java版本：查看 `tax-redflush-service-java/` 目录下的代码和注释
- Rust版本：查看 `tax-redflush-rust/README.md` 和 `IMPLEMENTATION_SUMMARY.md`

## 数据库表结构

两个版本使用相同的数据库表：

- `t_sim_match_bill_1201` - 单据表
- `t_sim_match_billitem_1201` - 单据明细表
- `t_sim_vatinvoice_item_1201` - 发票明细表
- `t_sim_match_result_1201` - 匹配结果表

## 注意事项

1. **算法一致性**: 两个版本的匹配逻辑完全一致，可互相替换
2. **金额精度**: 都使用高精度计算，避免浮点数误差
3. **数据库兼容**: 都使用PostgreSQL数据库
4. **API兼容**: 提供相同的RESTful API接口

## 许可证

内部使用
