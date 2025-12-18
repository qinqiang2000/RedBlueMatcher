# Tax RedFlush 匹配算法详细设计文档

## 目录

1. [概述](#概述)
2. [算法架构](#算法架构)
3. [核心流程](#核心流程)
4. [数据结构](#数据结构)
5. [匹配策略](#匹配策略)
6. [关键函数](#关键函数)
7. [数据库查询](#数据库查询)
8. [性能优化](#性能优化)
9. [代码示例](#代码示例)

---

## 概述

### 业务背景

税务红冲发票匹配服务用于将企业单据（采购单、销售单等）与对应的增值税发票进行智能匹配。核心目标是根据相同的**商品编码(SKU)**、**购方税号**和**销方税号**，将单据金额与发票金额进行精确配对。

### 设计目标

- **100% 算法一致性**: 完全复刻 Java 版本的 `batchMatchTempStrategy` 算法
- **高精度计算**: 使用 BigDecimal 确保金额计算无误差
- **高性能**: 通过算法优化和数据库索引提升匹配速度
- **可扩展性**: 支持批量处理和分层查询

---

## 算法架构

### 整体架构图

```
┌─────────────────────────────────────────────────────────────────────┐
│                         HTTP 服务层 (axum)                           │
│                   POST /api/match/batch                              │
└─────────────────────────────────┬───────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        业务逻辑层 (service)                          │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │                  MatcherService                               │   │
│  │                                                               │   │
│  │  batch_match_temp_strategy(bill_ids)                         │   │
│  │    │                                                          │   │
│  │    ├── 1. 预统计: stat_for_product()                         │   │
│  │    ├── 2. 稀缺度排序: sort_by(cnt, amount)                   │   │
│  │    ├── 3. 分层查询:                                           │   │
│  │    │   ├── match_on_invoices() [已用发票]                    │   │
│  │    │   └── match_by_tax_and_product() [全量候选]             │   │
│  │    ├── 4. 贪心填充: 逐个填充至目标金额                        │   │
│  │    └── 5. 批量插入: insert_batch()                           │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                                                                      │
└─────────────────────────────────┬───────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        数据访问层 (db)                               │
│                                                                      │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐      │
│  │   pool.rs       │  │   queries.rs    │  │   models/       │      │
│  │  连接池管理      │  │   SQL 查询函数   │  │   数据结构      │      │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘      │
│                                                                      │
└─────────────────────────────────┬───────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        PostgreSQL 数据库                             │
│                                                                      │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐   │
│  │ t_sim_match_     │  │ t_sim_vatinvoice │  │ t_sim_match_     │   │
│  │ bill_1201        │  │ _item_1201       │  │ result_1201      │   │
│  │ (单据表)          │  │ (发票明细表)      │  │ (结果表)          │   │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘   │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### 模块职责

| 模块 | 文件位置 | 职责 |
|------|---------|------|
| **HTTP 入口** | `src/main.rs` | 启动服务，注册路由 |
| **API 处理器** | `src/api/handlers.rs` | 解析请求，返回响应 |
| **匹配服务** | `src/service/matcher.rs` | 核心匹配算法实现 |
| **数据库查询** | `src/db/queries.rs` | SQL 查询封装 |
| **数据模型** | `src/models/*.rs` | 实体结构定义 |
| **配置管理** | `src/config.rs` | 环境变量读取 |

---

## 核心流程

### 完整流程图

```
HTTP 请求: POST /api/match/batch
    │
    ▼
┌───────────────────────────────────────────────────────────────┐
│  输入: BatchMatchRequest { bill_ids: [1001, 1002, ...] }      │
└───────────────────────────────────────────────────────────────┘
    │
    ▼
╔═══════════════════════════════════════════════════════════════╗
║  对每个 bill_id 执行以下步骤:                                  ║
╚═══════════════════════════════════════════════════════════════╝
    │
    │ ┌─────────────────────────────────────────────────────────┐
    ├─│ 步骤 1: 查询单据主表                                     │
    │ │   get_bill(pool, bill_id)                               │
    │ │   → 获取: {fid, fbuyertaxno, fsalertaxno}               │
    │ └─────────────────────────────────────────────────────────┘
    │
    │ ┌─────────────────────────────────────────────────────────┐
    ├─│ 步骤 2: 查询单据明细                                     │
    │ │   list_bill_items(pool, bill_id)                        │
    │ │   → 获取: [BillItem × N]                                │
    │ │     每项包含: fspbm(商品编码), famount(金额)             │
    │ └─────────────────────────────────────────────────────────┘
    │
    │ ┌─────────────────────────────────────────────────────────┐
    ├─│ 步骤 3: 预统计 - 为每个 SKU 统计候选发票                  │
    │ │   for each bill_item:                                   │
    │ │     stat_for_product(buyer_tax, seller_tax, fspbm)      │
    │ │     → 返回: {cnt: 候选数量, sum_amount: 总金额}          │
    │ │     → 构建: TempSummary 对象                            │
    │ └─────────────────────────────────────────────────────────┘
    │
    │ ┌─────────────────────────────────────────────────────────┐
    ├─│ 步骤 4: 稀缺度排序                                       │
    │ │   summaries.sort_by(|a, b|                              │
    │ │     a.item_count.cmp(&b.item_count)        // 数量升序  │
    │ │       .then_with(|| a.total_amount.cmp(&b.total_amount))│
    │ │   )                                        // 金额升序  │
    │ │   → 优先处理稀缺商品（候选少的先处理）                    │
    │ └─────────────────────────────────────────────────────────┘
    │
    │ ┌─────────────────────────────────────────────────────────┐
    ├─│ 步骤 5: 初始化匹配状态                                   │
    │ │   preferred_invoices: IndexSet<i64> = []   // 已用发票  │
    │ │   matched_by_product: HashMap<String, BigDecimal> = {}  │
    │ │                                           // 已匹配金额 │
    │ └─────────────────────────────────────────────────────────┘
    │
    │ ╔═════════════════════════════════════════════════════════╗
    ├─║  步骤 6: 匹配阶段 - 按稀缺度顺序逐个 SKU 处理            ║
    │ ╚═════════════════════════════════════════════════════════╝
    │     │
    │     │ for each ordered_item:
    │     │
    │     │ ┌─────────────────────────────────────────────────┐
    │     ├─│ 6.1 计算剩余需匹配金额                           │
    │     │ │   remaining = target_amount - already_matched   │
    │     │ │   if remaining ≤ 0: continue (已足额)           │
    │     │ └─────────────────────────────────────────────────┘
    │     │
    │     │ ┌─────────────────────────────────────────────────┐
    │     ├─│ 6.2 构建候选集合 (两层查询 + 去重)               │
    │     │ │                                                 │
    │     │ │ 【第一层查询】从已匹配发票中查询 (复用优先)       │
    │     │ │   for chunk in preferred_invoices.chunks(1000): │
    │     │ │     match_on_invoices(tax, product, chunk)      │
    │     │ │     → 返回: [InvoiceItem × M]                   │
    │     │ │     → 排序: famount ASC (小金额优先)            │
    │     │ │     → 去重: IndexSet<item_id>                   │
    │     │ │                                                 │
    │     │ │ 【第二层查询】从全量候选发票中查询 (新发票)       │
    │     │ │   match_by_tax_and_product(tax, product)        │
    │     │ │     → 返回: [InvoiceItem × K]                   │
    │     │ │     → 排序: famount DESC (大金额优先)           │
    │     │ │     → 去重: IndexSet<item_id>                   │
    │     │ └─────────────────────────────────────────────────┘
    │     │
    │     │ ┌─────────────────────────────────────────────────┐
    │     ├─│ 6.3 贪心填充                                     │
    │     │ │   for each candidate in source:                 │
    │     │ │     if remaining ≤ 0: break                     │
    │     │ │     use_amount = min(invoice_amount, remaining) │
    │     │ │     → 生成 MatchResult 记录                      │
    │     │ │     → 加入 preferred_invoices                   │
    │     │ │     → 更新 matched_by_product                   │
    │     │ │     → 扣除 remaining                            │
    │     │ └─────────────────────────────────────────────────┘
    │     │
    │     │ ┌─────────────────────────────────────────────────┐
    │     └─│ 6.4 批量插入结果                                 │
    │       │   for chunk in batch.chunks(1000):              │
    │       │     insert_batch(pool, chunk)                   │
    │       └─────────────────────────────────────────────────┘
    │
    ▼
┌───────────────────────────────────────────────────────────────┐
│  输出: BatchMatchResponse { success: true, message: "..." }   │
└───────────────────────────────────────────────────────────────┘
```

### 流程伪代码

```rust
fn batch_match_temp_strategy(bill_ids: &[i64]) {
    for bill_id in bill_ids {
        // 1. 获取单据信息
        let bill = get_bill(bill_id);
        let bill_items = list_bill_items(bill_id);

        // 2. 预统计 - 构建 TempSummary
        let mut summaries = Vec::new();
        for item in &bill_items {
            let stat = stat_for_product(
                bill.fbuyertaxno,
                bill.fsalertaxno,
                item.fspbm
            );
            summaries.push(TempSummary {
                fspbm: item.fspbm,
                item_count: stat.cnt,
                total_amount: stat.sum_amount,
            });
        }

        // 3. 稀缺度排序
        summaries.sort_by(|a, b| {
            a.item_count.cmp(&b.item_count)
                .then_with(|| a.total_amount.cmp(&b.total_amount))
        });

        // 4. 初始化状态
        let mut preferred_invoices = IndexSet::new();
        let mut matched_by_product = HashMap::new();
        let mut batch = Vec::new();

        // 5. 按稀缺度顺序匹配
        for summary in &summaries {
            let code = &summary.fspbm;
            let target = get_target_amount(bill_items, code);
            let matched = matched_by_product.get(code).unwrap_or(&ZERO);
            let remaining = target - matched;

            if remaining <= 0 {
                continue;
            }

            // 构建候选集合
            let source = build_candidate_source(
                &preferred_invoices,
                bill.fbuyertaxno,
                bill.fsalertaxno,
                code
            );

            // 贪心填充
            for candidate in &source {
                if remaining <= 0 {
                    break;
                }
                let use_amount = min(candidate.amount, remaining);

                // 生成匹配记录
                batch.push(MatchResult {
                    fbillid: bill_id,
                    finvoiceid: candidate.invoice_id,
                    fmatchamount: use_amount,
                    // ... 其他字段
                });

                preferred_invoices.insert(candidate.invoice_id);
                *matched_by_product.entry(code.clone()).or_insert(ZERO) += use_amount;
                remaining -= use_amount;
            }
        }

        // 6. 批量插入
        for chunk in batch.chunks(1000) {
            insert_batch(chunk);
        }
    }
}
```

---

## 数据结构

### 单据模型

```rust
/// 单据主表 - 包含税务信息
#[derive(Debug, Clone)]
pub struct MatchBill1201 {
    pub fid: i64,              // 单据ID (主键)
    pub fbuyertaxno: String,   // 购方税号
    pub fsalertaxno: String,   // 销方税号
}

/// 单据明细项 - 包含商品和金额信息
#[derive(Debug, Clone)]
pub struct MatchBillItem1201 {
    pub fid: i64,                              // 关联单据ID
    pub fentryid: i64,                         // 明细行ID
    pub fspbm: String,                         // 商品编码 (SKU)
    pub famount: BigDecimal,                   // 金额
    pub fnum: Option<BigDecimal>,              // 数量
    pub funitprice: Option<BigDecimal>,        // 单价
}
```

### 发票模型

```rust
/// 候选发票项 - SQL 查询结果映射
#[derive(Debug, Clone)]
pub struct MatchedInvoiceItem {
    pub invoice_id: i64,                    // 发票ID
    pub item_id: i64,                       // 明细行ID (用于去重)
    pub product_code: String,               // 商品编码
    pub quantity: BigDecimal,               // 数量
    pub amount: BigDecimal,                 // 金额
    pub unit_price: Option<BigDecimal>,     // 单价
}

/// 候选发票统计结果 - 预统计阶段使用
#[derive(Debug, Clone)]
pub struct CandidateStat {
    pub cnt: i64,                           // 候选数量
    pub sum_amount: BigDecimal,             // 总金额
}
```

### 临时汇总结构

```rust
/// 临时汇总 - 用于稀缺度排序
#[derive(Debug, Clone)]
pub struct TempSummary {
    pub fspbm: String,                         // 商品编码
    pub item_count: i64,                       // 候选发票项数量
    pub total_amount: BigDecimal,              // 候选发票总金额
}
```

### 匹配结果模型

```rust
/// 匹配结果 - 写入数据库的最终记录
#[derive(Debug, Clone)]
pub struct MatchResult1201 {
    pub fbillid: i64,                          // 单据ID
    pub fbuyertaxno: String,                   // 购方税号
    pub fsalertaxno: String,                   // 销方税号
    pub fspbm: String,                         // 商品编码
    pub finvoiceid: i64,                       // 发票ID
    pub finvoiceitemid: i64,                   // 发票明细行ID
    pub fnum: BigDecimal,                      // 数量
    pub fbillamount: BigDecimal,               // 单据金额
    pub finvoiceamount: BigDecimal,            // 发票金额
    pub fmatchamount: BigDecimal,              // 实际匹配金额
    pub fbillunitprice: Option<BigDecimal>,    // 单据单价
    pub fbillqty: Option<BigDecimal>,          // 单据数量
    pub finvoiceunitprice: Option<BigDecimal>, // 发票单价
    pub finvoiceqty: Option<BigDecimal>,       // 发票数量
    pub fmatchtime: DateTime<Utc>,             // 匹配时间
}
```

### 运行时状态

```rust
/// 匹配过程中的运行时状态
struct MatchingState {
    /// 已匹配发票ID集合 - 用于复用优先查询
    /// 使用 IndexSet 保持插入顺序的同时提供 O(1) 查找
    preferred_invoices: IndexSet<i64>,

    /// 每个商品编码已匹配的金额
    /// Key: fspbm (商品编码)
    /// Value: 已匹配金额总和
    matched_by_product: HashMap<String, BigDecimal>,

    /// 待批量插入的匹配结果
    batch: Vec<MatchResult1201>,
}
```

---

## 匹配策略

### 策略 1: 稀缺度优先

**原理**: 优先处理候选发票少的商品，避免稀缺商品被"挤占"。

```rust
// 排序规则
summaries.sort_by(|a, b| {
    // 第一优先级: 候选数量升序 (少的先处理)
    a.item_count.cmp(&b.item_count)
        // 第二优先级: 总金额升序 (金额小的先处理)
        .then_with(|| a.total_amount.cmp(&b.total_amount))
});
```

**示例**:

| SKU | 候选数量 | 总金额 | 处理顺序 |
|-----|---------|--------|---------|
| A | 5 | 10000 | 1 (最先) |
| B | 5 | 20000 | 2 |
| C | 10 | 15000 | 3 |
| D | 20 | 50000 | 4 (最后) |

### 策略 2: 分层查询

**原理**: 优先复用已匹配过的发票上的剩余金额，减少发票使用数量。

```
┌─────────────────────────────────────────────────────────────┐
│              分层查询策略示意图                              │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│   第一层: preferred_invoices (已用发票)                     │
│   ┌───────────────────────────────────────┐                │
│   │  排序: famount ASC (小金额优先)        │                │
│   │  目的: 消耗已用发票上的小额剩余         │                │
│   │  场景: preferred_invoices 非空时       │                │
│   └───────────────────────────────────────┘                │
│                      │                                      │
│                      ▼                                      │
│   第二层: 全量候选发票                                      │
│   ┌───────────────────────────────────────┐                │
│   │  排序: famount DESC (大金额优先)       │                │
│   │  目的: 用大额发票快速凑齐目标金额        │                │
│   │  场景: 始终执行                        │                │
│   └───────────────────────────────────────┘                │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

**排序方向说明**:

| 查询层 | SQL 排序 | 逻辑 |
|--------|---------|------|
| 第一层 (match_on_invoices) | `ORDER BY famount ASC` | 小金额优先 - 清理碎片 |
| 第二层 (match_by_tax_and_product) | `ORDER BY famount DESC` | 大金额优先 - 快速填充 |

### 策略 3: 保序去重

**原理**: 使用 `IndexSet` 确保候选发票不重复，同时保持查询顺序。

```rust
let mut seen_item_ids: IndexSet<i64> = IndexSet::new();

// 添加第一层查询结果
for mi in first_layer_results {
    if seen_item_ids.insert(mi.item_id) {  // 返回 true = 新增
        source.push(mi);
    }
}

// 添加第二层查询结果
for mi in second_layer_results {
    if seen_item_ids.insert(mi.item_id) {  // 已存在则跳过
        source.push(mi);
    }
}
```

**为什么不用 HashSet?**
- `HashSet` 不保证迭代顺序
- `IndexSet` 保持插入顺序，便于调试和结果追踪

### 策略 4: 贪心填充

**原理**: 按顺序遍历候选发票，每次取 `min(发票金额, 剩余需求)` 进行匹配。

```rust
for candidate in &source {
    if remaining <= BigDecimal::zero() {
        break;  // 已满足需求
    }

    // 贪心策略: 能用多少用多少
    let use_amount = if &candidate.amount >= &remaining {
        remaining.clone()      // 需要多少用多少
    } else {
        candidate.amount.clone() // 用完整个发票金额
    };

    if use_amount <= BigDecimal::zero() {
        continue;  // 跳过零金额
    }

    // 生成匹配记录...
    remaining = &remaining - &use_amount;
}
```

**示例**: 需匹配金额 2500

| 候选发票 | 金额 | 本次使用 | 剩余需求 |
|---------|------|---------|---------|
| INV-001 | 1000 | 1000 | 1500 |
| INV-002 | 800 | 800 | 700 |
| INV-003 | 1200 | 700 | 0 (完成) |

---

## 关键函数

### 核心匹配函数

```rust
// 文件: src/service/matcher.rs

impl MatcherService {
    /// 批量匹配入口函数 - 完全复刻 Java 版本
    pub async fn batch_match_temp_strategy(
        &self,
        bill_ids: &[i64]
    ) -> Result<(), Box<dyn std::error::Error>> {
        for bill_id in bill_ids {
            self.match_single_bill(*bill_id).await?;
        }
        Ok(())
    }

    /// 单个单据匹配
    async fn match_single_bill(&self, bill_id: i64) -> Result<(), Error> {
        // 步骤 1-6 的实现
    }
}
```

### 数据库查询函数

| 函数 | 用途 | 返回值 |
|------|------|--------|
| `get_bill(pool, bill_id)` | 获取单据主表 | `Option<MatchBill1201>` |
| `list_bill_items(pool, bill_id)` | 获取单据明细 | `Vec<MatchBillItem1201>` |
| `stat_for_product(pool, buyer, seller, code)` | 预统计候选发票 | `CandidateStat` |
| `match_on_invoices(pool, buyer, seller, code, ids)` | 从指定发票查询 | `Vec<MatchedInvoiceItem>` |
| `match_by_tax_and_product(pool, buyer, seller, code)` | 全量候选查询 | `Vec<MatchedInvoiceItem>` |
| `insert_batch(pool, results)` | 批量插入结果 | `Result<(), Error>` |

---

## 数据库查询

### 预统计查询

```sql
-- stat_for_product: 统计候选发票数量和总金额
SELECT
    count(*) as cnt,
    coalesce(sum(vii.famount), 0) as sum_amount
FROM t_sim_vatinvoice_item_1201 vii
INNER JOIN t_sim_vatinvoice_1201 vi ON vi.fid = vii.fid
WHERE vii.fspbm = $1           -- 商品编码
  AND vi.fbuyertaxno = $2      -- 购方税号
  AND vi.fsalertaxno = $3      -- 销方税号
  AND vi.ftotalamount > 0      -- 排除零金额发票
```

### 第二层查询 (全量候选)

```sql
-- match_by_tax_and_product: 按金额降序 (大金额优先)
SELECT
    vii.fid as invoice_id,
    vii.fentryid as item_id,
    vii.fspbm as product_code,
    vii.fnum as quantity,
    vii.famount as amount,
    vii.funitprice as unit_price
FROM t_sim_vatinvoice_item_1201 vii
INNER JOIN t_sim_vatinvoice_1201 vi ON vi.fid = vii.fid
WHERE vii.fspbm = $1
  AND vi.fbuyertaxno = $2
  AND vi.fsalertaxno = $3
  AND vi.ftotalamount > 0
ORDER BY vii.famount DESC   -- 大金额优先
```

### 第一层查询 (已用发票)

```sql
-- match_on_invoices: 按金额升序 (小金额优先)
SELECT
    vii.fid as invoice_id,
    vii.fentryid as item_id,
    vii.fspbm as product_code,
    vii.fnum as quantity,
    vii.famount as amount,
    vii.funitprice as unit_price
FROM t_sim_vatinvoice_item_1201 vii
INNER JOIN t_sim_vatinvoice_1201 vi ON vi.fid = vii.fid
WHERE vii.fspbm = $1
  AND vi.fbuyertaxno = $2
  AND vi.fsalertaxno = $3
  AND vi.ftotalamount > 0
  AND vii.fid = ANY($4)      -- 限定发票ID范围
ORDER BY vii.famount ASC     -- 小金额优先
```

### 批量插入

```sql
-- insert_batch: 动态构建多行 INSERT
INSERT INTO t_sim_match_result_1201 (
    fbillid, fbuyertaxno, fsalertaxno, fspbm,
    finvoiceid, finvoiceitemid, fnum,
    fbillamount, finvoiceamount, fmatchamount,
    fbillunitprice, fbillqty,
    finvoiceunitprice, finvoiceqty, fmatchtime
) VALUES
    ($1, $2, $3, ...),
    ($14, $15, $16, ...),
    ...
```

---

## 性能优化

### 覆盖索引

**问题**: `match_by_tax_and_product` 查询耗时约 3000ms

**解决方案**: 创建覆盖索引，避免回表操作

```sql
-- 覆盖索引 - 包含所有查询需要的字段
CREATE INDEX CONCURRENTLY idx_invoice_item_match_covering
ON t_sim_vatinvoice_item_1201(
    fspbm,          -- WHERE 过滤条件
    fid,            -- JOIN 关联字段
    famount DESC,   -- ORDER BY 排序字段
    fentryid,       -- SELECT 字段
    fnum,           -- SELECT 字段
    funitprice      -- SELECT 字段
);
```

**效果**:

| 指标 | 优化前 | 优化后 | 提升 |
|------|--------|--------|------|
| 执行时间 | ~3000ms | ~30ms | 100x |
| 查询方式 | Bitmap Heap Scan | Index Only Scan | - |
| 回表次数 | 25,148次 | 0次 | 完全消除 |

### 批量操作

**策略**: 每 1000 条记录分批插入

```rust
for chunk in batch.chunks(1000) {
    queries::insert_batch(&self.pool, chunk).await?;
}
```

**好处**:
- 减少数据库往返次数
- 控制单次事务大小
- 避免内存溢出

### 连接池

```rust
PgPoolOptions::new()
    .max_connections(20)              // 最大 20 个连接
    .acquire_timeout(Duration::from_secs(10))
    .connect(database_url)
```

### 异步 I/O

- 使用 Tokio 异步运行时
- 数据库 I/O 等待期间可处理其他请求
- 单线程可处理多个并发请求

---

## 代码示例

### 完整匹配流程示例

```rust
// src/service/matcher.rs - 简化版核心逻辑

pub async fn batch_match_temp_strategy(&self, bill_ids: &[i64]) -> Result<()> {
    for &bill_id in bill_ids {
        // 1. 查询单据
        let Some(bill) = queries::get_bill(&self.pool, bill_id).await? else {
            tracing::warn!("Bill {} not found", bill_id);
            continue;
        };

        let bill_items = queries::list_bill_items(&self.pool, bill_id).await?;
        if bill_items.is_empty() {
            continue;
        }

        // 2. 预统计
        let mut summaries = Vec::with_capacity(bill_items.len());
        for bi in &bill_items {
            let stat = queries::stat_for_product(
                &self.pool,
                &bill.fbuyertaxno,
                &bill.fsalertaxno,
                &bi.fspbm,
            ).await?;

            summaries.push(TempSummary {
                fspbm: bi.fspbm.clone(),
                item_count: stat.cnt,
                total_amount: stat.sum_amount,
            });
        }

        // 3. 稀缺度排序
        summaries.sort_by(|a, b| {
            a.item_count.cmp(&b.item_count)
                .then_with(|| a.total_amount.cmp(&b.total_amount))
        });

        // 4. 初始化状态
        let mut preferred_invoices: IndexSet<i64> = IndexSet::new();
        let mut matched_by_product: HashMap<String, BigDecimal> = HashMap::new();
        let mut batch: Vec<MatchResult1201> = Vec::new();

        // 5. 按稀缺度顺序匹配
        for summary in &summaries {
            let code = &summary.fspbm;

            // 计算目标金额
            let target: BigDecimal = bill_items.iter()
                .filter(|bi| &bi.fspbm == code)
                .map(|bi| &bi.famount)
                .sum();

            let matched = matched_by_product
                .get(code)
                .cloned()
                .unwrap_or_else(BigDecimal::zero);

            let mut remaining = &target - &matched;
            if remaining <= BigDecimal::zero() {
                continue;
            }

            // 构建候选集合
            let mut source = Vec::new();
            let mut seen_item_ids: IndexSet<i64> = IndexSet::new();

            // 第一层: 从 preferred_invoices 查询
            let ids: Vec<i64> = preferred_invoices.iter().cloned().collect();
            for chunk in ids.chunks(1000) {
                let pref = queries::match_on_invoices(
                    &self.pool,
                    &bill.fbuyertaxno,
                    &bill.fsalertaxno,
                    code,
                    chunk,
                ).await?;

                for mi in pref {
                    if seen_item_ids.insert(mi.item_id) {
                        source.push(mi);
                    }
                }
            }

            // 第二层: 全量候选查询
            let general = queries::match_by_tax_and_product(
                &self.pool,
                &bill.fbuyertaxno,
                &bill.fsalertaxno,
                code,
            ).await?;

            for mi in general {
                if seen_item_ids.insert(mi.item_id) {
                    source.push(mi);
                }
            }

            // 贪心填充
            for mi in &source {
                if remaining <= BigDecimal::zero() {
                    break;
                }

                let use_amount = if &mi.amount >= &remaining {
                    remaining.clone()
                } else {
                    mi.amount.clone()
                };

                if use_amount <= BigDecimal::zero() {
                    continue;
                }

                let bi = bill_items.iter().find(|b| &b.fspbm == code).unwrap();

                batch.push(MatchResult1201 {
                    fbillid: bill_id,
                    fbuyertaxno: bill.fbuyertaxno.clone(),
                    fsalertaxno: bill.fsalertaxno.clone(),
                    fspbm: code.clone(),
                    finvoiceid: mi.invoice_id,
                    finvoiceitemid: mi.item_id,
                    fnum: mi.quantity.clone(),
                    fbillamount: bi.famount.clone(),
                    finvoiceamount: mi.amount.clone(),
                    fmatchamount: use_amount.clone(),
                    fbillunitprice: bi.funitprice.clone(),
                    fbillqty: bi.fnum.clone(),
                    finvoiceunitprice: mi.unit_price.clone(),
                    finvoiceqty: Some(mi.quantity.clone()),
                    fmatchtime: Utc::now(),
                });

                preferred_invoices.insert(mi.invoice_id);

                let entry = matched_by_product
                    .entry(code.clone())
                    .or_insert_with(BigDecimal::zero);
                *entry = &*entry + &use_amount;

                remaining = &remaining - &use_amount;
            }
        }

        // 6. 批量插入
        if !batch.is_empty() {
            for chunk in batch.chunks(1000) {
                queries::insert_batch(&self.pool, chunk).await?;
            }
            tracing::info!("Bill {} matched successfully", bill_id);
        }
    }

    Ok(())
}
```

---

## 总结

### 算法特点

1. **稀缺度排序**: 优先处理稀缺商品，避免匹配失败
2. **分层查询**: 复用已匹配发票，减少发票使用数量
3. **保序去重**: IndexSet 实现高效去重且保持顺序
4. **贪心填充**: 简单高效的递进式金额匹配
5. **批量操作**: 1000 条分批，平衡性能和资源

### 性能指标

| 指标 | 数值 |
|------|------|
| 单据处理速度 | ~1000 条/秒 |
| 内存占用 | ~50MB |
| 查询优化后延迟 | ~30ms/次 |

### 约束条件

1. **金额精度**: 使用 BigDecimal，禁止使用浮点数
2. **排序顺序**: 第一层 ASC，第二层 DESC，不可混淆
3. **批量大小**: 固定 1000 条，不可随意修改

---

**文档版本**: 1.0
**最后更新**: 2025-12-18
**适用版本**: tax-redflush-rust v1.0+
