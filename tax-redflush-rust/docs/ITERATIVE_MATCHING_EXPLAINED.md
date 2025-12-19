# 迭代匹配详解 (Phase 2: Iterative Matching Explained)

**文档版本**: 2025-12-19
**目的**: 详细解释 MATCHING_ALGORITHM_V2_1.md 中的阶段2迭代匹配，并说明其与核心数据结构的对应关系

---

## 问题背景

阶段2的伪代码看起来很简洁，但实际上每一行都紧密依赖于第1节定义的数据结构。本文档通过具体示例，逐行解释迭代匹配的工作原理。

---

## 1. 数据结构回顾（来自第1节）

### 1.1 输入数据

| 数据结构 | 类型 | 说明 | 示例 |
|---------|------|------|------|
| **待匹配单据** | `Map<SkuCode, RequiredAmount>` | 需要红冲的金额 | `{SKU_A: 100, SKU_B: 200}` |
| **候选发票池** | `List<Invoice>` | 可用的蓝票 | `Invoice{id:1, items:[{sku:A, amt:80}]}` |

### 1.2 运行时状态（关键！）

| 数据结构 | 类型 | 作用 | 构建时机 |
|---------|------|------|---------|
| **倒排索引** | `Map<SkuCode, Set<InvoiceID>>` | 快速查找：哪些发票包含某个SKU | 预处理阶段 |
| **全局频率表** | `Map<SkuCode, Integer>` | 计算稀缺性：每个SKU出现在多少张发票中 | 预处理阶段 |
| **需求跟踪器** | `Map<SkuCode, RemainingAmount>` | 实时追踪：每个SKU还差多少金额 | 初始=待匹配单据，迭代中动态更新 |

---

## 2. 完整示例：一次迭代的全过程

### 📥 初始状态

**待匹配单据（需求跟踪器初始值）**:
```
SKU_A: 需要 100 元
SKU_B: 需要 200 元
SKU_C: 需要 50 元
```

**候选发票池**:
```
Invoice_1: {SKU_A: 80元, SKU_B: 150元}
Invoice_2: {SKU_A: 50元, SKU_C: 60元}
Invoice_3: {SKU_B: 100元, SKU_D: 200元}
Invoice_4: {SKU_D: 300元}
```

**倒排索引**（预处理时构建）:
```
SKU_A -> {Invoice_1, Invoice_2}
SKU_B -> {Invoice_1, Invoice_3}
SKU_C -> {Invoice_2}
SKU_D -> {Invoice_3, Invoice_4}
```

**全局频率表**（预处理时构建）:
```
SKU_A: 2  (出现在 Invoice_1, Invoice_2)
SKU_B: 2  (出现在 Invoice_1, Invoice_3)
SKU_C: 1  (出现在 Invoice_2) ← 稀缺！
SKU_D: 2  (出现在 Invoice_3, Invoice_4)
```

---

### 🔄 第一轮迭代（详细拆解）

#### Step 1: 检查循环条件（第 54 行）

```python
while not requirements.is_satisfied():
```

**使用数据**: 需求跟踪器
**当前状态**: `{SKU_A: 100, SKU_B: 200, SKU_C: 50}`
**判断结果**: 还有未满足的需求 → 继续循环

---

#### Step 2: 确定候选范围（第 58-59 行）⭐ 核心优化

```python
candidate_invoices = get_candidates_from_index(requirements.needed_skus)
```

**使用数据**: 倒排索引 + 需求跟踪器

**详细过程**:
1. 从需求跟踪器获取当前所需的SKU: `{SKU_A, SKU_B, SKU_C}`
2. 查询倒排索引:
   ```
   SKU_A -> {Invoice_1, Invoice_2}
   SKU_B -> {Invoice_1, Invoice_3}
   SKU_C -> {Invoice_2}
   ```
3. 合并结果（去重）: `candidate_invoices = {Invoice_1, Invoice_2, Invoice_3}`

**关键点**:
- ✅ Invoice_4 **不会被搜索**，因为它只包含 SKU_D，而我们不需要 SKU_D
- ✅ 这就是倒排索引的价值：避免遍历所有发票（假如有3万张）

**如果没有倒排索引会怎样？**
```python
# 低效做法（暴力搜索）
candidate_invoices = []
for invoice in all_invoices:  # 遍历所有3万张发票
    for item in invoice.items:
        if item.sku in requirements.needed_skus:
            candidate_invoices.append(invoice)
            break
```

---

#### Step 3: 评分选优（第 62-70 行）

```python
for invoice in candidate_invoices:
    score = calculate_score(invoice, requirements, frequency_map)
```

**使用数据**: 候选发票池 + 需求跟踪器 + 全局频率表

**逐个计算分数**:

##### Invoice_1 评分:
- **可匹配的SKU**: SKU_A (80), SKU_B (150)
- **MatchAmount**:
  - `min(80, 100)` = 80  (SKU_A可用80，需要100，取80)
  - `min(150, 200)` = 150  (SKU_B可用150，需要200，取150)
  - 总计: 80 + 150 = **230 元**
- **ScarcityScore**:
  - SKU_A: `1000 / 2` = 500 分
  - SKU_B: `1000 / 2` = 500 分
  - 总计: **1000 分**
- **Total Score**: 230 + 1000 = **1230 分**

##### Invoice_2 评分:
- **可匹配的SKU**: SKU_A (50), SKU_C (60)
- **MatchAmount**:
  - `min(50, 100)` = 50
  - `min(60, 50)` = 50
  - 总计: 50 + 50 = **100 元**
- **ScarcityScore**:
  - SKU_A: `1000 / 2` = 500 分
  - SKU_C: `1000 / 1` = **1000 分** ← 稀缺SKU！
  - 总计: **1500 分**
- **Total Score**: 100 + 1500 = **1600 分** ⭐ 最高！

##### Invoice_3 评分:
- **可匹配的SKU**: SKU_B (100)
- **MatchAmount**: 100 元
- **ScarcityScore**: `1000 / 2` = 500 分
- **Total Score**: 100 + 500 = **600 分**

**选择结果**: Invoice_2 (1600分) 胜出！

**为什么 Invoice_2 胜出？**
- 虽然 Invoice_1 匹配金额更多（230 vs 100）
- 但 Invoice_2 包含稀缺的 SKU_C（只有这一张发票有）
- 稀缺性加分 (1500) 使得总分超过 Invoice_1
- **策略目的**: 优先消费稀缺资源，避免后续无法匹配

---

#### Step 4: 消费扣减（第 76-77 行）

```python
consume(best_invoice, requirements)
```

**使用数据**: 候选发票池 + 需求跟踪器

**具体操作**（针对选中的 Invoice_2）:

**扣减需求跟踪器**:
```
SKU_A: 100 -> 50   (消费了50元)
SKU_B: 200 -> 200  (无变化，Invoice_2没有SKU_B)
SKU_C: 50 -> 0     (完全满足，消费了50元)
```

**扣减候选发票池中的 Invoice_2**:
```
Invoice_2 剩余金额:
  SKU_A: 50 -> 0    (全部用完)
  SKU_C: 60 -> 10   (用了50，还剩10)
```

**更新倒排索引**（可选优化）:
- 如果 Invoice_2 的 SKU_A 用完了，可以从倒排索引中移除:
  ```
  SKU_A -> {Invoice_1, Invoice_2} 变为 {Invoice_1}
  ```

---

### 🔄 第二轮迭代（简述）

**当前需求跟踪器**: `{SKU_A: 50, SKU_B: 200}`

#### Step 1: 获取候选
```python
needed_skus = {SKU_A, SKU_B}
candidate_invoices = {Invoice_1, Invoice_3}  # 通过倒排索引查询
```

#### Step 2: 评分
- Invoice_1: 匹配 80+150=230，但 SKU_A 只能用 50，实际匹配 50+150=200，稀缺性 1000，总分 1200
- Invoice_3: 匹配 100，稀缺性 500，总分 600

**选择**: Invoice_1

#### Step 3: 消费
- SKU_A: 50 -> 0 ✅
- SKU_B: 200 -> 50

---

### 🔄 第三轮迭代

**当前需求跟踪器**: `{SKU_B: 50}`

- 查倒排索引: SKU_B -> {Invoice_3} (Invoice_1已用过)
- 选择 Invoice_3，匹配 50 元
- SKU_B: 50 -> 0 ✅

---

### ✅ 循环结束

```python
requirements.is_satisfied() == True
```

**结果统计**:
- 使用发票数: 3 张 (Invoice_2, Invoice_1, Invoice_3)
- 匹配覆盖率: 100%

---

## 3. 数据结构的协同工作

| 步骤 | 使用的数据结构 | 作用 |
|-----|-------------|------|
| **循环条件判断** | 需求跟踪器 | 检查是否还有未满足的需求 |
| **候选范围确定** | 倒排索引 + 需求跟踪器 | 快速定位相关发票，避免全量搜索 |
| **发票评分** | 候选发票池 + 需求跟踪器 + 全局频率表 | 计算匹配价值和稀缺性加分 |
| **消费扣减** | 需求跟踪器 + 候选发票池 | 更新剩余需求和发票余额 |

**关键理解**:
1. **倒排索引** 是性能优化的核心：从 O(N*M) 降到 O(K)，其中 N=发票总数，M=SKU总数，K=相关发票数
2. **全局频率表** 是策略优化的核心：让算法"智能"地优先消费稀缺资源
3. **需求跟踪器** 是状态管理的核心：实时反映匹配进度，驱动循环终止

---

## 4. 常见疑问解答

### Q1: 为什么不直接遍历所有发票？
**A**: 在真实场景中，候选发票可能有 3万+ 张，但每次迭代只需要搜索包含"当前所需SKU"的发票。例如：
- 待匹配单据有 50 个 SKU
- 每个 SKU 平均出现在 100 张发票中
- 候选范围 = 50 * 100 = 5000 张（去重后可能更少）
- 避免了搜索剩余的 25000 张无关发票

### Q2: 稀缺性加分会不会太武断？
**A**: 稀缺性加分是经过权衡的设计：
- **问题**: 如果只按金额贪心，可能会因为金额优势错过稀缺SKU，导致后续无法匹配
- **解决**: 给稀缺SKU虚拟增加价值（1000分 ≈ 1000元）
- **效果**: 即使匹配金额少一点，稀缺发票也能胜出
- **调优**: 权重因子（默认1000）可根据业务场景调整

### Q3: 如果两张发票分数相同怎么办？
**A**: 第 68-70 行有 Tie-breaker 逻辑：
```python
elif score == max_score and invoice.sku_count > best_invoice.sku_count:
    best_invoice = invoice
```
优先选择覆盖SKU数量更多的发票。

### Q4: 会不会出现死循环？
**A**: 不会，因为：
1. 每次迭代都会消费至少一个SKU的至少一部分金额
2. 如果找不到候选发票（第73行），直接 break
3. 可以设置安全上限（如 max_iterations=5000）

---

## 5. 与实际代码的对应关系

如果你在看 Rust 实现代码，可以参考以下映射：

| 伪代码概念 | Rust 结构体/函数 |
|----------|---------------|
| `requirements` | `BillRequirement` / `needs` HashMap |
| `倒排索引` | `inverted_index: HashMap<String, HashSet<String>>` |
| `全局频率表` | `sku_frequency_map: HashMap<String, usize>` |
| `get_candidates_from_index()` | 遍历 `needed_skus`，合并 `inverted_index[sku]` |
| `calculate_score()` | 计算 `match_amount + scarcity_bonus` |
| `consume()` | 更新 `needs` 和 `invoice.remaining_amount` |

---

## 6. 总结

**迭代匹配的本质**: 一个数据结构协同工作的贪心循环

```
┌─────────────────────────────────────────────────┐
│  While (需求跟踪器 未满足)                        │
│    ↓                                            │
│  [倒排索引] 快速定位候选发票                      │
│    ↓                                            │
│  [全局频率表] 计算稀缺性加分                      │
│    ↓                                            │
│  [候选发票池] 提供金额匹配数据                    │
│    ↓                                            │
│  选出最佳发票，消费并更新 [需求跟踪器]            │
│    ↓                                            │
│  循环 ────────────────────────┘                 │
└─────────────────────────────────────────────────┘
```

**为什么设计得这么复杂？**
- **倒排索引**: 处理海量发票（3万+）时的性能保障
- **稀缺性加权**: 避免贪心算法的短视问题，提高覆盖率
- **动态跟踪**: 实时更新状态，支持部分消费和多轮迭代

---

*Generated by Claude - 2025-12-19*
*关联文档: MATCHING_ALGORITHM_V2_1.md*
