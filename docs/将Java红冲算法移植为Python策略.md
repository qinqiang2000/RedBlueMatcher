# 实现计划：将 Java 红冲算法移植为 Python 策略

## 用户需求确认

- **发票复用范围**：同一分组内复用（无需重构多进程架构）
- **商品排序优化**：需要实现（按蓝票候选数量升序、总金额升序）
- **Python 特性**：保留整数数量优化和尾差校验

---

## 背景分析

### Java 核心算法：batchMatchTempStrategy

**文件**：`tax-redflush-service-java/src/main/java/com/kingdee/taxc/service/RedFlushService.java` (Lines 254-363)

**算法流程**：
1. 统计每个商品的候选蓝票数量和总金额 (`CandidateStat`)
2. 按"行数升序、金额升序"排序商品（稀缺商品优先处理）
3. 维护 `preferredInvoices` 集合，记录已用过的发票ID
4. 匹配时优先从已用发票中查找同商品的其他行
5. 使用 `min(蓝票金额, 剩余目标金额)` 计算每次匹配金额

**关键代码**：
```java
// 已匹配过的发票集合（用于优先复用）
LinkedHashSet<Long> preferredInvoices = new LinkedHashSet<>();

// 构建候选：已匹配发票优先 + 常规候选
if (!preferredInvoices.isEmpty()) {
    List<MatchedInvoiceItem> pref = mapper.matchOnInvoices(..., preferredInvoices);
    for (MatchedInvoiceItem mi : pref) {
        if (seenItemIds.add(mi.getItemId())) { source.add(mi); }
    }
}
List<MatchedInvoiceItem> general = mapper.matchByTaxAndProduct(...);
for (MatchedInvoiceItem mi : general) {
    if (seenItemIds.add(mi.getItemId())) { source.add(mi); }
}

// 贪心匹配
for (MatchedInvoiceItem mi : source) {
    BigDecimal use = mi.getAmount().compareTo(remaining) >= 0 ? remaining : mi.getAmount();
    preferredInvoices.add(mi.getInvoiceId());  // 记录已用发票
    remaining = remaining.subtract(use);
}
```

### Python 现有架构

**文件**：`red_blue_matcher.py` + `strategies/`

- 按 (销方, 购方, SKU, 税率) 分组，多进程并行处理
- 策略通过 `match_single_negative()` 处理单个负数单据
- 支持 `pre_process_negatives()` 预处理钩子

---

## 实现方案

### 新增策略：`invoice_reuse`

**策略名称**：`invoice_reuse`
**类名**：`InvoiceReuseStrategy`

### 核心设计

```
InvoiceReuseStrategy
├── __init__()
│   └── 初始化状态变量
├── name -> "invoice_reuse"
├── set_blue_pool(blue_pool)        # 新增：设置蓝票池上下文
│   └── 计算候选统计，用于排序
├── pre_process_negatives(negatives)
│   ├── 重置状态：_preferred_invoices.clear()
│   └── 按候选统计排序负数单据（稀缺优先）
└── match_single_negative(...)
    ├── 重排序候选：已用发票优先
    ├── 按 (fid, fentryid) 去重
    ├── 贪心匹配 + 整数数量优化 + 尾差校验
    └── 记录已用发票到 _preferred_invoices
```

### 状态管理

```python
class InvoiceReuseStrategy:
    def __init__(self):
        self._preferred_invoices: Set[int] = set()  # 已用发票ID
        self._blue_pool = None  # 蓝票池引用（用于计算统计）
        self._sku_candidate_stats: Dict[str, Tuple[int, Decimal]] = {}  # {sku: (count, total_amount)}
```

---

## 详细实现步骤

### Step 1: 修改策略基类

**文件**：`strategies/base.py`

添加可选方法 `set_blue_pool()`：

```python
def set_blue_pool(self, blue_pool: Dict[Tuple[str, str], List]) -> None:
    """
    设置蓝票池上下文（可选）

    在批量匹配开始前调用，允许策略访问完整的蓝票池信息。
    默认实现：不做任何操作。
    """
    pass
```

### Step 2: 创建新策略文件

**文件**：`strategies/invoice_reuse.py`

**完整实现要点**：

1. **候选统计计算**（在 `set_blue_pool()` 中）：
   ```python
   def set_blue_pool(self, blue_pool):
       self._blue_pool = blue_pool
       self._sku_candidate_stats.clear()
       for (spbm, taxrate), candidates in blue_pool.items():
           valid = [b for b in candidates if b.current_remain_amount > 0]
           count = len(valid)
           total = sum(b.current_remain_amount for b in valid)
           self._sku_candidate_stats[(spbm, taxrate)] = (count, total)
   ```

2. **稀缺度排序**（在 `pre_process_negatives()` 中）：
   ```python
   def pre_process_negatives(self, negatives):
       self._preferred_invoices.clear()

       def sort_key(neg):
           key = (neg.fspbm, neg.ftaxrate)
           count, total = self._sku_candidate_stats.get(key, (999999, Decimal('999999')))
           return (count, total)  # 行数少优先，总金额小优先

       return sorted(negatives, key=sort_key)
   ```

3. **发票复用匹配**（在 `match_single_negative()` 中）：
   ```python
   # 重排序：已用发票优先
   preferred = []
   others = []
   seen_items = set()

   for blue in candidates:
       item_key = (blue.fid, blue.fentryid)
       if item_key in seen_items:
           continue
       seen_items.add(item_key)

       if blue.fid in self._preferred_invoices:
           preferred.append(blue)
       else:
           others.append(blue)

   sorted_candidates = preferred + others

   # 贪心匹配（复用 greedy_large 的整数优化逻辑）
   for blue in sorted_candidates:
       # ... 匹配逻辑
       self._preferred_invoices.add(blue.fid)  # 记录已用
   ```

### Step 3: 修改主流程

**文件**：`red_blue_matcher.py`

在 `match_group_worker()` 中调用 `set_blue_pool()`：

```python
def match_group_worker(args):
    group_key, neg_items_data, blue_candidates_data, strategy_name = args

    strategy = get_strategy(strategy_name)
    neg_items = [NegativeItem(**d) for d in neg_items_data]
    blue_candidates = [BlueInvoiceItem(**d) for d in blue_candidates_data]

    temp_pool = {(group_key[2], group_key[3]): blue_candidates}

    # 新增：设置蓝票池上下文
    if hasattr(strategy, 'set_blue_pool'):
        strategy.set_blue_pool(temp_pool)

    neg_items = strategy.pre_process_negatives(neg_items)
    # ... 其余逻辑不变
```

### Step 4: 注册新策略

**文件**：`strategies/__init__.py`

```python
from .invoice_reuse import InvoiceReuseStrategy

STRATEGIES = {
    'greedy_large': GreedyLargeStrategy,
    'ffd': FFDStrategy,
    'invoice_reuse': InvoiceReuseStrategy,  # 新增
}
```

---

## 涉及文件清单

| 文件 | 操作 | 改动量 |
|------|------|--------|
| `strategies/invoice_reuse.py` | **新建** | ~200行 |
| `strategies/__init__.py` | **修改** | +3行 |
| `strategies/base.py` | **修改** | +10行 |
| `red_blue_matcher.py` | **修改** | +5行 |

---

## 测试验证

```bash
# 使用新策略运行（测试模式）
python red_blue_matcher.py --algorithm invoice_reuse --test-limit 100

# 对比不同策略
python red_blue_matcher.py --algorithm greedy_large --test-limit 100 --output greedy.xlsx
python red_blue_matcher.py --algorithm invoice_reuse --test-limit 100 --output reuse.xlsx
```

---

## 算法对比

| 特性 | greedy_large | ffd | invoice_reuse (新) |
|------|-------------|-----|-------------------|
| 精确匹配优先 | NumPy向量化 | 否 | NumPy向量化 |
| 发票复用 | 否 | 否 | **是** |
| 商品排序 | 无 | 负数降序 | **稀缺优先** |
| 整数数量优化 | 是 | 是 | 是 |
| 尾差校验 | 是 | 是 | 是 |
| 来源 | Python原生 | Python原生 | **Java移植** |
