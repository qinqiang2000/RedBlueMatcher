# 算法选择与扩展指南

本文档说明如何使用不同的匹配算法，以及如何自定义实现新算法。

## 快速开始

### 使用不同算法

```bash
# 使用默认算法 (greedy_large)
python red_blue_matcher.py

# 显式指定算法
python red_blue_matcher.py --algorithm greedy_large

# 测试模式 + 指定算法
python red_blue_matcher.py --test-limit 100 --algorithm greedy_large --output test_results.xlsx
```

### 查看可用算法

```bash
python red_blue_matcher.py --help
```

## 当前支持的算法

### 1. greedy_large（默认）

**算法名称**：贪心大额优先匹配策略

**特点**：
- ✓ 优先精确匹配：使用 NumPy 向量化查找金额完全匹配的蓝票
- ✓ 贪心消耗：按蓝票金额从大到小消耗
- ✓ 整数数量优先：尽量使红冲数量为整数
- ✓ 吃光策略：对余额极小的蓝票进行清零处理

**适用场景**：
- 标准的负数发票匹蓝场景
- 需要平衡匹配效率和红冲金额精度的场景

**使用方式**：
```bash
python red_blue_matcher.py --algorithm greedy_large
```

**性能参考**（50条负数单据）：
- 平均耗时：约 20 秒
- 并发度：多进程并行处理

## 架构设计

### 策略模式（Strategy Pattern）

系统采用策略模式来支持多算法切换：

```
┌─────────────────────┐
│  MatchingStrategy   │  (抽象基类)
│  - match()          │
└──────────┬──────────┘
           │
    ┌──────┴──────┐
    │             │
┌───▼────┐   ┌───▼────┐
│Greedy  │   │Future  │
│Large   │   │Algos   │
│Strategy│   │........│
└────────┘   └────────┘
```

### 目录结构

```
RedBlueMatcher/
├── red_blue_matcher.py         # 主程序（算法入口）
├── strategies/                  # 策略模块
│   ├── __init__.py             # 工厂函数和注册表
│   ├── base.py                 # 抽象基类
│   └── greedy_large.py         # 现有算法实现
└── docs/
    └── ALGORITHM_SELECTION.md  # 本文档
```

## 自定义算法

### Step 1: 创建新算法文件

在 `strategies/` 目录下创建新文件，如 `strategies/ffd_packing.py`：

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
首次适应递减打包算法 (First Fit Decreasing)
"""

from decimal import Decimal
from typing import List, Dict, Tuple

from .base import MatchingStrategy

class FFDPackingStrategy(MatchingStrategy):
    """首次适应递减打包算法"""

    @property
    def name(self) -> str:
        return "ffd_packing"

    def pre_process_negatives(self, negatives: List) -> List:
        """
        预处理：按金额降序排列（FFD 的关键步骤）
        """
        return sorted(negatives, key=lambda x: abs(x.famount), reverse=True)

    def match_single_negative(
        self,
        negative,
        blue_pool: Dict[Tuple[str, str], List],
        results: List,
        seq_counter: List[int],
        skip_validation: bool = False
    ) -> Tuple[bool, str]:
        """
        匹配逻辑：使用首次适应（选择第一个足够大的蓝票）
        """
        # 实现你的算法逻辑
        # ...
        pass
```

### Step 2: 注册算法

编辑 `strategies/__init__.py`，添加导入和注册：

```python
from .ffd_packing import FFDPackingStrategy

STRATEGIES = {
    'greedy_large': GreedyLargeStrategy,
    'ffd_packing': FFDPackingStrategy,  # 新增
}
```

### Step 3: 实现接口

#### 实现必需方法

1. **`name` 属性**
   ```python
   @property
   def name(self) -> str:
       return "ffd_packing"
   ```

2. **`match_single_negative()` 方法**
   ```python
   def match_single_negative(
       self,
       negative: 'NegativeItem',
       blue_pool: Dict[Tuple[str, str], List['BlueInvoiceItem']],
       results: List['MatchResult'],
       seq_counter: List[int],
       skip_validation: bool = False
   ) -> Tuple[bool, str]:
       """
       为单个负数明细匹配蓝票

       Args:
           negative: 负数单据明细
           blue_pool: 蓝票池 {(spbm, taxrate): [BlueInvoiceItem]}
           results: 匹配结果列表（直接追加结果）
           seq_counter: 序号计数器 [当前序号]
           skip_validation: 是否跳过尾差校验

       Returns:
           (是否匹配成功, 失败原因)
       """
       # 你的匹配算法实现
       pass
   ```

#### 实现可选方法

```python
def pre_process_negatives(self, negatives: List) -> List:
    """
    预处理负数单据列表（可选）

    示例：按金额降序排列用于 FFD 算法
    """
    return sorted(negatives, key=lambda x: abs(x.famount), reverse=True)
```

### Step 4: 测试新算法

```bash
# 测试新算法（处理前10条）
python red_blue_matcher.py --test-limit 10 --algorithm ffd_packing --output ffd_test.xlsx

# 对比两种算法的输出
python count_red_invoices.py greedy_large_results.xlsx
python count_red_invoices.py ffd_test.xlsx
```

## 关键数据结构

### NegativeItem（负数单据明细）

```python
@dataclass
class NegativeItem:
    fid: int                # 单据主表ID
    fentryid: int          # 明细行ID
    fbillno: str           # 单据编号
    fspbm: str             # 商品编码
    fgoodsname: str        # 商品名称
    ftaxrate: str          # 税率
    famount: Decimal       # 金额(负数)
    fnum: Decimal          # 数量(负数)
    ftax: Decimal          # 税额(负数)
    fsalertaxno: str       # 销方税号
    fbuyertaxno: str       # 购方税号
```

### BlueInvoiceItem（蓝票明细行）

```python
@dataclass
class BlueInvoiceItem:
    fid: int                            # 发票主表ID
    fentryid: int                       # 明细行ID
    finvoiceno: str                     # 发票号码
    fspbm: str                          # 商品编码
    fgoodsname: str                     # 商品名称
    ftaxrate: str                       # 税率
    fitemremainredamount: Decimal       # 剩余可红冲金额
    fitemremainrednum: Decimal          # 剩余可红冲数量
    fredprice: Decimal                  # 可红冲单价
    fissuetime: datetime                # 开票时间
    # 动态余额（在匹配过程中更新）
    _current_remain_amount: Decimal
    _current_remain_num: Decimal

    @property
    def current_remain_amount(self) -> Decimal:
        """当前剩余金额"""
        return self._current_remain_amount

    def deduct(self, amount: Decimal, num: Decimal):
        """扣减余额"""
        self._current_remain_amount -= amount
        self._current_remain_num -= num
```

### MatchResult（匹配结果）

```python
@dataclass
class MatchResult:
    seq: int                        # 序号
    sku_code: str                   # SKU编码
    blue_fid: int                   # 蓝票fid
    blue_entryid: int               # 蓝票行号
    remain_amount_before: Decimal   # 匹配前剩余可红冲金额
    unit_price: Decimal             # 可红冲单价
    matched_amount: Decimal         # 本次红冲金额(正数)
    negative_fid: int               # 负数单据fid
    negative_entryid: int           # 负数单据行号
    blue_invoice_no: str            # 蓝票号码
    goods_name: str                 # 商品名称
    fissuetime: datetime            # 蓝票开票日期
```

## 最佳实践

### 1. 算法命名规范

```python
# 好的名称
'greedy_large'      # 描述算法特点
'ffd_packing'       # 使用业界公认的算法名称

# 避免
'algo1'             # 过于通用
'my_algorithm'      # 太个性化
'test'              # 容易混淆
```

### 2. 预处理钩子

```python
def pre_process_negatives(self, negatives: List) -> List:
    """
    利用预处理实现算法特定的优化

    示例场景：
    - FFD: 按金额降序排列
    - LPT: 按数量降序排列
    - 分组: 按类别预分组
    """
    # 不修改原列表，返回新列表
    return sorted(negatives, key=...)
```

### 3. 性能考虑

```python
# NumPy 向量化（快）
import numpy as np
amounts = np.array([...])
matches = np.where(amounts >= target)[0]

# Python 循环（慢）
matches = [i for i, amt in enumerate(amounts) if amt >= target]
```

### 4. 错误处理

```python
def match_single_negative(self, negative, blue_pool, ...):
    match_key = (negative.fspbm, negative.ftaxrate)

    if match_key not in blue_pool:
        # 返回清晰的失败原因
        reason = f"找不到匹配的蓝票 - SKU: {negative.fspbm}"
        return False, reason

    candidates = blue_pool[match_key]
    # ... 继续匹配逻辑
```

## 常见问题

### Q: 为什么选择策略模式？

**A:** 策略模式提供以下优势：
- 算法切换无需修改主程序
- 易于添加新算法而无需改动现有代码
- 便于对比测试不同算法
- 符合开闭原则（对扩展开放，对修改关闭）

### Q: 新算法会影响现有功能吗？

**A:** 不会。新算法与现有算法完全独立，只要实现了 `MatchingStrategy` 接口即可。

### Q: 如何测试新算法？

**A:**
```bash
# 1. 小数据量测试
python red_blue_matcher.py --test-limit 100 --algorithm new_algo

# 2. 对比输出
python count_red_invoices.py output1.xlsx
python count_red_invoices.py output2.xlsx

# 3. 验证数据一致性（金额总和、红冲票数等）
```

### Q: 新算法可以改变匹配结果的结构吗？

**A:** `MatchResult` 的结构是固定的，以保证输出报表的一致性。如需扩展，应添加新字段而不是修改现有字段。

## 算法性能对标

| 算法 | 特点 | 适用场景 | 复杂度 |
|------|------|---------|--------|
| greedy_large | 贪心、快速 | 通用场景 | O(n²) |
| ffd_packing | 打包优化 | 最小化红票数 | O(n log n) |
| （待实现）| ... | ... | ... |

## 相关资源

- 策略模式：https://refactoring.guru/design-patterns/strategy
- Python dataclass：https://docs.python.org/3/library/dataclasses.html
- NumPy 向量化：https://numpy.org/doc/stable/user/basics.broadcasting.html
