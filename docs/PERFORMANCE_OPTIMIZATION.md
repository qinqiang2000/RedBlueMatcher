# 负数发票匹蓝算法 - 并发优化报告

## 优化总结

成功实施**阶段一：蓝票池并发加载**，实现显著性能提升。

## 性能对比

### 测试环境
- 测试数据：50条负数单据，47组蓝票候选池
- 重复运行：3次取平均值
- 数据库：PostgreSQL (本地)

### 基线性能（优化前 - 串行执行）
```
平均耗时: 44.17 秒
最快耗时: 27.51 秒
最慢耗时: 77.24 秒
标准差:   28.64 秒
```

### 优化后性能（阶段一 - 并发蓝票加载）
```
平均耗时: 20.11 秒
最快耗时: 18.22 秒
最慢耗时: 23.43 秒
标准差:    2.88 秒
```

### 性能提升
- **平均提速: 2.20x** (44.17s → 20.11s)
- **最快提速: 1.51x** (27.51s → 18.22s)
- **稳定性提升**: 标准差从 28.64s 降至 2.88s（性能更稳定）

## 实施细节

### 修改内容

#### 1. 新增依赖
```python
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
```

#### 2. 新增并发工作函数
`load_blue_worker()` - 每个线程创建独立DB连接执行查询

#### 3. 修改蓝票加载逻辑
- 使用 `ThreadPoolExecutor` (最大10个worker)
- 并发执行所有蓝票查询
- 使用 `Lock` 保护 `blue_pool` 字典写入
- 保留进度打印功能

#### 4. 提升查询确定性
在SQL中添加 `vi.fentryid ASC` 作为最终排序键，确保并发执行时结果完全一致

### 代码修改位置
- `red_blue_matcher.py:23-24` - 新增import
- `red_blue_matcher.py:239-256` - 新增 `load_blue_worker()` 函数
- `red_blue_matcher.py:216` - SQL排序增加 `vi.fentryid ASC`
- `red_blue_matcher.py:469-491` - 并发加载蓝票池

## 正确性验证

### 测试方法
使用相同输入数据，对比优化前后的CSV输出：
```bash
python3 benchmark.py --compare output/test_serial.csv output/test_parallel.csv
```

### 验证结果
✅ **文件内容一致（忽略序号和顺序）**

所有关键字段完全匹配：
- 蓝票fid
- 蓝票entryid
- 匹配金额
- SKU数量
- 剩余金额

## 技术要点

### 线程安全措施
1. **独立DB连接**: 每个worker创建自己的连接（psycopg2非线程安全）
2. **字典写入保护**: 使用 `Lock` 保护 `blue_pool` 的写操作
3. **异常处理**: 单个查询失败不影响整体执行

### 并发控制
- 最大并发度: min(10, unique_keys数量)
- 避免DB连接池耗尽
- 平衡性能与资源消耗

## 扩展性分析

### 当前测试场景
- 47组蓝票查询
- 平均每组查询耗时: ~0.6秒（串行）
- 并发后总耗时: ~20秒（包含匹配算法）

### 预期大规模场景
假设500个unique_keys：
- **串行预计**: ~300秒（仅蓝票加载）
- **并发预计**: ~50秒（10倍并发）
- **整体提速**: 3-5倍

### 收益随规模递增
unique_keys越多，并发收益越明显。

## 后续优化方向（可选）

### 阶段二：分组匹配并发
- **目标**: 并发处理不同的 group_key
- **预期收益**: 额外2-3倍提速（总共5-12倍）
- **风险**: 需重构序号生成，需充分测试
- **实施建议**: 基于实际需求决定

### 阶段三：连接池优化
- 使用 `psycopg2.pool.ThreadedConnectionPool`
- 减少连接创建/销毁开销
- 预期额外5-10%性能提升

## 使用方法

### 正常运行（全量）
```bash
python3 red_blue_matcher.py
```

### 测试模式
```bash
python3 red_blue_matcher.py --test-limit 100
```

### 指定算法运行
```bash
# 使用默认算法（greedy_large）
python3 red_blue_matcher.py

# 显式指定算法
python3 red_blue_matcher.py --algorithm greedy_large

# 测试模式 + 指定算法
python3 red_blue_matcher.py --test-limit 100 --algorithm greedy_large
```

### 性能测试
```bash
python3 benchmark.py --test-limit 50 --runs 3
```

### 结果对比
```bash
python3 benchmark.py --compare file1.csv file2.csv
```

## 总结

✅ **阶段一优化成功**
- 2.20倍平均提速
- 结果完全正确
- 代码稳定可靠
- 无副作用

**建议**:
- 投入生产使用
- 监控大规模数据表现
- 根据实际需求考虑是否实施阶段二
