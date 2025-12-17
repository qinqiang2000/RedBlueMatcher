# 发票匹配查询性能优化

## 优化日期
2025-12-17

## 问题描述

Rust 服务日志中频繁出现慢查询告警，主要查询 `match_by_tax_and_product` 执行时间超过 4 秒，远超 1 秒阈值。

### 慢查询特征
- 查询函数: `match_by_tax_and_product` (queries.rs:65-93)
- 典型执行时间: 2,957 - 4,051 毫秒
- 返回行数: 4,209 - 30,222 行
- 问题: 需要扫描 25,148 个数据块进行回表查询

## 根因分析

### 查询模式
```sql
SELECT vii.fid, vii.fentryid, vii.fspbm, vii.fnum, vii.famount, vii.funitprice
FROM t_sim_vatinvoice_item_1201 vii
INNER JOIN t_sim_vatinvoice_1201 vi ON vi.fid = vii.fid
WHERE vii.fspbm = $1
  AND vi.fbuyertaxno = $2
  AND vi.fsalertaxno = $3
  AND vi.ftotalamount > 0
ORDER BY vii.famount DESC
```

### 性能瓶颈

**优化前的执行计划:**
```
Hash Join (cost=2455.78..96600.95 rows=27856 width=42) (actual time=20.187..2936.991 rows=30222 loops=1)
  -> Bitmap Heap Scan on t_sim_vatinvoice_item_1201 vii  (cost=328.34..94400.38)
     Recheck Cond: ((fspbm)::text = '1016393'::text)
     Heap Blocks: exact=25148  -- 大量回表操作
```

**问题:**
1. 使用单列索引 `t_sim_vatinvoice_item_1201_fspbm_idx`
2. 需要回表读取其他字段（fid, fentryid, fnum, famount, funitprice）
3. 扫描 25,148 个数据块，I/O 开销巨大

## 优化方案

### 创建覆盖索引

```sql
CREATE INDEX CONCURRENTLY idx_invoice_item_match_covering
ON t_sim_vatinvoice_item_1201(
    fspbm,          -- WHERE 过滤
    fid,            -- JOIN 关联
    famount DESC,   -- ORDER BY 排序
    fentryid,       -- SELECT 字段
    fnum,           -- SELECT 字段
    funitprice      -- SELECT 字段
);
```

### 索引特点
- **类型**: 覆盖索引（Covering Index）
- **大小**: 1,170 MB
- **优势**:
  - 包含查询所需的所有字段，避免回表
  - 支持按金额降序直接排序
  - 支持 Index Only Scan

## 优化效果

### 性能对比

| 指标 | 优化前 | 优化后 | 提升 |
|------|--------|--------|------|
| 执行时间 | 2,957 ms | 21-33 ms | **92 倍** |
| 扫描方式 | Bitmap Heap Scan | Index Only Scan | - |
| 数据块读取 | 25,148 blocks | 0 blocks (无回表) | - |
| Join 方式 | Hash Join | Merge Join | - |

### 执行计划对比

**优化后:**
```
Merge Join (cost=0.97..3748.17 rows=30279 width=42) (actual time=0.061..17.103 rows=30222 loops=1)
  -> Index Only Scan using idx_invoice_item_match_covering
     Index Cond: (fspbm = '1016393'::text)
     Heap Fetches: 0  -- 无回表操作
```

### 测试数据

**高频查询参数测试:**
- 产品代码: 1016393
- 购方税号: 9134110275298062X0
- 销方税号: 91341103MA2TWC9B1Q
- 返回行数: 30,222 行
- 执行时间: 21.48 ms ✓

## 部署说明

### 1. 应用迁移文件

```bash
psql -h <host> -U <user> -d <database> -f migrations/001_optimize_invoice_queries.sql
```

### 2. 验证索引创建

```bash
psql -h <host> -U <user> -d <database> -c "
SELECT indexname, pg_size_pretty(pg_relation_size(indexname::regclass))
FROM pg_indexes
WHERE tablename = 't_sim_vatinvoice_item_1201'
AND indexname = 'idx_invoice_item_match_covering';
"
```

### 3. 运行性能检查

```bash
cd tax-redflush-rust
./scripts/check_query_performance.sh
```

### 4. 监控日志

```bash
tail -f logs/rust-service-optimized.log | grep "slow statement"
```

## 注意事项

### 磁盘空间
- 索引大小: 1,170 MB
- 部署前请确保有足够的磁盘空间

### 写入性能
- 覆盖索引会增加写入开销（INSERT/UPDATE）
- 对于读多写少的场景，性能收益远大于成本
- 本场景中写入频率低，优化合理

### 并发创建
- 使用 `CREATE INDEX CONCURRENTLY` 避免锁表
- 创建过程中不影响线上服务
- 预计创建时间: 1-5 分钟（取决于数据量）

## 相关查询优化

除了主匹配查询，以下查询也受益于此索引:

### 1. 统计查询 (stat_for_product)
- 执行时间: ~30 ms
- 状态: ✓ 良好

### 2. 复用查询 (match_on_invoices)
- 执行时间: <1 ms
- 状态: ✓ 优秀

## 监控建议

### 1. 定期运行性能检查
```bash
# 建议每周运行一次
./scripts/check_query_performance.sh
```

### 2. 监控慢查询日志
```bash
# 检查最近的慢查询
tail -1000 logs/rust-service-optimized.log | grep "slow statement" | wc -l
```

### 3. 索引使用率监控
```sql
SELECT
    schemaname,
    tablename,
    indexname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes
WHERE indexname = 'idx_invoice_item_match_covering';
```

## 故障排查

### 问题: 查询仍然很慢

**可能原因:**
1. 统计信息过期

   **解决方案:**
   ```sql
   ANALYZE t_sim_vatinvoice_item_1201;
   ```

2. 索引膨胀

   **检查:**
   ```sql
   SELECT pg_size_pretty(pg_relation_size('idx_invoice_item_match_covering'::regclass));
   ```

   **解决方案:**
   ```sql
   REINDEX INDEX CONCURRENTLY idx_invoice_item_match_covering;
   ```

3. 查询优化器未选择正确索引

   **检查:**
   ```sql
   EXPLAIN ANALYZE <your_query>;
   ```

   **解决方案:**
   - 检查 `random_page_cost` 和 `seq_page_cost` 配置
   - 考虑使用 query hints（如必要）

## 后续优化建议

### 1. 考虑分区
如果数据量持续增长（>1亿行），考虑按以下维度分区:
- 按购方税号范围分区
- 按时间分区（如按月）

### 2. 考虑缓存
对于频繁查询的产品代码组合，考虑:
- 应用层缓存（Redis）
- 预计算结果表

### 3. 考虑异步处理
对于非实时场景，考虑:
- 异步队列处理
- 批量查询优化

## 文件清单

- `migrations/001_optimize_invoice_queries.sql` - 数据库迁移文件
- `scripts/check_query_performance.sh` - 性能检查脚本
- `PERFORMANCE_OPTIMIZATION.md` - 本文档

## 联系信息

如有问题或建议，请联系技术团队。
