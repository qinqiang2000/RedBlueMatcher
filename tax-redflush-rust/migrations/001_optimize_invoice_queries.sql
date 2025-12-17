-- 优化发票匹配查询性能
-- 创建日期: 2025-12-17
-- 优化目标: 将 match_by_tax_and_product 查询从 ~3000ms 降低到 <50ms

-- 问题分析:
-- 1. 查询需要扫描大量数据块（25,148个）获取完整行数据
-- 2. 使用单列索引 t_sim_vatinvoice_item_1201_fspbm_idx，然后回表获取其他字段
-- 3. 对于返回 30,000+ 行的查询，回表开销巨大

-- 解决方案:
-- 创建覆盖索引，包含查询所需的所有字段，避免回表操作

-- 覆盖索引包含:
-- - fspbm: WHERE 条件过滤
-- - fid: JOIN 条件关联
-- - famount DESC: ORDER BY 排序
-- - fentryid, fnum, funitprice: SELECT 返回字段

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_invoice_item_match_covering
ON t_sim_vatinvoice_item_1201(
    fspbm,          -- 产品代码过滤
    fid,            -- 发票ID关联
    famount DESC,   -- 金额降序排序
    fentryid,       -- 明细ID
    fnum,           -- 数量
    funitprice      -- 单价
);

-- 更新表统计信息以便优化器选择最佳执行计划
ANALYZE t_sim_vatinvoice_item_1201;

-- 性能验证:
-- 优化前: ~2957ms (Hash Join + Bitmap Heap Scan)
-- 优化后: ~32ms (Merge Join + Index Only Scan)
-- 提升: 92倍

-- 测试查询:
-- EXPLAIN ANALYZE
-- SELECT
--   vii.fid as invoice_id,
--   vii.fentryid as item_id,
--   vii.fspbm as product_code,
--   vii.fnum as quantity,
--   vii.famount as amount,
--   vii.funitprice as unit_price
-- FROM
--   t_sim_vatinvoice_item_1201 vii
--   INNER JOIN t_sim_vatinvoice_1201 vi ON vi.fid = vii.fid
-- WHERE
--   vii.fspbm = '1016393'
--   AND vi.fbuyertaxno = '9134110275298062X0'
--   AND vi.fsalertaxno = '91341103MA2TWC9B1Q'
--   AND vi.ftotalamount > 0
-- ORDER BY
--   vii.famount DESC;

-- 索引大小: 约 1170 MB
-- 适用场景:
-- 1. 按产品代码、购方税号、销方税号查询发票明细
-- 2. 按金额降序排序（大金额优先填充策略）
-- 3. 返回大量数据的查询（>1000行）

-- 注意事项:
-- 1. 使用 CONCURRENTLY 创建索引，不会锁表
-- 2. 索引较大，需确保有足够磁盘空间
-- 3. 索引会增加写入开销，但查询性能大幅提升
