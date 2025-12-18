-- Migration: 性能提升补充索引
-- 目标: 进一步优化大批量SKU查询（4830+ SKU场景）

-- 1. 发票主表：添加覆盖更多列的索引，减少回表次数
-- 这个索引支持更快的 JOIN 操作
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_vatinvoice_tax_total_optimized
ON t_sim_vatinvoice_1201(fbuyertaxno, fsalertaxno, ftotalamount, fid)
WHERE ftotalamount > 0;

-- 2. 发票明细表：添加反向索引支持不同的查询模式
-- 支持按 fid 分组的查询优化
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_invoice_item_fid_amount_sku
ON t_sim_vatinvoice_item_1201(fid, famount DESC, fspbm)
WHERE famount > 0;

-- 3. 发票明细表：优化 ANY(array) 查询的BRIN索引（适合大表）
-- BRIN索引占用空间小，适合按范围扫描
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_invoice_item_fid_brin
ON t_sim_vatinvoice_item_1201 USING BRIN(fid)
WITH (pages_per_range = 128);

-- 4. 统计信息更新（确保查询计划准确）
ANALYZE t_sim_vatinvoice_1201;
ANALYZE t_sim_vatinvoice_item_1201;

-- 5. 设置更激进的统计目标（提高查询计划准确性）
ALTER TABLE t_sim_vatinvoice_1201 ALTER COLUMN fbuyertaxno SET STATISTICS 1000;
ALTER TABLE t_sim_vatinvoice_1201 ALTER COLUMN fsalertaxno SET STATISTICS 1000;
ALTER TABLE t_sim_vatinvoice_item_1201 ALTER COLUMN fspbm SET STATISTICS 1000;
ALTER TABLE t_sim_vatinvoice_item_1201 ALTER COLUMN fid SET STATISTICS 1000;

-- 重新收集统计信息
ANALYZE t_sim_vatinvoice_1201;
ANALYZE t_sim_vatinvoice_item_1201;
