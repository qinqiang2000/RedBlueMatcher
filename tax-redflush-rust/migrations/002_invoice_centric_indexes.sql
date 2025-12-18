-- Migration: Invoice-Centric算法优化索引
-- 用于支持批量多SKU查询

-- 新增覆盖索引：支持多SKU批量查询
-- 包含 fspbm, fid, famount 作为索引键，其他字段作为 INCLUDE 列
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_invoice_item_multi_sku_covering
ON t_sim_vatinvoice_item_1201(fspbm, fid, famount DESC)
INCLUDE (fentryid, fnum, funitprice);

-- 为发票主表创建索引：支持购销方税号过滤
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_vatinvoice_tax_match
ON t_sim_vatinvoice_1201(fbuyertaxno, fsalertaxno, fid)
WHERE ftotalamount > 0;

-- 复合索引：支持发票明细的批量IN查询
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_invoice_item_fid_fspbm
ON t_sim_vatinvoice_item_1201(fid, fspbm)
WHERE famount > 0;
