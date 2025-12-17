-- 清理结果表中的重复数据
-- 保留每组重复记录中 fid 最小的那条（即第一次插入的记录）

BEGIN;

-- 1. 查看当前重复情况
SELECT '当前重复记录统计:' as info;
SELECT
    COUNT(DISTINCT (fbillid, finvoiceid, finvoiceitemid)) as unique_combinations,
    COUNT(*) as total_records,
    COUNT(*) - COUNT(DISTINCT (fbillid, finvoiceid, finvoiceitemid)) as duplicate_records
FROM t_sim_match_result_1201;

-- 2. 查看重复最严重的记录
SELECT '重复次数最多的记录 (top 10):' as info;
SELECT
    fbillid,
    finvoiceid,
    finvoiceitemid,
    COUNT(*) as duplicate_count
FROM t_sim_match_result_1201
GROUP BY fbillid, finvoiceid, finvoiceitemid
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC
LIMIT 10;

-- 3. 删除重复记录（保留 fid 最小的）
SELECT '开始删除重复记录...' as info;

DELETE FROM t_sim_match_result_1201
WHERE fid IN (
    SELECT fid
    FROM (
        SELECT
            fid,
            ROW_NUMBER() OVER (
                PARTITION BY fbillid, finvoiceid, finvoiceitemid
                ORDER BY fid ASC
            ) as rn
        FROM t_sim_match_result_1201
    ) t
    WHERE rn > 1
);

-- 4. 显示删除结果
SELECT '删除完成！' as info;
SELECT
    COUNT(*) as remaining_records
FROM t_sim_match_result_1201;

-- 5. 验证没有重复了
SELECT '验证去重结果:' as info;
SELECT
    fbillid,
    finvoiceid,
    finvoiceitemid,
    COUNT(*) as duplicate_count
FROM t_sim_match_result_1201
GROUP BY fbillid, finvoiceid, finvoiceitemid
HAVING COUNT(*) > 1;

COMMIT;

-- 6. 建议：添加唯一约束防止未来重复
SELECT '建议添加唯一约束:' as info;
SELECT 'ALTER TABLE t_sim_match_result_1201 ADD CONSTRAINT uk_match_result UNIQUE (fbillid, finvoiceid, finvoiceitemid);' as suggestion;
