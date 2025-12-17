#!/bin/bash

# 查询性能检查脚本
# 用于验证发票匹配查询的性能

set -e

# 从环境变量或参数获取数据库连接信息
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-gaoji}"
DB_USER="${DB_USER:-qinqiang02}"

echo "======================================"
echo "发票匹配查询性能检查"
echo "======================================"
echo "数据库: $DB_NAME@$DB_HOST:$DB_PORT"
echo "时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

# 查找高频查询参数
echo "1. 查找高频查询参数（返回 >5000 行的组合）"
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "
SELECT
    vii.fspbm,
    vi.fbuyertaxno,
    vi.fsalertaxno,
    COUNT(*) as row_count
FROM t_sim_vatinvoice_item_1201 vii
INNER JOIN t_sim_vatinvoice_1201 vi ON vi.fid = vii.fid
WHERE vi.ftotalamount > 0
GROUP BY vii.fspbm, vi.fbuyertaxno, vi.fsalertaxno
HAVING COUNT(*) > 5000
ORDER BY COUNT(*) DESC
LIMIT 3;
"

echo ""
echo "2. 检查索引状态"
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "
SELECT
    schemaname,
    tablename,
    indexname,
    pg_size_pretty(pg_relation_size(indexname::regclass)) as size
FROM pg_indexes
WHERE tablename = 't_sim_vatinvoice_item_1201'
    AND indexname LIKE '%match_covering%'
ORDER BY indexname;
"

echo ""
echo "3. 测试查询性能（使用高频参数）"
echo "   测试参数: fspbm='1016393', buyer='9134110275298062X0', seller='91341103MA2TWC9B1Q'"
echo ""

# 执行测试查询并提取执行时间
RESULT=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "
EXPLAIN ANALYZE
SELECT
  vii.fid as invoice_id,
  vii.fentryid as item_id,
  vii.fspbm as product_code,
  vii.fnum as quantity,
  vii.famount as amount,
  vii.funitprice as unit_price
FROM
  t_sim_vatinvoice_item_1201 vii
  INNER JOIN t_sim_vatinvoice_1201 vi ON vi.fid = vii.fid
WHERE
  vii.fspbm = '1016393'
  AND vi.fbuyertaxno = '9134110275298062X0'
  AND vi.fsalertaxno = '91341103MA2TWC9B1Q'
  AND vi.ftotalamount > 0
ORDER BY
  vii.famount DESC;
" | grep "Execution Time")

echo "$RESULT"

# 提取执行时间（毫秒）- 兼容 macOS 和 Linux
EXEC_TIME=$(echo "$RESULT" | sed -n 's/.*Execution Time: \([0-9.]*\) ms/\1/p')

echo ""
if (( $(echo "$EXEC_TIME < 50" | bc -l) )); then
    echo "✓ 性能良好: ${EXEC_TIME}ms < 50ms"
elif (( $(echo "$EXEC_TIME < 100" | bc -l) )); then
    echo "⚠ 性能一般: ${EXEC_TIME}ms (建议 < 50ms)"
else
    echo "✗ 性能不佳: ${EXEC_TIME}ms (建议 < 50ms)"
    echo "  请检查索引是否正常，运行 ANALYZE 更新统计信息"
fi

echo ""
echo "4. 检查最近的慢查询日志"
if [ -f "../logs/rust-service-optimized.log" ]; then
    SLOW_COUNT=$(tail -1000 ../logs/rust-service-optimized.log | grep -c "slow statement" || echo "0")
    echo "   最近1000行日志中慢查询数量: $SLOW_COUNT"

    if [ "$SLOW_COUNT" -gt 0 ]; then
        echo ""
        echo "   最新的慢查询:"
        tail -1000 ../logs/rust-service-optimized.log | grep "slow statement" | tail -3
    fi
else
    echo "   日志文件不存在: ../logs/rust-service-optimized.log"
fi

echo ""
echo "======================================"
echo "检查完成"
echo "======================================"
