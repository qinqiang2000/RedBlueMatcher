#!/bin/bash

# 触发Java版本的匹配，并实时对比日志
# 用法: ./trigger_java_match.sh

SELLER="91341103MA2TWC9B1Q"
BUYER="9134110275298062X0"

echo "=== 查询匹配的billIds ==="
# 使用Python代码中的查询逻辑
cat > /tmp/query_billids.sql <<EOF
SELECT DISTINCT fid
FROM t_sim_match_bill_1201
WHERE fsalertaxno = '$SELLER'
  AND fbuyertaxno = '$BUYER'
ORDER BY fid;
EOF

echo "执行SQL查询..."
PGPASSWORD="" psql -h localhost -U qinqiang02 -d gaoji -t -A -F',' -f /tmp/query_billids.sql > /tmp/billids.txt

# 读取billIds并构建JSON
BILL_IDS=$(cat /tmp/billids.txt | tr '\n' ',' | sed 's/,$//' | sed 's/^/[/' | sed 's/$/]/')

echo "找到的billIds: $BILL_IDS"
echo ""

# 清理旧的匹配结果（避免重复）
echo "=== 清理旧的匹配结果 ==="

# 脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLEAN_SCRIPT="$SCRIPT_DIR/scripts/clean_results.sh"

# 检查清理脚本是否存在
if [ ! -f "$CLEAN_SCRIPT" ]; then
    echo "✗ 清理脚本不存在: $CLEAN_SCRIPT"
    exit 1
fi

# 调用清理脚本（非交互模式）
echo "调用清理脚本: $CLEAN_SCRIPT tax $SELLER $BUYER"
echo "yes" | "$CLEAN_SCRIPT" tax "$SELLER" "$BUYER" 2>&1 | grep -E "(删除|错误|✓|✗)" || true
echo ""

# 发送HTTP请求触发匹配
echo "=== 触发Java匹配 (端口8085) ==="
curl -X POST http://localhost:8085/redflush/match/temp \
  -H "Content-Type: application/json" \
  -d "{\"billIds\": $BILL_IDS}" \
  &

CURL_PID=$!

echo ""
echo "=== 实时监控日志 ==="
echo "Java日志: tax-redflush-service-java/logs/tax-redflush-java.log"
echo ""

# 等待Java日志文件创建
sleep 2

# 实时显示Java日志（只显示进度行）
tail -f tax-redflush-service-java/logs/tax-redflush-java.log | grep --line-buffered "SKU进度\|跳过稀缺度\|处理销购方\|匹配完成" &
TAIL_PID=$!

# 等待curl完成
wait $CURL_PID

echo ""
echo "=== Java匹配完成 ==="

# 停止tail
kill $TAIL_PID 2>/dev/null

echo ""
echo "完整日志查看:"
echo "  tail -f tax-redflush-service-java/logs/tax-redflush-java.log"
