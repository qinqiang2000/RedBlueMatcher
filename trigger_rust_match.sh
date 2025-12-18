#!/bin/bash

# Rust 发票匹配触发脚本
# 用法: ./trigger_rust_match.sh [v1|v2] [销方税号] [购方税号]
# 或:   ./trigger_rust_match.sh [销方税号] [购方税号] [v1|v2]

# 脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env.local"

# 颜色输出
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo_green() { echo -e "${GREEN}$1${NC}"; }
echo_blue() { echo -e "${BLUE}$1${NC}"; }
echo_yellow() { echo -e "${YELLOW}$1${NC}"; }
echo_red() { echo -e "${RED}$1${NC}"; }

# 1. 加载环境配置
echo_blue "=== 加载环境配置 ==="

if [ ! -f "$ENV_FILE" ]; then
    echo_red "错误: 找不到 $ENV_FILE 文件"
    exit 1
fi

# 安全地加载环境变量
while IFS='=' read -r key value; do
    # 跳过空行和注释
    if [[ -z "$key" ]] || [[ "$key" =~ ^#.* ]]; then
        continue
    fi
    # 去除值两边的引号和空格
    value=$(echo "$value" | sed -e 's/^"//' -e 's/"$//' -e "s/^'//" -e "s/'$//")
    export "$key=$value"
done < "$ENV_FILE"

# 设置默认值
SERVER_HOST=${SERVER_HOST:-127.0.0.1}
SERVER_PORT=${SERVER_PORT:-8089}

echo_green "✓ 数据库: $DB_HOST:$DB_PORT/$DB_NAME"
echo_green "✓ 服务地址: $SERVER_HOST:$SERVER_PORT"
echo ""

# 解析命令行参数
VERSION="v2"  # 默认版本
SELLER="91341103MA2TWC9B1Q"  # 默认销方税号
BUYER="9134110275298062X0"   # 默认购方税号

# 检查参数数量和类型
if [ $# -eq 0 ]; then
    # 无参数，使用所有默认值
    echo_blue "使用默认配置: v2 算法, 默认税号"
elif [ $# -eq 1 ]; then
    # 一个参数，可能是版本号
    if [[ "$1" == "v1" ]] || [[ "$1" == "v2" ]]; then
        VERSION="$1"
        echo_blue "使用版本: $VERSION, 默认税号"
    else
        echo_red "错误: 单个参数必须是 'v1' 或 'v2'"
        echo_red "用法: $0 [v1|v2] [销方税号] [购方税号]"
        exit 1
    fi
elif [ $# -eq 2 ]; then
    # 两个参数，视为税号
    SELLER="$1"
    BUYER="$2"
    echo_blue "使用版本: v2, 自定义税号"
elif [ $# -eq 3 ]; then
    # 三个参数
    # 检查第一个参数是否是版本
    if [[ "$1" == "v1" ]] || [[ "$1" == "v2" ]]; then
        VERSION="$1"
        SELLER="$2"
        BUYER="$3"
    # 检查第三个参数是否是版本
    elif [[ "$3" == "v1" ]] || [[ "$3" == "v2" ]]; then
        SELLER="$1"
        BUYER="$2"
        VERSION="$3"
    else
        echo_red "错误: 版本参数必须是 'v1' 或 'v2'"
        exit 1
    fi
else
    echo_red "错误: 参数过多"
    echo_red "用法: $0 [v1|v2] [销方税号] [购方税号]"
    exit 1
fi

echo ""

# 2. 停止并重启服务
echo_blue "=== 停止并重启 Rust 服务 ==="

# 2.1 检查并停止旧服务
if curl -s "http://$SERVER_HOST:$SERVER_PORT/health" > /dev/null 2>&1; then
    echo_yellow "检测到服务正在运行，正在停止..."

    # 查找进程并停止
    PIDS=$(pgrep -f "tax-redflush-rust|target.*(release|debug).tax-redflush-rust")
    if [ -n "$PIDS" ]; then
        echo_yellow "找到进程: $PIDS"
        kill $PIDS 2>/dev/null || true
        sleep 2

        # 强制杀死如果还在运行
        PIDS=$(pgrep -f "tax-redflush-rust|target.*(release|debug).tax-redflush-rust")
        if [ -n "$PIDS" ]; then
            echo_yellow "强制停止进程..."
            kill -9 $PIDS 2>/dev/null || true
            sleep 1
        fi
        echo_green "✓ 旧服务已停止"
    fi
else
    echo_yellow "服务未运行"
fi

# 2.2 启动新服务
echo_blue "启动新服务..."
cd "$SCRIPT_DIR/tax-redflush-rust"

# 设置数据库连接
export DATABASE_URL="postgres://$DB_USER:$DB_PASSWORD@$DB_HOST:$DB_PORT/$DB_NAME"

# 生成带时间戳的日志文件名
LOG_TIMESTAMP=$(date '+%Y-%m-%d_%H-%M-%S')
LOG_FILE="logs/rust-service-${LOG_TIMESTAMP}.log"

# 后台启动服务
nohup cargo run --release > "$LOG_FILE" 2>&1 &
SERVICE_PID=$!

echo_yellow "日志文件: $LOG_FILE"
echo_yellow "服务进程 PID: $SERVICE_PID"

# 自动打开新终端窗口查看日志
echo_blue "打开新终端窗口查看实时日志..."
FULL_LOG_PATH="$SCRIPT_DIR/tax-redflush-rust/$LOG_FILE"
osascript <<EOF 2>/dev/null &
tell application "Terminal"
    do script "cd '$SCRIPT_DIR/tax-redflush-rust' && echo '=== 实时日志 ===' && echo '日志文件: $LOG_FILE' && echo '' && tail -f '$LOG_FILE'"
    activate
end tell
EOF

echo_yellow "等待服务启动..."

# 2.3 等待服务就绪（最多30秒）
for i in {1..30}; do
    sleep 1
    if curl -s "http://$SERVER_HOST:$SERVER_PORT/health" > /dev/null 2>&1; then
        echo_green "✓ 服务启动成功 (耗时 ${i}s)"
        cd "$SCRIPT_DIR"
        break
    fi

    if [ $i -eq 30 ]; then
        echo_red "✗ 服务启动超时"
        echo_yellow "请检查日志: tail -f tax-redflush-rust/$LOG_FILE"
        cd "$SCRIPT_DIR"
        exit 1
    fi
done
echo ""

# 3. 查询待匹配单据
echo_blue "=== 查询待匹配单据 ==="

# 默认税号（已在上面通过命令行参数解析设置）
# SELLER=${1:-"91341103MA2TWC9B1Q"}
# BUYER=${2:-"9134110275298062X0"}

# 生成 SQL 查询
cat > /tmp/query_billids_rust.sql <<EOF
SELECT DISTINCT fid
FROM ${TABLE_ORIGINAL_BILL}
WHERE fsalertaxno = '$SELLER'
  AND fbuyertaxno = '$BUYER'
ORDER BY fid;
EOF

# 执行查询
echo_blue "执行 SQL 查询..."
PGPASSWORD="${DB_PASSWORD}" psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" \
    -t -A -F',' -f /tmp/query_billids_rust.sql > /tmp/billids_rust.txt 2>/dev/null

if [ $? -ne 0 ]; then
    echo_red "✗ 数据库查询失败"
    exit 1
fi

# 构建 JSON 数组
BILL_IDS=$(cat /tmp/billids_rust.txt | tr '\n' ',' | sed 's/,$//' | sed 's/^/\[/' | sed 's/$/\]/')

if [ "$BILL_IDS" == "[]" ] || [ -z "$BILL_IDS" ]; then
    echo_red "✗ 未找到匹配的单据"
    exit 1
fi

echo_green "✓ 找到的 billIds: $BILL_IDS"
echo ""

# 4. 清理旧的匹配结果（避免重复）
echo_blue "=== 清理旧的匹配结果 ==="

# 检查清理脚本是否存在
CLEAN_SCRIPT="$SCRIPT_DIR/scripts/clean_results.sh"
if [ ! -f "$CLEAN_SCRIPT" ]; then
    echo_red "✗ 清理脚本不存在: $CLEAN_SCRIPT"
    exit 1
fi

# 调用清理脚本（非交互模式）
echo_yellow "调用清理脚本: $CLEAN_SCRIPT tax $SELLER $BUYER"
echo "yes" | "$CLEAN_SCRIPT" tax "$SELLER" "$BUYER" 2>&1 | grep -E "(删除|错误|✓|✗)" || true
echo ""

# 5. 触发匹配
if [[ "$VERSION" == "v1" ]]; then
    echo_blue "=== 触发批量匹配 (SKU-Centric v1) ==="
    ENDPOINT="/api/match/batch"
    ALGORITHM="SKU-Centric"
else
    echo_blue "=== 触发批量匹配 (Invoice-Centric v2) ==="
    ENDPOINT="/api/match/batch/v2"
    ALGORITHM="Invoice-Centric"
fi
echo_blue "请求 URL: http://$SERVER_HOST:$SERVER_PORT$ENDPOINT"
echo_blue "算法版本: $ALGORITHM"
echo_blue "销方税号: $SELLER"
echo_blue "购方税号: $BUYER"
echo ""

# 发送请求（带超时）
echo_yellow "匹配进行中，请等待..."
echo_yellow "实时日志已在新终端窗口中打开"
echo ""

RESPONSE=$(curl -s --max-time 300 -X POST "http://$SERVER_HOST:$SERVER_PORT$ENDPOINT" \
    -H "Content-Type: application/json" \
    -d "{\"bill_ids\": $BILL_IDS}")

CURL_EXIT=$?

# 显示结果
echo ""
if [ $CURL_EXIT -eq 0 ]; then
    echo_green "✓ 匹配完成"
    echo_green "服务响应: $RESPONSE"
else
    echo_red "✗ 请求失败（退出码: $CURL_EXIT）"
    if [ $CURL_EXIT -eq 28 ]; then
        echo_yellow "超时说明: 匹配可能仍在后台进行，请查看日志确认"
    fi
fi

echo ""
echo_green "=== 完成 ==="
echo_yellow "日志文件: tax-redflush-rust/$LOG_FILE"
echo_yellow "实时日志正在新终端窗口中显示"
echo_yellow "如需手动查看: tail -f tax-redflush-rust/$LOG_FILE"
