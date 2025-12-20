#!/bin/bash

# Rust 发票匹配触发脚本

set -e

# 脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

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

# ========== 帮助信息 ==========
show_help() {
    cat << EOF
Rust 发票匹配触发脚本 - 启动 Rust 服务并触发批量匹配

用法:
  $0 [OPTIONS] [VERSION]

选项:
  --env ENV_NAME      指定环境名称，加载 .env.{ENV_NAME} 文件（如: dev, prod）
  --env-file FILE     直接指定环境配置文件的完整路径
  -h, --help          显示此帮助信息

位置参数:
  VERSION             算法版本: v1 (SKU-Centric) 或 v2 (Invoice-Centric，默认)

环境变量:
  ENV_FILE            环境配置文件路径（优先级低于命令行参数）

示例:
  # 使用默认环境(.env.local)和默认版本(v2)
  $0

  # 指定算法版本
  $0 v1
  $0 v2

  # 使用命令行参数指定环境
  $0 --env dev
  $0 --env prod v1
  $0 --env-file /path/to/.env.custom v2

  # 使用环境变量指定配置文件
  ENV_FILE=.env.dev $0
  ENV_FILE=.env.prod $0 v1

EOF
    exit 0
}

# ========== 参数解析 ==========

# 默认值
CMD_ENV_FILE=""
ENV_NAME=""
VERSION="v2"

# 解析命令行参数
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            ;;
        --env)
            if [ -z "$2" ]; then
                echo_red "错误: --env 需要指定环境名称"
                echo_red "使用 --help 查看帮助"
                exit 1
            fi
            ENV_NAME="$2"
            shift 2
            ;;
        --env-file)
            if [ -z "$2" ]; then
                echo_red "错误: --env-file 需要指定文件路径"
                echo_red "使用 --help 查看帮助"
                exit 1
            fi
            CMD_ENV_FILE="$2"
            shift 2
            ;;
        v1|v2)
            VERSION="$1"
            shift
            ;;
        *)
            echo_red "错误: 未知参数 '$1'"
            echo_red "使用 --help 查看帮助"
            exit 1
            ;;
    esac
done

# ========== 确定环境配置文件 ==========

# 优先级: --env-file > --env > ENV_FILE 环境变量 > 默认值
if [ -n "$CMD_ENV_FILE" ]; then
    # 使用 --env-file 指定的文件
    FINAL_ENV_FILE="$CMD_ENV_FILE"
elif [ -n "$ENV_NAME" ]; then
    # 使用 --env 指定的环境名称
    FINAL_ENV_FILE="$SCRIPT_DIR/.env.$ENV_NAME"
elif [ -n "$ENV_FILE" ]; then
    # 使用 ENV_FILE 环境变量
    # 如果是相对路径，转换为基于当前工作目录的绝对路径
    if [ "${ENV_FILE:0:1}" != "/" ]; then
        FINAL_ENV_FILE="$(pwd)/$ENV_FILE"
    else
        FINAL_ENV_FILE="$ENV_FILE"
    fi
else
    # 使用默认值
    FINAL_ENV_FILE="$SCRIPT_DIR/.env.local"
fi

# ========== 加载环境配置 ==========

echo_blue "=== 加载环境配置 ==="
echo_green "✓ 加载配置文件: $FINAL_ENV_FILE"
echo ""

if [ ! -f "$FINAL_ENV_FILE" ]; then
    echo_red "错误: 找不到 $FINAL_ENV_FILE 文件"
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
done < "$FINAL_ENV_FILE"

# 设置默认值
SERVER_HOST=${SERVER_HOST:-127.0.0.1}
SERVER_PORT=${SERVER_PORT:-8089}

echo_green "✓ 数据库: $DB_HOST:$DB_PORT/$DB_NAME"
echo_green "✓ 服务地址: $SERVER_HOST:$SERVER_PORT"

# 显示算法版本
if [[ "$VERSION" == "v1" ]]; then
    echo_green "✓ 算法版本: SKU-Centric v1"
else
    echo_green "✓ 算法版本: Invoice-Centric v2"
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

# 生成带数据库名称和时间戳的日志文件名
LOG_TIMESTAMP=$(date '+%Y-%m-%d_%H-%M-%S')
LOG_FILE="logs/rust-service-${DB_NAME}-${LOG_TIMESTAMP}.log"

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

# 生成 SQL 查询 - 直接查询所有单据 IDs
cat > /tmp/query_billids_rust.sql <<EOF
SELECT DISTINCT fid
FROM ${TABLE_ORIGINAL_BILL}
ORDER BY fid;
EOF

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
    echo_red "✗ 未找到待匹配单据"
    exit 1
fi

# 计算单据数量
BILL_COUNT=$(cat /tmp/billids_rust.txt | wc -l | tr -d ' ')
echo_green "✓ 找到 $BILL_COUNT 个待匹配单据"

# 显示税号对统计信息
echo_blue "单据税号对统计:"
PGPASSWORD="${DB_PASSWORD}" psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" \
    -t -A -F'|' -c "
SELECT
    fsalertaxno,
    fbuyertaxno,
    COUNT(*) as count
FROM ${TABLE_ORIGINAL_BILL}
GROUP BY fsalertaxno, fbuyertaxno
ORDER BY COUNT(*) DESC;
" | while IFS='|' read -r seller buyer count; do
    echo_yellow "  销方: $seller | 购方: $buyer | 单据数: $count"
done

echo ""

# 4. 清理旧的匹配结果
echo_blue "=== 清理旧的匹配结果 ==="
CLEAN_SCRIPT="$SCRIPT_DIR/scripts/clean_results.sh"
if [ -f "$CLEAN_SCRIPT" ]; then
    echo_yellow "清理所有旧的匹配结果..."
    echo "DELETE_ALL" | "$CLEAN_SCRIPT" --env-file "$FINAL_ENV_FILE" all 2>&1 | grep -E "(删除|错误|✓|✗)" || true

    # 验证是否已清空
    REMAINING_COUNT=$(PGPASSWORD="${DB_PASSWORD}" psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -t -A -c \
        "SELECT COUNT(*) FROM ${TABLE_MATCH_RESULT};" 2>/dev/null || echo "-1")

    if [ "$REMAINING_COUNT" = "0" ]; then
        echo_green "✓ 表已清空"
    elif [ "$REMAINING_COUNT" = "-1" ]; then
        echo_yellow "⊘ 无法验证清理结果"
    else
        echo_yellow "⊘ 表中仍有 $REMAINING_COUNT 条记录（可能取消了清理）"
    fi
else
    echo_yellow "⊘ 清理脚本不存在，跳过清理"
fi

# 清理旧的 CSV 结果文件 (防止导入上次运行的残留文件)
echo_yellow "清理旧的 CSV 文件..."
rm -f "$SCRIPT_DIR/tax-redflush-rust/match_results_"*.csv
echo_green "✓ 旧 CSV 文件已清理"

echo ""

# 5. 触发匹配
if [[ "$VERSION" == "v1" ]]; then
    echo_blue "=== 触发批量匹配 (SKU-Centric v1) ==="
    ENDPOINT="/api/match/batch"
    ALGORITHM="SKU-Centric v1"
else
    echo_blue "=== 触发批量匹配 (Invoice-Centric v2) ==="
    ENDPOINT="/api/match/batch/v2"
    ALGORITHM="Invoice-Centric v2"
fi

echo_blue "请求信息:"
echo_yellow "  URL: http://$SERVER_HOST:$SERVER_PORT$ENDPOINT"
echo_yellow "  算法: $ALGORITHM"
echo_yellow "  单据数: $BILL_COUNT"
echo ""

# 发送请求（带超时 - 设置为30分钟以应对大规模匹配）
echo_yellow "匹配进行中，请等待..."
echo_yellow "实时日志已在新终端窗口中打开"
echo ""

# 记录开始时间（用于后续查找CSV文件）
START_TIMESTAMP=$(date +%s)

RESPONSE=$(curl -s --max-time 1800 -X POST "http://$SERVER_HOST:$SERVER_PORT$ENDPOINT" \
    -H "Content-Type: application/json" \
    -d "{\"bill_ids\": $BILL_IDS}")

CURL_EXIT=$?

# 显示结果
echo ""
echo_green "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if [ $CURL_EXIT -eq 0 ]; then
    echo_green "✓ 匹配完成"
    echo ""
    echo_green "服务响应:"
    echo "$RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE"
else
    echo_red "✗ 匹配失败（退出码: $CURL_EXIT）"
    if [ $CURL_EXIT -eq 28 ]; then
        echo_yellow "超时说明: 匹配可能仍在后台进行，请查看日志确认"
    fi
fi
echo_green "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo ""
echo_green "=== 完成 ==="
echo_yellow "日志文件: tax-redflush-rust/$LOG_FILE"
echo_yellow "实时日志正在新终端窗口中显示"
echo_yellow "如需手动查看: tail -f tax-redflush-rust/$LOG_FILE"

# ========== 自动导入 CSV ==========
echo ""
echo_blue "=== 自动导入 CSV 到数据库 ==="

# 从 Rust 服务响应中解析 CSV 文件名 (比解析日志更稳健)
# 响应结构: { "stats": [ { "output_file": "match_results_xxx.csv", ... } ] }
if [ -n "$RESPONSE" ]; then
    # 使用 Python 解析 JSON 获取 output_file 字段
    CSV_FILES_BASENAME=$(echo "$RESPONSE" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    stats = data.get('stats', [])
    for s in stats:
        if s.get('output_file'):
            print(s['output_file'])
except Exception as e:
    pass
")
    
    if [ -n "$CSV_FILES_BASENAME" ]; then
        CSV_FILES_FULL=""
        for file in $CSV_FILES_BASENAME; do
            CSV_FILES_FULL="$CSV_FILES_FULL $SCRIPT_DIR/tax-redflush-rust/$file"
        done
        CSV_FILES="$CSV_FILES_FULL"
        echo_green "✓ 从 API 响应中获取 CSV 文件名: $(echo $CSV_FILES_BASENAME | tr '\n' ' ')"
    else
         echo_yellow "⊘ API 响应中未包含 output_file，尝试回退到日志解析..."
         CSV_FILES=$(grep "导出到 CSV 文件:" "$FULL_LOG_PATH" | sed -n 's/.*导出到 CSV 文件: \(match_results_[0-9]*\.csv\).*/\1/p' | sort | uniq)
         if [ -n "$CSV_FILES" ]; then
            CSV_FILES_FULL=""
            for file in $CSV_FILES; do
                CSV_FILES_FULL="$CSV_FILES_FULL $SCRIPT_DIR/tax-redflush-rust/$file"
            done
            CSV_FILES="$CSV_FILES_FULL"
         fi
    fi
else
    echo_red "✗ 无 API 响应，无法获取 CSV 文件名"
    exit 1
fi

if [ -z "$CSV_FILES" ]; then
    echo_yellow "⊘ 未找到本次生成的 CSV 文件，跳过导入"
    echo_yellow "提示：可以手动导入 CSV 文件："
    echo_yellow "  ./scripts/import_csv_to_db.sh --csv tax-redflush-rust/match_results_*.csv --env-file $ENV_FILE"
else
    CSV_COUNT=$(echo "$CSV_FILES" | wc -l | tr -d ' ')
    echo_green "✓ 找到 $CSV_COUNT 个 CSV 文件"

    for CSV_FILE in $CSV_FILES; do
        CSV_BASENAME=$(basename "$CSV_FILE")
        echo_blue "导入: $CSV_BASENAME"

        # 打印导入命令
        echo_yellow "命令: ./scripts/import_csv_to_db.sh --csv \"$CSV_FILE\" --env-file \"$FINAL_ENV_FILE\""

        # 调用导入脚本
        if [ -f "$SCRIPT_DIR/scripts/import_csv_to_db.sh" ]; then
            "$SCRIPT_DIR/scripts/import_csv_to_db.sh" --csv "$CSV_FILE" --env-file "$FINAL_ENV_FILE" 2>&1 | \
                grep -E "(导入|Import|records|错误|Error|✓|✗|Before|After|Imported|completed)" || true
        else
            echo_red "✗ 导入脚本不存在: scripts/import_csv_to_db.sh"
        fi
        echo ""
    done

    echo_green "✓ CSV 导入完成"
fi
