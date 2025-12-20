#!/bin/bash
# 将匹配结果 CSV 导入 PostgreSQL
#
# 用法:
#   # 默认方式（向后兼容）
#   ./import_csv_to_db.sh [csv文件] [.env文件]
#
#   # 新的参数方式
#   ./import_csv_to_db.sh --csv match_results.csv --env local
#   ./import_csv_to_db.sh --csv match_results.csv --env-file /path/to/.env.custom
#
# 例如:
#   ./import_csv_to_db.sh match_results.csv .env.local       # 旧方式（仍然支持）
#   ./import_csv_to_db.sh --csv match_results.csv --env prod # 新方式

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ========== 参数解析 ==========

show_help() {
    cat << EOF
CSV Import Tool - 将匹配结果 CSV 导入 PostgreSQL

用法:
  $0 [OPTIONS] [CSV_FILE] [ENV_FILE]

选项:
  --csv FILE          指定要导入的 CSV 文件
  --env ENV_NAME      指定环境名称，加载 .env.{ENV_NAME} 文件（如: local, prod）
  --env-file FILE     直接指定环境配置文件的完整路径
  -h, --help          显示此帮助信息

位置参数（向后兼容）:
  CSV_FILE            要导入的 CSV 文件（默认: match_results.csv）
  ENV_FILE            环境配置文件路径（默认: .env.local）

示例:
  # 默认方式（向后兼容）
  $0 match_results.csv .env.local
  $0 match_results.csv

  # 使用新的参数方式
  $0 --csv match_results.csv --env local
  $0 --csv ../tax-redflush-rust/match_results_123.csv --env prod
  $0 --csv match_results.csv --env-file /path/to/.env.custom

  # 混合使用（CSV 通过位置参数，环境通过选项）
  $0 match_results.csv --env prod

EOF
    exit 0
}

# 默认值
CSV_FILE=""
ENV_FILE=""
ENV_NAME=""
POSITIONAL_ARGS=()

# 解析参数
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            ;;
        --csv)
            CSV_FILE="$2"
            shift 2
            ;;
        --env)
            ENV_NAME="$2"
            shift 2
            ;;
        --env-file)
            ENV_FILE="$2"
            shift 2
            ;;
        -*)
            echo "Error: Unknown option: $1"
            echo "Use --help to see available options"
            exit 1
            ;;
        *)
            POSITIONAL_ARGS+=("$1")
            shift
            ;;
    esac
done

# 处理位置参数（向后兼容）
if [ ${#POSITIONAL_ARGS[@]} -gt 0 ] && [ -z "$CSV_FILE" ]; then
    CSV_FILE="${POSITIONAL_ARGS[0]}"
fi

if [ ${#POSITIONAL_ARGS[@]} -gt 1 ] && [ -z "$ENV_FILE" ] && [ -z "$ENV_NAME" ]; then
    ENV_FILE="${POSITIONAL_ARGS[1]}"
fi

# 设置默认值
if [ -z "$CSV_FILE" ]; then
    CSV_FILE="match_results.csv"
fi

# 确定环境文件
if [ -z "$ENV_FILE" ]; then
    if [ -n "$ENV_NAME" ]; then
        # 使用 --env 参数指定的环境名称
        ENV_FILE="$PROJECT_ROOT/.env.$ENV_NAME"
    else
        # 默认使用 .env.local
        ENV_FILE="$PROJECT_ROOT/.env.local"
    fi
fi

# ========== 路径处理 ==========

# 转换为绝对路径（如果不是绝对路径）
if [ "${CSV_FILE:0:1}" != "/" ]; then
    # 如果是相对路径，基于当前工作目录转换
    if [ -f "$CSV_FILE" ]; then
        CSV_FILE="$(cd "$(dirname "$CSV_FILE")" && pwd)/$(basename "$CSV_FILE")"
    elif [ -f "$PROJECT_ROOT/$CSV_FILE" ]; then
        CSV_FILE="$PROJECT_ROOT/$CSV_FILE"
    fi
fi

if [ "${ENV_FILE:0:1}" != "/" ]; then
    # 相对于项目根目录
    if [ -f "$ENV_FILE" ]; then
        ENV_FILE="$(cd "$(dirname "$ENV_FILE")" && pwd)/$(basename "$ENV_FILE")"
    elif [ -f "$PROJECT_ROOT/$ENV_FILE" ]; then
        ENV_FILE="$PROJECT_ROOT/$ENV_FILE"
    fi
fi

# ========== 验证和加载配置 ==========

# 检查环境文件
if [ ! -f "$ENV_FILE" ]; then
    echo "Error: ENV file not found: $ENV_FILE"
    echo ""
    echo "Available .env files in project root:"
    ls -1 "$PROJECT_ROOT"/.env* 2>/dev/null || echo "  (none found)"
    echo ""
    echo "Use --env <name> to specify environment or --env-file <path> for custom file"
    exit 1
fi

# 加载环境变量
echo "Loading config from: $ENV_FILE"
set -a
source "$ENV_FILE"
set +a

# 检查 CSV 文件
if [ ! -f "$CSV_FILE" ]; then
    echo "Error: CSV file not found: $CSV_FILE"
    exit 1
fi

# ========== 执行导入 ==========

TABLE_NAME="t_sim_match_result_1201"
RECORD_COUNT=$(wc -l < "$CSV_FILE" | tr -d ' ')

echo "========================================"
echo "CSV Import Tool"
echo "========================================"
echo "CSV File:    $CSV_FILE"
echo "Records:     $RECORD_COUNT"
echo "Database:    $DB_NAME"
echo "Table:       $TABLE_NAME"
echo "========================================"

# 导入前记录数
BEFORE_COUNT=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM $TABLE_NAME;" 2>/dev/null | tr -d ' ' || echo "0")
echo "Before import: $BEFORE_COUNT records"

# 执行导入
echo "Importing..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "\copy $TABLE_NAME (fbillid, fbuyertaxno, fsalertaxno, fspbm, finvoiceid, finvoiceitemid, fnum, fbillamount, finvoiceamount, fmatchamount, fbillunitprice, fbillqty, finvoiceunitprice, finvoiceqty, fmatchtime) FROM '$CSV_FILE' WITH (FORMAT csv, NULL '')"

# 导入后记录数
AFTER_COUNT=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM $TABLE_NAME;" | tr -d ' ')
IMPORTED=$((AFTER_COUNT - BEFORE_COUNT))

echo "========================================"
echo "Import completed!"
echo "After import:  $AFTER_COUNT records"
echo "Imported:      $IMPORTED records"
echo "========================================"
