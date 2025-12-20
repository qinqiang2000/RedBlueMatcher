#!/bin/bash

# 清理匹配结果脚本
# 用于删除指定 bill_ids 的匹配结果，避免重复运行产生重复数据

set -e

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

# 脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ========== 帮助信息 ==========
show_help() {
    cat << EOF
清理匹配结果脚本 - 删除匹配结果数据

用法:
  $0 [OPTIONS] MODE [ARGS...]

选项:
  --env ENV_NAME      指定环境名称，加载 .env.{ENV_NAME} 文件（如: dev, prod）
  --env-file FILE     直接指定环境配置文件的完整路径
  -h, --help          显示此帮助信息

模式:
  bill BILL_IDS...    删除指定单据的结果
  tax SELLER BUYER    删除指定税号组合的结果
  all                 删除所有结果（危险！）

环境变量:
  ENV_FILE            环境配置文件路径（优先级低于命令行参数）

示例:
  # 删除指定单据
  $0 bill 123 456 789
  $0 --env dev bill 123

  # 删除指定税号组合
  $0 tax 91110000600037297L 91110108MA001KRU45
  $0 --env prod tax 91110000600037297L 91110108MA001KRU45

  # 删除所有结果
  $0 all
  $0 --env dev all

  # 使用环境变量
  ENV_FILE=.env.dev $0 all

EOF
    exit 0
}

# ========== 参数解析 ==========

# 默认值
CMD_ENV_FILE=""
ENV_NAME=""
MODE=""
MODE_ARGS=()

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
        bill|tax|all)
            MODE="$1"
            shift
            # 剩余所有参数作为 MODE_ARGS
            MODE_ARGS=("$@")
            break
            ;;
        *)
            echo_red "错误: 未知参数 '$1'"
            echo_red "使用 --help 查看帮助"
            exit 1
            ;;
    esac
done

# 检查是否指定了模式
if [ -z "$MODE" ]; then
    echo_red "错误: 未指定操作模式"
    echo ""
    show_help
fi

# ========== 确定环境配置文件 ==========

# 优先级: --env-file > --env > ENV_FILE 环境变量 > 默认值
if [ -n "$CMD_ENV_FILE" ]; then
    # 使用 --env-file 指定的文件
    FINAL_ENV_FILE="$CMD_ENV_FILE"
elif [ -n "$ENV_NAME" ]; then
    # 使用 --env 指定的环境名称
    FINAL_ENV_FILE="$SCRIPT_DIR/../.env.$ENV_NAME"
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
    FINAL_ENV_FILE="$SCRIPT_DIR/../.env.local"
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
    if [[ -z "$key" ]] || [[ "$key" =~ ^#.* ]]; then
        continue
    fi
    value=$(echo "$value" | sed -e 's/^"//' -e 's/"$//' -e "s/^'//" -e "s/'$//")
    export "$key=$value"
done < "$FINAL_ENV_FILE"

# 设置默认值
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-gaoji}"
DB_USER="${DB_USER:-qinqiang02}"
TABLE_MATCH_RESULT="${TABLE_MATCH_RESULT:-t_sim_match_result_1201}"

echo_blue "======================================"
echo_blue "清理匹配结果"
echo_blue "======================================"
echo_green "数据库: $DB_NAME@$DB_HOST:$DB_PORT"
echo_green "结果表: $TABLE_MATCH_RESULT"
echo ""

# ========== 处理清理模式 ==========

case $MODE in
    "bill")
        if [ ${#MODE_ARGS[@]} -eq 0 ]; then
            echo_red "错误: 请提供至少一个 bill_id"
            exit 1
        fi

        BILL_IDS=$(IFS=,; echo "${MODE_ARGS[*]}")
        echo_yellow "删除单据结果: $BILL_IDS"
        echo ""
        read -p "确认删除? (yes/no): " confirm

        if [ "$confirm" != "yes" ]; then
            echo_yellow "已取消"
            exit 0
        fi

        SQL="DELETE FROM $TABLE_MATCH_RESULT WHERE fbillid IN ($BILL_IDS);"
        ;;

    "tax")
        if [ ${#MODE_ARGS[@]} -ne 2 ]; then
            echo_red "错误: 请提供销方税号和购方税号"
            exit 1
        fi

        SELLER="${MODE_ARGS[0]}"
        BUYER="${MODE_ARGS[1]}"

        echo_yellow "删除税号组合的结果:"
        echo_yellow "  销方税号: $SELLER"
        echo_yellow "  购方税号: $BUYER"
        echo ""

        # 先查询影响的记录数
        COUNT=$(PGPASSWORD="${DB_PASSWORD}" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c \
            "SELECT COUNT(*) FROM $TABLE_MATCH_RESULT WHERE fsalertaxno='$SELLER' AND fbuyertaxno='$BUYER';")

        echo_yellow "将删除 $COUNT 条记录"
        read -p "确认删除? (yes/no): " confirm

        if [ "$confirm" != "yes" ]; then
            echo_yellow "已取消"
            exit 0
        fi

        SQL="DELETE FROM $TABLE_MATCH_RESULT WHERE fsalertaxno='$SELLER' AND fbuyertaxno='$BUYER';"
        ;;

    "all")
        echo_red "⚠️  警告: 将删除所有匹配结果！"

        COUNT=$(PGPASSWORD="${DB_PASSWORD}" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c \
            "SELECT COUNT(*) FROM $TABLE_MATCH_RESULT;")

        echo_yellow "当前记录数: $COUNT"
        read -p "确认删除所有记录? (输入 DELETE_ALL 确认): " confirm

        if [ "$confirm" != "DELETE_ALL" ]; then
            echo_yellow "已取消"
            exit 0
        fi

        SQL="TRUNCATE TABLE $TABLE_MATCH_RESULT;"
        ;;

    *)
        echo_red "错误: 未知模式 '$MODE'"
        exit 1
        ;;
esac

# 执行删除
echo ""
echo_blue "执行删除..."
PGPASSWORD="${DB_PASSWORD}" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "$SQL"

if [ $? -eq 0 ]; then
    echo_green "✓ 删除成功"
else
    echo_red "✗ 删除失败"
    exit 1
fi

echo ""
echo_green "======================================"
echo_green "清理完成"
echo_green "======================================"
