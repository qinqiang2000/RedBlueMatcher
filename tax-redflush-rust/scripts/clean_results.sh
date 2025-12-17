#!/bin/bash

# 清理匹配结果脚本
# 用于删除指定 bill_ids 的匹配结果，避免重复运行产生重复数据

set -e

# 从环境变量或参数获取数据库连接信息
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../../.env.local"

# 加载环境变量
if [ -f "$ENV_FILE" ]; then
    while IFS='=' read -r key value; do
        if [[ -z "$key" ]] || [[ "$key" =~ ^#.* ]]; then
            continue
        fi
        value=$(echo "$value" | sed -e 's/^"//' -e 's/"$//' -e "s/^'//" -e "s/'$//")
        export "$key=$value"
    done < "$ENV_FILE"
fi

DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-gaoji}"
DB_USER="${DB_USER:-qinqiang02}"

echo "======================================"
echo "清理匹配结果"
echo "======================================"
echo "数据库: $DB_NAME@$DB_HOST:$DB_PORT"
echo ""

# 参数处理
if [ $# -eq 0 ]; then
    echo "用法:"
    echo "  1. 删除指定单据的结果:"
    echo "     $0 bill [bill_id1] [bill_id2] ..."
    echo ""
    echo "  2. 删除指定税号组合的结果:"
    echo "     $0 tax [销方税号] [购方税号]"
    echo ""
    echo "  3. 删除所有结果 (危险):"
    echo "     $0 all"
    echo ""
    exit 1
fi

MODE=$1
shift

case $MODE in
    "bill")
        if [ $# -eq 0 ]; then
            echo "错误: 请提供至少一个 bill_id"
            exit 1
        fi

        BILL_IDS=$(echo "$@" | tr ' ' ',')
        echo "删除单据结果: $BILL_IDS"
        echo ""
        read -p "确认删除? (yes/no): " confirm

        if [ "$confirm" != "yes" ]; then
            echo "已取消"
            exit 0
        fi

        SQL="DELETE FROM t_sim_match_result_1201 WHERE fbillid IN ($BILL_IDS);"
        ;;

    "tax")
        if [ $# -ne 2 ]; then
            echo "错误: 请提供销方税号和购方税号"
            exit 1
        fi

        SELLER=$1
        BUYER=$2

        echo "删除税号组合的结果:"
        echo "  销方税号: $SELLER"
        echo "  购方税号: $BUYER"
        echo ""

        # 先查询影响的记录数
        COUNT=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c \
            "SELECT COUNT(*) FROM t_sim_match_result_1201 WHERE fsalertaxno='$SELLER' AND fbuyertaxno='$BUYER';")

        echo "将删除 $COUNT 条记录"
        read -p "确认删除? (yes/no): " confirm

        if [ "$confirm" != "yes" ]; then
            echo "已取消"
            exit 0
        fi

        SQL="DELETE FROM t_sim_match_result_1201 WHERE fsalertaxno='$SELLER' AND fbuyertaxno='$BUYER';"
        ;;

    "all")
        echo "⚠️  警告: 将删除所有匹配结果！"

        COUNT=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c \
            "SELECT COUNT(*) FROM t_sim_match_result_1201;")

        echo "当前记录数: $COUNT"
        read -p "确认删除所有记录? (输入 DELETE_ALL 确认): " confirm

        if [ "$confirm" != "DELETE_ALL" ]; then
            echo "已取消"
            exit 0
        fi

        SQL="TRUNCATE TABLE t_sim_match_result_1201;"
        ;;

    *)
        echo "错误: 未知模式 '$MODE'"
        exit 1
        ;;
esac

# 执行删除
echo ""
echo "执行删除..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "$SQL"

if [ $? -eq 0 ]; then
    echo "✓ 删除成功"
else
    echo "✗ 删除失败"
    exit 1
fi

echo ""
echo "======================================"
echo "清理完成"
echo "======================================"
