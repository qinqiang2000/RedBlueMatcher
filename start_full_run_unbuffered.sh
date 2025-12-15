#!/bin/bash

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 检查虚拟环境是否存在
if [ ! -d "$SCRIPT_DIR/venv" ]; then
    echo "❌ 虚拟环境不存在，请先运行: python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt"
    exit 1
fi

# 激活虚拟环境
source "$SCRIPT_DIR/venv/bin/activate"

mkdir -p logs

# 解析命令行参数
PYTHON_ARGS=""
ALGO_LABEL=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --algorithm)
            PYTHON_ARGS="$PYTHON_ARGS --algorithm $2"
            ALGO_LABEL="_$2"
            shift 2
            ;;
        --test-limit)
            PYTHON_ARGS="$PYTHON_ARGS --test-limit $2"
            shift 2
            ;;
        --output)
            PYTHON_ARGS="$PYTHON_ARGS --output $2"
            shift 2
            ;;
        --seller)
            PYTHON_ARGS="$PYTHON_ARGS --seller $2"
            shift 2
            ;;
        --buyer)
            PYTHON_ARGS="$PYTHON_ARGS --buyer $2"
            shift 2
            ;;
        *)
            echo "未知参数: $1"
            echo "用法: $0 [--algorithm ALGO] [--test-limit NUM] [--output FILE] [--seller TAXNO] [--buyer TAXNO]"
            echo "示例: $0 --algorithm ffd --seller 91341103MA2TWC9B1Q --buyer 9134110275298062X0"
            exit 1
            ;;
    esac
done

LOG_FILE="logs/full_run${ALGO_LABEL}_$(date +%Y%m%d_%H%M%S).log"

# 使用 -u 标志运行Python，禁用输出缓冲
# 这样可以看到实时日志
# 使用虚拟环境中的 Python
nohup python -u red_blue_matcher.py $PYTHON_ARGS > "$LOG_FILE" 2>&1 &

PID=$!
echo "✅ 后台进程已启动（无缓冲模式）"
echo "   进程ID: $PID"
echo "   参数: $PYTHON_ARGS"
echo "   日志文件: $LOG_FILE"
echo ""
echo "📊 实时监控日志："
echo "   tail -f $LOG_FILE"
echo ""
echo "⏱️  统计进度（每10秒更新）："
echo "   watch -n 10 'wc -l $LOG_FILE'"

echo $PID > logs/full_run.pid

# 自动开始实时监控日志
echo ""
echo "🚀 开始实时监控日志（Ctrl+C 退出监控，不影响后台进程）..."
echo ""
tail -f "$LOG_FILE"
