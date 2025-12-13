#!/bin/bash

mkdir -p logs

LOG_FILE="logs/full_run_$(date +%Y%m%d_%H%M%S).log"

# 使用 -u 标志运行Python，禁用输出缓冲
# 这样可以看到实时日志
nohup python3 -u red_blue_matcher.py > "$LOG_FILE" 2>&1 &

PID=$!
echo "✅ 后台进程已启动（无缓冲模式）"
echo "   进程ID: $PID"
echo "   日志文件: $LOG_FILE"
echo ""
echo "📊 实时监控日志："
echo "   tail -f $LOG_FILE"
echo ""
echo "⏱️  统计进度（每10秒更新）："
echo "   watch -n 10 'wc -l $LOG_FILE'"

echo $PID > logs/full_run.pid
