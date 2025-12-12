#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
性能追踪器模块
用于追踪算法各阶段的执行时间
"""

import time
from typing import Dict, List, Optional
from dataclasses import dataclass, field


@dataclass
class PerformanceTimer:
    """性能计时器"""
    name: str
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None

    def stop(self) -> float:
        """停止计时并返回耗时（秒）"""
        if self.end_time is None:
            self.end_time = time.time()
        return self.elapsed()

    def elapsed(self) -> float:
        """返回耗时（秒）"""
        if self.end_time is None:
            return time.time() - self.start_time
        return self.end_time - self.start_time


class PerformanceTracker:
    """性能追踪器"""

    def __init__(self):
        self.root: Optional[PerformanceTimer] = None
        self.timers: Dict[str, PerformanceTimer] = {}

    def start(self, name: str) -> PerformanceTimer:
        """开始一个新的计时段"""
        timer = PerformanceTimer(name=name)
        self.timers[name] = timer

        if self.root is None:
            self.root = timer

        return timer

    def stop(self, name: str) -> float:
        """停止指定的计时段"""
        if name in self.timers:
            return self.timers[name].stop()
        return 0.0

    def get_elapsed(self, name: str) -> float:
        """获取指定计时段的耗时"""
        if name in self.timers:
            return self.timers[name].elapsed()
        return 0.0

    def print_summary(self):
        """打印性能摘要"""
        if self.root is None:
            print("无性能数据")
            return

        total_time = self.root.elapsed()
        print("\n" + "=" * 60)
        print("性能分析报告")
        print("=" * 60)
        print(f"总耗时: {total_time:.2f}秒 (100.0%)")

        # 打印各阶段
        stage_names = [name for name in self.timers.keys() if name != self.root.name]
        for idx, name in enumerate(stage_names):
            timer = self.timers[name]
            elapsed = timer.elapsed()
            percentage = (elapsed / total_time * 100) if total_time > 0 else 0
            indent = "  "
            # 最后一个用 └─,其他用 ├─
            symbol = "└─" if idx == len(stage_names) - 1 else "├─"
            print(f"{indent}{symbol} {name}: {elapsed:.2f}秒 ({percentage:.1f}%)")

        print("=" * 60)

    def export_json(self, filepath: str):
        """导出性能数据到JSON"""
        import json

        data = {
            "total_time": self.root.elapsed() if self.root else 0,
            "stages": {
                name: {
                    "elapsed": timer.elapsed(),
                    "percentage": (timer.elapsed() / self.root.elapsed() * 100)
                                  if self.root and self.root.elapsed() > 0 else 0
                }
                for name, timer in self.timers.items()
                if timer != self.root
            }
        }

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
