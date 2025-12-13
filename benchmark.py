#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
性能基准测试脚本
用于测量 red_blue_matcher.py 的执行性能
"""

import time
import subprocess
import sys
import os
import glob as glob_module
from datetime import datetime
from result_writer import ResultWriter, OutputConfig


def run_benchmark(test_limit=100, num_runs=3):
    """
    运行性能基准测试

    Args:
        test_limit: 测试模式下处理的负数单据数量
        num_runs: 重复运行次数以获取平均值
    """
    print("=" * 80)
    print(f"负数发票匹蓝算法 - 性能基准测试")
    print(f"测试限制: {test_limit} 条负数单据")
    print(f"运行次数: {num_runs}")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print()

    script_path = os.path.join(
        os.path.dirname(__file__),
        'red_blue_matcher.py'
    )

    # 使用 ResultWriter 获取输出配置（与 red_blue_matcher.py 保持一致）
    output_base_name = f'benchmark_output_{test_limit}.csv'

    times = []
    actual_output_files = []  # 记录实际生成的文件

    for run_idx in range(num_runs):
        print(f"\n--- 运行 {run_idx + 1}/{num_runs} ---")

        start_time = time.time()

        # 运行匹蓝算法
        cmd = [
            sys.executable,
            script_path,
            '--test-limit', str(test_limit),
            '--output', output_base_name
        ]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )

            end_time = time.time()
            elapsed = end_time - start_time
            times.append(elapsed)

            print(f"✓ 完成 - 耗时: {elapsed:.2f} 秒")

            # 从输出中提取实际生成的文件路径
            for line in result.stdout.split('\n'):
                if '结果已导出到:' in line:
                    # 提取文件路径
                    file_path = line.split('结果已导出到:')[-1].strip()
                    actual_output_files.append(file_path)
                    break

            # 打印算法输出的关键信息
            if run_idx == 0:  # 只在第一次运行时打印详细输出
                print("\n算法输出:")
                print("-" * 80)
                print(result.stdout)
                print("-" * 80)

        except subprocess.CalledProcessError as e:
            print(f"✗ 运行失败")
            print(f"错误输出:\n{e.stderr}")
            return None

    # 计算统计信息
    avg_time = sum(times) / len(times)
    min_time = min(times)
    max_time = max(times)

    print("\n" + "=" * 80)
    print("基准测试结果")
    print("=" * 80)
    print(f"运行次数:     {num_runs}")
    print(f"平均耗时:     {avg_time:.2f} 秒")
    print(f"最快耗时:     {min_time:.2f} 秒")
    print(f"最慢耗时:     {max_time:.2f} 秒")

    if num_runs >= 3:
        import statistics
        stdev = statistics.stdev(times)
        print(f"标准差:       {stdev:.2f} 秒")

    print("=" * 80)
    print()

    # 输出实际生成的文件列表
    if actual_output_files:
        print(f"输出文件（最新）: {actual_output_files[-1]}")
        if len(actual_output_files) > 1:
            print(f"  所有输出文件: {len(actual_output_files)} 个")

    return {
        'avg': avg_time,
        'min': min_time,
        'max': max_time,
        'times': times,
        'output_files': actual_output_files
    }


def compare_outputs(file1, file2):
    """
    比较两个CSV输出文件（忽略序号列）
    用于验证并发优化后结果的正确性
    """
    import csv
    import hashlib

    def read_csv_without_seq(filepath):
        """读取CSV并排除序号列"""
        rows = []
        with open(filepath, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            headers = next(reader)

            # 找到序号列的索引
            seq_idx = headers.index('序号') if '序号' in headers else 0

            for row in reader:
                # 排除序号列，保留其他列
                filtered_row = [v for i, v in enumerate(row) if i != seq_idx]
                rows.append(tuple(filtered_row))

        # 排序以便比较（因为并发可能改变顺序）
        rows.sort()
        return rows

    try:
        rows1 = read_csv_without_seq(file1)
        rows2 = read_csv_without_seq(file2)

        if rows1 == rows2:
            print(f"✓ 文件内容一致（忽略序号和顺序）")
            return True
        else:
            print(f"✗ 文件内容不一致")
            print(f"  文件1行数: {len(rows1)}")
            print(f"  文件2行数: {len(rows2)}")

            # 找出差异
            set1 = set(rows1)
            set2 = set(rows2)
            only_in_1 = set1 - set2
            only_in_2 = set2 - set1

            if only_in_1:
                print(f"  仅在文件1中: {len(only_in_1)} 行")
                for row in list(only_in_1)[:3]:
                    print(f"    {row}")

            if only_in_2:
                print(f"  仅在文件2中: {len(only_in_2)} 行")
                for row in list(only_in_2)[:3]:
                    print(f"    {row}")

            return False

    except Exception as e:
        print(f"✗ 比较文件时出错: {e}")
        return False


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='性能基准测试')
    parser.add_argument(
        '--test-limit',
        type=int,
        default=100,
        help='测试模式下处理的负数单据数量（默认: 100）'
    )
    parser.add_argument(
        '--runs',
        type=int,
        default=3,
        help='重复运行次数（默认: 3）'
    )
    parser.add_argument(
        '--compare',
        nargs=2,
        metavar=('FILE1', 'FILE2'),
        help='比较两个CSV文件的内容（忽略序号）'
    )

    args = parser.parse_args()

    if args.compare:
        print("比较CSV文件...")
        compare_outputs(args.compare[0], args.compare[1])
    else:
        result = run_benchmark(
            test_limit=args.test_limit,
            num_runs=args.runs
        )

        if result:
            print(f"\n基准测试完成！")
            print(f"保存此结果以便后续对比优化效果。")
