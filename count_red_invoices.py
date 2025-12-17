#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统计红票数量工具
根据匹配结果分析需要开具的红票数量

规则: 一张红票只能对应一张蓝票
     因此需要开具的红票数量 = 被红冲的蓝票数量（即整票红冲判断表的行数）
"""

from python_calamine import CalamineWorkbook
import sys
from pathlib import Path


def count_red_invoices(excel_file: str):
    """
    统计需要开具的红票数量

    Args:
        excel_file: 匹配结果Excel文件路径
    """
    print(f"\n正在分析文件: {excel_file}")
    print("=" * 60)

    try:
        # 使用 openpyxl 流式读取（高效内存使用）
        sheet_name = 'SKU 红冲扣除蓝票明细表'

        # 检查文件大小
        file_size = Path(excel_file).stat().st_size
        print(f"📁 文件大小: {file_size / 1024 / 1024:.1f} MB")

        # 使用 calamine 高速读取 (基于 Rust)
        try:
            print("🔄 使用calamine高速读取...")

            wb = CalamineWorkbook.from_path(excel_file)

            # 检查工作表是否存在
            if sheet_name not in wb.sheet_names:
                print(f"❌ 错误: 文件中未找到工作表 '{sheet_name}'")
                print(f"   可用的工作表: {wb.sheet_names}")
                return

            ws = wb.get_sheet_by_name(sheet_name)
            data = ws.to_python()

            # 统计C列（索引2）唯一值，跳过表头
            invoice_numbers = set()
            for row in data[1:]:
                if len(row) > 2 and row[2]:
                    invoice_numbers.add(str(row[2]))

            total_rows = len(data) - 1
            unique_count = len(invoice_numbers)

            print(f"✅ 处理完成: {total_rows:,} 行")

        except Exception as e:
            print(f"❌ 读取Excel时出错: {e}")
            return

        print(f"\n📊 统计结果:")
        print(f"   明细表总行数: {total_rows} 行")
        print(f"   唯一蓝票fid数: {unique_count} 个")
        print(f"   需要开具的红票数量: {unique_count} 张")
        print(f"\n说明:")
        print(f"   - 一张红票只能对应一张蓝票")
        print(f"   - C列（该SKU红冲对应蓝票的fid）的唯一值 = 需要开具的红票数")
        print(f"   - 相当于Excel公式: =ROWS(UNIQUE(C2:C{total_rows + 1}))")

        # 显示前10个不同的蓝票fid
        print(f"\n📋 蓝票fid样例（前10个）:")
        sample_invoices = sorted(invoice_numbers)[:10]
        for i, inv_no in enumerate(sample_invoices, start=1):
            print(f"   {i}. {inv_no}")

        if unique_count > 10:
            print(f"   ... (还有 {unique_count - 10} 张)")

        # 显式关闭文件句柄（虽然pandas会自动处理）

        print("\n" + "=" * 60)
        print(f"✅ 结论: 需要开具 {unique_count} 张红票")
        print("=" * 60)

    except FileNotFoundError:
        print(f"❌ 错误: 文件不存在: {excel_file}")
    except Exception as e:
        print(f"❌ 错误: {e}")
        import traceback
        traceback.print_exc()


def main():
    """主函数"""
    if len(sys.argv) < 2:
        # 自动查找最新的输出文件
        output_dir = Path('./output')
        if not output_dir.exists():
            print("❌ 错误: output目录不存在")
            print("\n用法:")
            print("  python count_red_invoices.py <Excel文件路径>")
            print("\n示例:")
            print("  python count_red_invoices.py ./output/match_results_20251213_113609.xlsx")
            return

        # 查找最新的xlsx文件
        xlsx_files = sorted(output_dir.glob('match_results_*.xlsx'),
                           key=lambda x: x.stat().st_mtime,
                           reverse=True)

        if not xlsx_files:
            print("❌ 错误: output目录下没有找到匹配结果文件")
            print("\n用法:")
            print("  python count_red_invoices.py <Excel文件路径>")
            return

        excel_file = str(xlsx_files[0])
        print(f"ℹ️  自动选择最新文件: {excel_file}")
    else:
        excel_file = sys.argv[1]

    count_red_invoices(excel_file)


if __name__ == '__main__':
    main()
