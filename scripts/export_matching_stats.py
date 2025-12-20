#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
匹配统计SQL导出工具

功能：
1. 从环境配置文件读取数据库配置
2. 执行 scripts/匹配统计.sql 中的三个查询
3. 导出结果到 Excel 文件（3个sheet）

依赖：
- psycopg2-binary: PostgreSQL 数据库驱动
- xlsxwriter: Excel 文件写入
- python-dotenv: 环境变量管理

使用：
    # 默认加载 .env 文件
    python scripts/export_matching_stats.py

    # 指定环境名称，加载 .env.{ENV} 文件
    python scripts/export_matching_stats.py --env local      # 加载 .env.local
    python scripts/export_matching_stats.py --env prod       # 加载 .env.prod

    # 直接指定环境文件路径
    python scripts/export_matching_stats.py --env-file /path/to/.env.custom
"""

import os
import sys
import argparse
import psycopg2
import xlsxwriter
from datetime import datetime
from typing import Tuple, List, Any

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import load_config, get_db_config


# ========== 配置常量 ==========

# SQL文件路径
SQL_FILE_PATH = os.path.join(
    os.path.dirname(__file__),
    '匹配统计.sql'
)

# 表头映射
HEADERS_MAPPING = {
    'summary': [  # Sheet1: SKU 统计汇总表（对应query2）
        '序号',
        '待红冲 SKU 编码',
        '待红冲 SKU 总金额\n（不价税金额，后续金额全部取同样口径）',
        '待红冲 SKU 总数量',
        '待红冲 SKU 平均单价',
        '该 SKU红冲扣除蓝票的总数量',
        '该 SKU 红冲扣除蓝票的总金额',
        '该 SKU 红冲扣除蓝票的总数量 \n（按红冲扣除金额/可红冲单价，计算出来的数量）',
        '该 SKU红冲扣除蓝票的总行数',
        '该 SKU红冲扣除蓝票上，对应蓝票行剩余可红冲金额的合计总金额'
    ],
    'detail': [  # Sheet2: SKU 红冲扣除蓝票明细表（对应query1）
        '序号',
        '待红冲 SKU 编码',
        '该 SKU 红冲对应蓝票的fid',
        '该 SKU 红冲对应蓝票的发票号码',
        '该 SKU 红冲对应蓝票的开票日期',
        '该 SKU 红冲对应蓝票的发票行id',
        '该 SKU 红冲对应蓝票的发票行号',
        '该 SKU红冲对应蓝票行的剩余可红冲金额',
        '该 SKU红冲对应蓝票行的可红冲单价',
        '本次红冲扣除的红冲金额（正数）',
        '本次红冲扣除 SKU数量 \n（按红冲扣除金额/可红冲单价，计算出来的数量）',
        '扣除本次红冲后，对应蓝票行的剩余可红冲金额',
        '是否属于整行红冲\n（左侧计算出的剩余可红冲金额，小于等于 0.10 元，大于等于 0）'
    ],
    'invoice': [  # Sheet3: 整票红冲判断表（对应query3）
        '序号',
        '红冲计算结果对应的蓝票fid',
        '红冲计算结果对应的蓝票发票号码',
        '红冲计算结果对应的蓝票开票日期',
        '红冲计算结果对应蓝票的总行数',
        '红冲计算结果对应蓝票的总金额',
        '本次红冲结果运算扣除的蓝票总行数',
        '红冲计算结果对应蓝票的总剩余可红冲金额',
        '整张红冲的行数比例'
    ]
}

# 需要文本格式的列索引（防止科学计数法）
TEXT_COLUMNS = {
    'summary': [1],              # SKU编码
    'detail': [1, 2, 3, 5, 6],   # SKU编码, fid, 发票号码, 行id, 行号
    'invoice': [1, 2]            # fid, 发票号码
}

# 日期列索引
DATE_COLUMNS = {
    'summary': [],
    'detail': [4],   # 开票日期
    'invoice': [3]   # 开票日期
}


# ========== 核心函数 ==========

def parse_sql_file(sql_file_path: str) -> Tuple[str, str, str]:
    """
    解析SQL文件，提取三个查询

    Args:
        sql_file_path: SQL文件路径

    Returns:
        (query1, query2, query3) 元组
        - query1: SKU红冲扣除蓝票明细表查询
        - query2: SKU统计汇总表查询
        - query3: 整票红冲判断表查询

    Raises:
        FileNotFoundError: SQL文件不存在
    """
    if not os.path.exists(sql_file_path):
        raise FileNotFoundError(f"SQL文件不存在: {sql_file_path}")

    with open(sql_file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # 查询1（明细表）：第1-37行
    query1 = ''.join(lines[0:37]).strip()

    # 查询2（汇总表）：第39-56行
    query2 = ''.join(lines[38:56]).strip()

    # 查询3（整票红冲）：第59行到文件结尾
    query3 = ''.join(lines[58:]).strip()

    return query1, query2, query3


def format_cell_value(value: Any, col_idx: int,
                      text_columns: List[int],
                      date_columns: List[int]) -> Any:
    """
    格式化单元格值

    Args:
        value: 原始值
        col_idx: 列索引
        text_columns: 需要文本格式的列索引列表
        date_columns: 日期列索引列表

    Returns:
        格式化后的值
    """
    # None值处理
    if value is None:
        return ''

    # 日期格式化
    if col_idx in date_columns:
        if hasattr(value, 'strftime'):
            return value.strftime('%Y-%m-%d')
        return str(value)[:10]  # 截取日期部分

    # 文本格式（大整数）
    if col_idx in text_columns:
        return str(value)

    # 数值类型保持原样
    return value


def execute_query_and_write_sheet(
    cursor,
    query: str,
    worksheet,
    headers: List[str],
    text_columns: List[int],
    date_columns: List[int],
    header_format,
    text_format,
    special_header_formats: dict = None,
    column_formats: dict = None
) -> int:
    """
    执行查询并流式写入sheet

    Args:
        cursor: 数据库游标
        query: SQL查询语句
        worksheet: xlsxwriter worksheet对象
        headers: 表头列表
        text_columns: 需要文本格式的列索引
        date_columns: 日期列索引
        header_format: 表头格式
        text_format: 文本格式
        special_header_formats: 特殊列的表头格式 {列索引: 格式对象}
        column_formats: 特殊列的数据格式 {列索引: 格式对象}

    Returns:
        写入的数据行数（不含表头）
    """
    special_header_formats = special_header_formats or {}
    column_formats = column_formats or {}

    # 执行查询
    cursor.execute(query)

    # 写入表头
    for col_idx, header in enumerate(headers):
        # 使用特殊格式或默认格式
        fmt = special_header_formats.get(col_idx, header_format)
        worksheet.write(0, col_idx, header, fmt)

    # 流式写入数据
    row_idx = 1
    batch_size = 10000  # 每批1万行

    while True:
        rows = cursor.fetchmany(size=batch_size)
        if not rows:
            break

        for row in rows:
            for col_idx, value in enumerate(row):
                # 格式化值
                formatted_value = format_cell_value(
                    value, col_idx, text_columns, date_columns
                )

                # 写入单元格
                if col_idx in text_columns:
                    worksheet.write_string(row_idx, col_idx,
                                          str(formatted_value), text_format)
                elif col_idx in column_formats:
                    # 使用特殊格式（如百分比）
                    worksheet.write(row_idx, col_idx, formatted_value, column_formats[col_idx])
                else:
                    worksheet.write(row_idx, col_idx, formatted_value)

            row_idx += 1

    return row_idx - 1  # 返回数据行数


def get_output_path() -> str:
    """
    生成输出文件路径

    Returns:
        完整的输出文件路径
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'matching_stats_{timestamp}.xlsx'

    # 确保output目录存在
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'output')
    output_dir = os.path.abspath(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    return os.path.join(output_dir, filename)


def validate_columns(cursor, expected_count: int, query_name: str):
    """
    验证查询结果列数

    Args:
        cursor: 数据库游标
        expected_count: 期望的列数
        query_name: 查询名称（用于错误提示）

    Raises:
        ValueError: 列数不匹配
    """
    actual_count = len(cursor.description)
    if actual_count != expected_count:
        raise ValueError(
            f"{query_name}: 列数不匹配! "
            f"期望 {expected_count} 列，实际 {actual_count} 列"
        )


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='匹配统计SQL导出工具 - 导出匹配统计数据到Excel',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例：
  # 默认加载 .env 文件
  python scripts/export_matching_stats.py

  # 加载 .env.local 文件
  python scripts/export_matching_stats.py --env local

  # 加载 .env.prod 文件
  python scripts/export_matching_stats.py --env prod

  # 通过环境变量指定（不推荐，建议使用 --env 参数）
  ENV=local python scripts/export_matching_stats.py
        """
    )

    parser.add_argument(
        '--env',
        type=str,
        default=None,
        help='环境名称，用于加载 .env.{ENV} 文件（如: local, prod, test）'
    )

    parser.add_argument(
        '--env-file',
        type=str,
        default=None,
        dest='env_file',
        help='直接指定环境配置文件的完整路径'
    )

    return parser.parse_args()


def main():
    """主函数"""
    # 解析命令行参数
    args = parse_args()

    print("=" * 60)
    print("匹配统计SQL导出工具")
    print("=" * 60)

    # 1. 加载配置
    print("\n[1/5] 加载配置...")
    try:
        # 如果指定了 env-file，手动加载该文件
        if args.env_file:
            if not os.path.exists(args.env_file):
                raise FileNotFoundError(f"指定的配置文件不存在: {args.env_file}")

            # 手动加载指定的环境文件
            from dotenv import load_dotenv
            load_dotenv(args.env_file, override=True)
            print(f"已加载配置文件: {args.env_file}")

            # 调用 load_config() 但不传递参数，让它使用已加载的环境变量
            # 需要先重置配置状态，避免重复加载
            from config import reset_config
            reset_config()
            load_config()
        else:
            # 使用 --env 参数或默认配置
            load_config(env=args.env)

        db_config = get_db_config()
        print(f"✓ 配置加载成功")
    except Exception as e:
        print(f"✗ 配置加载失败: {e}")
        if args.env:
            print(f"请检查 .env.{args.env} 文件是否存在且配置正确")
        elif args.env_file:
            print(f"请检查配置文件 {args.env_file} 是否正确")
        else:
            print("请检查 .env 文件是否存在且配置正确，或使用 --env 参数指定环境")
        sys.exit(1)

    # 2. 解析SQL文件
    print("\n[2/5] 解析SQL文件...")
    try:
        query1, query2, query3 = parse_sql_file(SQL_FILE_PATH)
        print(f"✓ SQL解析成功（3个查询）")
    except Exception as e:
        print(f"✗ SQL解析失败: {e}")
        sys.exit(1)

    # 3. 连接数据库
    print("\n[3/5] 连接数据库...")
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        print(f"✓ 数据库连接成功")
    except psycopg2.OperationalError as e:
        print(f"✗ 数据库连接失败: {e}")
        print("请检查 .env.local 中的数据库配置")
        sys.exit(1)
    except Exception as e:
        print(f"✗ 连接失败: {e}")
        sys.exit(1)

    # 4. 执行查询并写入Excel
    print("\n[4/5] 执行查询并导出...")
    output_path = get_output_path()

    try:
        # 创建工作簿
        wb = xlsxwriter.Workbook(output_path, {'constant_memory': True})
        header_format = wb.add_format({'bold': True})
        text_format = wb.add_format({'num_format': '@'})

        # 特殊格式：红色表头（加粗 + 红色文字）
        red_header_format = wb.add_format({'bold': True, 'font_color': 'red'})

        # 特殊格式：百分比（保留2位小数）
        percentage_format = wb.add_format({'num_format': '0.00%'})

        # Sheet 1: SKU 统计汇总表（query2）
        print("  - 正在导出: SKU 统计汇总表...")
        ws_summary = wb.add_worksheet('SKU 统计汇总表')
        rows_summary = execute_query_and_write_sheet(
            cursor, query2, ws_summary,
            HEADERS_MAPPING['summary'],
            TEXT_COLUMNS['summary'],
            DATE_COLUMNS['summary'],
            header_format, text_format
        )
        print(f"    ✓ 已导出 {rows_summary} 行")

        # Sheet 2: SKU 红冲扣除蓝票明细表（query1）
        print("  - 正在导出: SKU 红冲扣除蓝票明细表...")
        ws_detail = wb.add_worksheet('SKU 红冲扣除蓝票明细表')
        rows_detail = execute_query_and_write_sheet(
            cursor, query1, ws_detail,
            HEADERS_MAPPING['detail'],
            TEXT_COLUMNS['detail'],
            DATE_COLUMNS['detail'],
            header_format, text_format
        )
        print(f"    ✓ 已导出 {rows_detail} 行")

        # Sheet 3: 整票红冲判断表（query3）
        print("  - 正在导出: 整票红冲判断表...")
        ws_invoice = wb.add_worksheet('整票红冲判断表')
        rows_invoice = execute_query_and_write_sheet(
            cursor, query3, ws_invoice,
            HEADERS_MAPPING['invoice'],
            TEXT_COLUMNS['invoice'],
            DATE_COLUMNS['invoice'],
            header_format, text_format,
            special_header_formats={8: red_header_format},  # 第8列表头为红色
            column_formats={8: percentage_format}  # 第8列数据为百分比格式
        )
        print(f"    ✓ 已导出 {rows_invoice} 行")

        # 关闭工作簿
        wb.close()
        print(f"\n✓ Excel导出成功")

    except psycopg2.Error as e:
        print(f"\n✗ SQL执行失败: {e}")
        import traceback
        traceback.print_exc()

        # 清理不完整的文件
        if os.path.exists(output_path):
            os.remove(output_path)

        cursor.close()
        conn.close()
        sys.exit(1)

    except Exception as e:
        print(f"\n✗ 导出失败: {e}")
        import traceback
        traceback.print_exc()

        # 清理不完整的文件
        if os.path.exists(output_path):
            os.remove(output_path)

        cursor.close()
        conn.close()
        sys.exit(1)

    # 5. 清理资源
    print("\n[5/5] 清理资源...")
    cursor.close()
    conn.close()

    # 输出结果
    print("\n" + "=" * 60)
    print("导出完成!")
    print(f"文件路径: {output_path}")
    file_size_mb = os.path.getsize(output_path) / 1024 / 1024
    print(f"文件大小: {file_size_mb:.2f} MB")
    print("=" * 60)


if __name__ == '__main__':
    main()
