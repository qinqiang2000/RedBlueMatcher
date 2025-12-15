#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
负数发票自动匹蓝算法
基于金税四期规则，将负数单据自动匹配到蓝票进行红冲

算法目标（优先级降序）:
1. 最小化蓝票数量 - 优先使用大额蓝票
2. 整行红冲优先 - 尽量将蓝票行余额一次冲完
3. 整数数量优先 - 红冲数量尽量为整数
4. 尾差控制 - 金额±0.01，税额±0.06
"""

import csv
import psycopg2
import argparse
import os
import sys
import time
from decimal import Decimal, ROUND_HALF_UP
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Tuple, Optional
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from multiprocessing import Pool, cpu_count
import numpy as np
from performance_tracker import PerformanceTracker
from result_writer import ResultWriter, OutputConfig
from config import load_config, get_db_config, get_tables
from strategies import get_strategy, list_strategies
from strategies.greedy_large import validate_tail_diff

def log(msg: str):
    """带时间戳的日志输出"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {msg}")


# 尾差容差
AMOUNT_TOLERANCE = Decimal('0.01')
TAX_TOLERANCE = Decimal('0.06')


@dataclass
class NegativeItem:
    """负数单据明细"""
    fid: int              # 单据主表ID
    fentryid: int         # 明细行ID
    fbillno: str          # 单据编号
    fspbm: str            # 商品编码
    fgoodsname: str       # 商品名称
    ftaxrate: str         # 税率
    famount: Decimal      # 金额(负数)
    fnum: Decimal         # 数量(负数)
    ftax: Decimal         # 税额(负数)
    fsalertaxno: str      # 销方税号
    fbuyertaxno: str      # 购方税号


@dataclass
class BlueInvoiceItem:
    """蓝票明细行"""
    fid: int                       # 发票主表ID
    fentryid: int                  # 明细行ID
    finvoiceno: str                # 发票号码
    fspbm: str                     # 商品编码
    fgoodsname: str                # 商品名称
    ftaxrate: str                  # 税率
    fitemremainredamount: Decimal  # 剩余可红冲金额
    fitemremainrednum: Decimal     # 剩余可红冲数量
    fredprice: Decimal             # 可红冲单价
    fissuetime: datetime           # 开票时间
    # 内存中维护的动态余额
    _current_remain_amount: Decimal = field(default=None, repr=False)
    _current_remain_num: Decimal = field(default=None, repr=False)

    def __post_init__(self):
        """初始化动态余额"""
        if self._current_remain_amount is None:
            self._current_remain_amount = self.fitemremainredamount
        if self._current_remain_num is None:
            self._current_remain_num = self.fitemremainrednum

    @property
    def current_remain_amount(self) -> Decimal:
        return self._current_remain_amount

    @property
    def current_remain_num(self) -> Decimal:
        return self._current_remain_num

    @property
    def effective_price(self) -> Decimal:
        """计算有效单价（考虑销售折让后的动态单价）"""
        if self.fredprice and self.fredprice > 0:
            return self.fredprice
        if self._current_remain_num and self._current_remain_num > 0:
            return self._current_remain_amount / self._current_remain_num
        return Decimal('0')

    def deduct(self, amount: Decimal, num: Decimal):
        """扣减余额"""
        self._current_remain_amount -= amount
        self._current_remain_num -= num
        # 吃光策略：如果余额极小则清零
        if abs(self._current_remain_amount) < AMOUNT_TOLERANCE:
            self._current_remain_amount = Decimal('0')
        if abs(self._current_remain_num) < Decimal('0.0001'):
            self._current_remain_num = Decimal('0')


@dataclass
class MatchResult:
    """匹配结果"""
    seq: int                    # 序号
    sku_code: str               # SKU编码
    blue_fid: int               # 蓝票fid
    blue_entryid: int           # 蓝票行号
    remain_amount_before: Decimal  # 匹配前剩余可红冲金额
    unit_price: Decimal         # 可红冲单价
    matched_amount: Decimal     # 本次红冲金额(正数)
    # 额外信息(便于调试)
    negative_fid: int = 0
    negative_entryid: int = 0
    blue_invoice_no: str = ''
    goods_name: str = ''
    fissuetime: datetime = None  # 蓝票开票日期
    tax_rate: Decimal = None     # 税率（用于聚合后尾差校验）


@dataclass
class SKUSummary:
    """SKU统计汇总"""
    seq: int                        # 序号
    sku_code: str                   # SKU编码
    original_total_amount: Decimal  # 待红冲SKU总金额（原始负数单据，不含税）
    original_total_quantity: Decimal # 待红冲SKU总数量（原始负数单据）
    original_avg_price: Decimal     # 待红冲SKU平均单价
    matched_blue_count: int         # 该SKU红冲扣除蓝票的总数量（票数）
    matched_total_amount: Decimal   # 该SKU红冲扣除蓝票的总金额
    matched_total_quantity: Decimal # 该SKU红冲扣除蓝票的总数量（按金额/单价计算）
    matched_line_count: int         # 该SKU红冲扣除蓝票的总行数
    remaining_blue_amount: Decimal  # 该SKU红冲扣除蓝票上，剩余可红冲金额合计


@dataclass
class FailedMatch:
    """匹配失败记录"""
    seq: int                    # 序号
    negative_fid: int           # 负数单据fid
    negative_entryid: int       # 负数单据行号
    negative_billno: str        # 负数单据编号
    sku_code: str               # SKU编码
    goods_name: str             # 商品名称
    tax_rate: str               # 税率
    amount: Decimal             # 金额（负数）
    quantity: Decimal           # 数量（负数）
    tax: Decimal                # 税额（负数）
    failed_reason: str          # 失败原因


@dataclass
class InvoiceRedFlushSummary:
    """整票红冲判断汇总（按蓝票维度统计）"""
    seq: int                              # 序号
    blue_fid: int                         # 红冲计算结果对应的蓝票fid
    blue_invoice_no: str                  # 红冲计算结果对应的蓝票发票号码
    blue_issue_date: datetime             # 红冲计算结果对应的蓝票开票日期
    original_line_count: int              # 红冲计算结果对应蓝票的总行数（原始）
    original_total_amount: Decimal        # 红冲计算结果对应蓝票的总金额（原始）
    total_remain_amount: Decimal          # 红冲计算结果对应蓝票的总剩余可红冲金额
    matched_line_count: int               # 本次红冲结果运算扣除的蓝票总行数
    matched_total_amount: Decimal         # 本次红冲结果运算扣除的蓝票总金额


@dataclass
class MatchingReport:
    """完整的匹配报告"""
    match_results: List[MatchResult]              # 成功匹配的明细
    sku_summaries: List[SKUSummary]               # SKU统计汇总
    failed_matches: List[FailedMatch]             # 匹配失败记录
    invoice_summaries: List[InvoiceRedFlushSummary] = field(default_factory=list)  # 整票红冲判断汇总


def get_db_connection():
    """获取数据库连接"""
    return psycopg2.connect(**get_db_config())


def load_negative_items(conn, limit: Optional[int] = None,
                        seller_taxno: Optional[str] = None,
                        buyer_taxno: Optional[str] = None) -> List[NegativeItem]:
    """
    加载待处理的负数单据明细
    筛选条件: fbillproperties='-1' AND fconfirmstate='0'

    Args:
        conn: 数据库连接
        limit: 限制加载的记录数（用于测试），None表示加载全部
        seller_taxno: 销方税号（可选，需同时指定 buyer_taxno）
        buyer_taxno: 购方税号（可选，需同时指定 seller_taxno）
    """
    tables = get_tables()
    sql = f"""
        SELECT
            b.fid,
            i.fentryid,
            b.fbillno,
            COALESCE(i.fspbm, '') as fspbm,
            COALESCE(i.fgoodsname, '') as fgoodsname,
            COALESCE(i.ftaxrate, '0.13') as ftaxrate,
            i.famount,
            i.fnum,
            i.ftax,
            b.fsalertaxno,
            b.fbuyertaxno
        FROM {tables.original_bill} b
        JOIN {tables.original_bill_item} i ON b.fid = i.fid
        WHERE b.fbillproperties = '-1'
          AND b.fconfirmstate = '0'
    """

    # 添加税号过滤条件（仅当同时指定时才生效）
    if seller_taxno and buyer_taxno:
        sql += f"      AND b.fsalertaxno = '{seller_taxno}'\n"
        sql += f"      AND b.fbuyertaxno = '{buyer_taxno}'\n"

    sql += "    ORDER BY b.fid, i.fentryid\n"

    if limit is not None:
        sql += f" LIMIT {limit}"

    items = []
    with conn.cursor() as cur:
        cur.execute(sql)
        for row in cur.fetchall():
            items.append(NegativeItem(
                fid=row[0],
                fentryid=row[1],
                fbillno=row[2],
                fspbm=row[3],
                fgoodsname=row[4],
                ftaxrate=row[5],
                famount=Decimal(str(row[6])),
                fnum=Decimal(str(row[7])),
                ftax=Decimal(str(row[8])),
                fsalertaxno=row[9],
                fbuyertaxno=row[10]
            ))

    # 日志输出
    if seller_taxno and buyer_taxno:
        log(f"过滤条件: 销方税号={seller_taxno}, 购方税号={buyer_taxno}")

    if limit is not None:
        log(f"加载了 {len(items)} 条待处理负数单据明细（测试模式: LIMIT {limit}）")
    else:
        log(f"加载了 {len(items)} 条待处理负数单据明细")
    return items


def load_candidate_blues(conn, salertaxno: str, buyertaxno: str,
                         spbm: str, taxrate: str) -> List[BlueInvoiceItem]:
    """
    加载候选蓝票明细
    条件: fissuetype='0', finvoicestatus IN ('0','2'), fspbm匹配, ftaxrate匹配, fitemremainredamount > 0
    排序: fitemremainredamount DESC, fissuetime ASC (优先大额，同额优先早期)
    """
    tables = get_tables()
    sql = f"""
        SELECT
            v.fid,
            vi.fentryid,
            v.finvoiceno,
            COALESCE(vi.fspbm, '') as fspbm,
            COALESCE(vi.fgoodsname, '') as fgoodsname,
            COALESCE(vi.ftaxrate, '0.13') as ftaxrate,
            vi.fitemremainredamount,
            vi.fitemremainrednum,
            vi.fredprice,
            v.fissuetime
        FROM {tables.vatinvoice} v
        JOIN {tables.vatinvoice_item} vi ON v.fid = vi.fid
        WHERE v.fissuetype = '0'
          AND v.finvoicestatus IN ('0', '2')
          AND v.fsalertaxno = %s
          AND v.fbuyertaxno = %s
          AND COALESCE(vi.fspbm, '') = %s
          AND COALESCE(vi.ftaxrate, '0.13') = %s
          AND vi.fitemremainredamount > 0
        ORDER BY vi.fitemremainredamount DESC, v.fissuetime ASC, vi.fentryid ASC
    """

    items = []
    with conn.cursor() as cur:
        cur.execute(sql, (salertaxno, buyertaxno, spbm, taxrate))
        for row in cur.fetchall():
            items.append(BlueInvoiceItem(
                fid=row[0],
                fentryid=row[1],
                finvoiceno=row[2],
                fspbm=row[3],
                fgoodsname=row[4],
                ftaxrate=row[5],
                fitemremainredamount=Decimal(str(row[6])),
                fitemremainrednum=Decimal(str(row[7])),
                fredprice=Decimal(str(row[8])) if row[8] else Decimal('0'),
                fissuetime=row[9]
            ))

    return items


def load_blue_worker(key: Tuple[str, str, str, str]) -> Tuple[Tuple[str, str, str, str], List[BlueInvoiceItem]]:
    """
    并发加载蓝票的工作线程函数
    每个线程创建独立的数据库连接（psycopg2连接非线程安全）

    Args:
        key: (salertaxno, buyertaxno, spbm, taxrate)

    Returns:
        (key, candidates): 原始key和查询结果
    """
    conn = get_db_connection()
    try:
        salertaxno, buyertaxno, spbm, taxrate = key
        candidates = load_candidate_blues(conn, salertaxno, buyertaxno, spbm, taxrate)
        return key, candidates
    finally:
        conn.close()


def load_blues_batch_by_seller_buyer(conn, seller_buyer_pairs: set) -> Dict[Tuple[str, str, str, str], List[BlueInvoiceItem]]:
    """
    按(销方,购方)批量加载蓝票，减少SQL查询次数

    Args:
        conn: 数据库连接
        seller_buyer_pairs: {(salertaxno, buyertaxno), ...} 唯一的销购方组合集合

    Returns:
        {(salertaxno, buyertaxno, spbm, taxrate): [BlueInvoiceItem]}
    """
    if not seller_buyer_pairs:
        return {}

    # 构建 WHERE IN 条件
    tables = get_tables()
    pairs_list = list(seller_buyer_pairs)
    placeholders = ', '.join(['(%s, %s)'] * len(pairs_list))
    params = []
    for s, b in pairs_list:
        params.extend([s, b])

    sql = f"""
        SELECT
            v.fid,
            vi.fentryid,
            v.finvoiceno,
            COALESCE(vi.fspbm, '') as fspbm,
            COALESCE(vi.fgoodsname, '') as fgoodsname,
            COALESCE(vi.ftaxrate, '0.13') as ftaxrate,
            vi.fitemremainredamount,
            vi.fitemremainrednum,
            vi.fredprice,
            v.fissuetime,
            v.fsalertaxno,
            v.fbuyertaxno
        FROM {tables.vatinvoice} v
        JOIN {tables.vatinvoice_item} vi ON v.fid = vi.fid
        WHERE v.fissuetype = '0'
          AND v.finvoicestatus IN ('0', '2')
          AND (v.fsalertaxno, v.fbuyertaxno) IN ({placeholders})
          AND vi.fitemremainredamount > 0
        ORDER BY v.fsalertaxno, v.fbuyertaxno, vi.fitemremainredamount DESC, v.fissuetime ASC, vi.fentryid ASC
    """

    # 执行查询并按 (salertaxno, buyertaxno, spbm, taxrate) 分组
    blue_pool: Dict[Tuple[str, str, str, str], List[BlueInvoiceItem]] = defaultdict(list)

    with conn.cursor() as cur:
        cur.execute(sql, params)
        for row in cur.fetchall():
            item = BlueInvoiceItem(
                fid=row[0],
                fentryid=row[1],
                finvoiceno=row[2],
                fspbm=row[3],
                fgoodsname=row[4],
                ftaxrate=row[5],
                fitemremainredamount=Decimal(str(row[6])),
                fitemremainrednum=Decimal(str(row[7])),
                fredprice=Decimal(str(row[8])) if row[8] else Decimal('0'),
                fissuetime=row[9]
            )
            key = (row[10], row[11], row[3], row[5])  # salertaxno, buyertaxno, spbm, taxrate
            blue_pool[key].append(item)

    return dict(blue_pool)


def load_blues_by_sku_batch(conn,
                            salertaxno: str,
                            buyertaxno: str,
                            sku_list: List[Tuple[str, str]]) -> Dict[Tuple[str, str], List[BlueInvoiceItem]]:
    """
    按SKU列表批量加载蓝票（分批优化版本）

    Args:
        conn: 数据库连接
        salertaxno: 销方税号
        buyertaxno: 购方税号
        sku_list: [(spbm, taxrate), ...] SKU和税率的组合列表

    Returns:
        {(spbm, taxrate): [BlueInvoiceItem]}
    """
    if not sku_list:
        return {}

    # 构建WHERE IN条件 - 针对(fspbm, ftaxrate)
    tables = get_tables()
    placeholders = ', '.join(['(%s, %s)'] * len(sku_list))
    params = [salertaxno, buyertaxno]
    for spbm, taxrate in sku_list:
        params.extend([spbm, taxrate])

    sql = f"""
        SELECT
            v.fid,
            vi.fentryid,
            v.finvoiceno,
            COALESCE(vi.fspbm, '') as fspbm,
            COALESCE(vi.fgoodsname, '') as fgoodsname,
            COALESCE(vi.ftaxrate, '0.13') as ftaxrate,
            vi.fitemremainredamount,
            vi.fitemremainrednum,
            vi.fredprice,
            v.fissuetime
        FROM {tables.vatinvoice} v
        JOIN {tables.vatinvoice_item} vi ON v.fid = vi.fid
        WHERE v.fissuetype = '0'
          AND v.finvoicestatus IN ('0', '2')
          AND v.fsalertaxno = %s
          AND v.fbuyertaxno = %s
          AND (vi.fspbm, vi.ftaxrate) IN ({placeholders})
          AND vi.fitemremainredamount > 0
        ORDER BY vi.fitemremainredamount DESC, v.fissuetime ASC, vi.fentryid ASC
    """

    blue_pool: Dict[Tuple[str, str], List[BlueInvoiceItem]] = defaultdict(list)

    with conn.cursor() as cur:
        cur.execute(sql, params)
        for row in cur.fetchall():
            item = BlueInvoiceItem(
                fid=row[0],
                fentryid=row[1],
                finvoiceno=row[2],
                fspbm=row[3],
                fgoodsname=row[4],
                ftaxrate=row[5],
                fitemremainredamount=Decimal(str(row[6])),
                fitemremainrednum=Decimal(str(row[7])),
                fredprice=Decimal(str(row[8])) if row[8] else Decimal('0'),
                fissuetime=row[9]
            )
            key = (row[3], row[5])  # (spbm, taxrate)
            blue_pool[key].append(item)

    return dict(blue_pool)


def load_invoice_original_data(conn, blue_fids: List[int]) -> Dict[int, Dict]:
    """
    批量查询蓝票的原始总行数和总金额

    Args:
        conn: 数据库连接
        blue_fids: 本次红冲涉及的蓝票fid列表

    Returns:
        {fid: {'original_line_count', 'original_total_amount', 'total_remain_amount'}}
    """
    if not blue_fids:
        return {}

    tables = get_tables()
    placeholders = ','.join(['%s'] * len(blue_fids))
    sql = f"""
        SELECT
            v.fid,
            COUNT(DISTINCT vi.fentryid) as original_line_count,
            SUM(vi.famount) as original_total_amount,
            SUM(vi.fitemremainredamount) as total_remain_amount
        FROM {tables.vatinvoice} v
        JOIN {tables.vatinvoice_item} vi ON v.fid = vi.fid
        WHERE v.fid IN ({placeholders})
        GROUP BY v.fid
    """

    invoice_data = {}
    with conn.cursor() as cur:
        cur.execute(sql, blue_fids)
        for row in cur.fetchall():
            invoice_data[row[0]] = {
                'original_line_count': row[1],
                'original_total_amount': Decimal(str(row[2])) if row[2] else Decimal('0'),
                'total_remain_amount': Decimal(str(row[3])) if row[3] else Decimal('0')
            }

    return invoice_data


def load_batch_worker(task_args):
    """
    并发加载蓝票批次的工作函数
    
    Args:
        task_args: (salertaxno, buyertaxno, batch_sku_list)
        
    Returns:
        (batch_result, elapsed_time)
    """
    salertaxno, buyertaxno, batch = task_args
    
    # 每个线程创建独立的数据库连接
    conn = get_db_connection()
    try:
        start_time = time.time()
        result = load_blues_by_sku_batch(conn, salertaxno, buyertaxno, batch)
        elapsed = time.time() - start_time
        return result, elapsed
    finally:
        conn.close()



def match_group_worker(args: Tuple) -> Tuple[List[dict], int, int, List[dict]]:
    """
    多进程匹配工作函数（顶层函数，满足pickle要求）
    处理单个分组的所有负数单据匹配

    Args:
        args: (group_key, neg_items_data, blue_candidates_data, strategy_name)
              group_key: (salertaxno, buyertaxno, spbm, taxrate)
              neg_items_data: List[dict] - 负数单据数据（已序列化）
              blue_candidates_data: List[dict] - 蓝票数据（已序列化）
              strategy_name: 策略名称

    Returns:
        (local_results_data, matched_count, failed_count, failed_items_data)
        local_results_data 为 dict 列表，便于跨进程传输
        failed_items_data 为失败的负数单据及原因
    """
    group_key, neg_items_data, blue_candidates_data, strategy_name = args
    spbm, taxrate = group_key[2], group_key[3]

    # 获取策略实例
    strategy = get_strategy(strategy_name)

    # 反序列化数据为对象
    neg_items = [NegativeItem(**d) for d in neg_items_data]
    blue_candidates = [BlueInvoiceItem(**d) for d in blue_candidates_data]

    # 构建本地蓝票池（该组独占，无需同步）
    temp_pool = {(spbm, taxrate): blue_candidates}

    # 设置蓝票池上下文（允许策略预计算统计信息）
    strategy.set_blue_pool(temp_pool)

    # 预处理负数单据（策略可覆盖，如排序）
    neg_items = strategy.pre_process_negatives(neg_items)

    local_results = []
    failed_items = []  # 记录失败的负数单据
    seq_counter = [0]  # 本地序号，后续统一重编
    matched_count = 0
    failed_count = 0

    for neg in neg_items:
        # 启用延迟校验模式（skip_validation=True），Phase 2 再批量校验
        success, reason = strategy.match_single_negative(neg, temp_pool, local_results, seq_counter, skip_validation=True)
        if success:
            matched_count += 1
        else:
            failed_count += 1
            # 记录失败信息
            failed_items.append({
                'negative': negative_item_to_dict(neg),
                'reason': reason
            })

    # 将结果转换为 dict 列表便于跨进程传输
    results_data = [
        {
            'seq': r.seq,
            'sku_code': r.sku_code,
            'blue_fid': r.blue_fid,
            'blue_entryid': r.blue_entryid,
            'remain_amount_before': r.remain_amount_before,
            'unit_price': r.unit_price,
            'matched_amount': r.matched_amount,
            'negative_fid': r.negative_fid,
            'negative_entryid': r.negative_entryid,
            'blue_invoice_no': r.blue_invoice_no,
            'goods_name': r.goods_name,
            'fissuetime': r.fissuetime
        }
        for r in local_results
    ]

    return results_data, matched_count, failed_count, failed_items


def negative_item_to_dict(item: NegativeItem) -> dict:
    """将 NegativeItem 转换为 dict，用于多进程序列化"""
    return {
        'fid': item.fid,
        'fentryid': item.fentryid,
        'fbillno': item.fbillno,
        'fspbm': item.fspbm,
        'fgoodsname': item.fgoodsname,
        'ftaxrate': item.ftaxrate,
        'famount': item.famount,
        'fnum': item.fnum,
        'ftax': item.ftax,
        'fsalertaxno': item.fsalertaxno,
        'fbuyertaxno': item.fbuyertaxno
    }


def blue_item_to_dict(item: BlueInvoiceItem) -> dict:
    """将 BlueInvoiceItem 转换为 dict，用于多进程序列化"""
    return {
        'fid': item.fid,
        'fentryid': item.fentryid,
        'finvoiceno': item.finvoiceno,
        'fspbm': item.fspbm,
        'fgoodsname': item.fgoodsname,
        'ftaxrate': item.ftaxrate,
        'fitemremainredamount': item.fitemremainredamount,
        'fitemremainrednum': item.fitemremainrednum,
        'fredprice': item.fredprice,
        'fissuetime': item.fissuetime,
        '_current_remain_amount': item._current_remain_amount,
        '_current_remain_num': item._current_remain_num
    }




def batch_validate_results(results: List[MatchResult],
                           default_tax_rate: Decimal = Decimal('0.13')) -> Tuple[List[MatchResult], List[MatchResult]]:
    """
    批量校验匹配结果（两阶段校验的Phase 2）

    Args:
        results: 待校验的匹配结果列表
        default_tax_rate: 默认税率

    Returns:
        (valid_results, invalid_results): 校验通过和未通过的结果
    """
    valid_results = []
    invalid_results = []

    for r in results:
        # 计算数量
        if r.unit_price > 0:
            qty = (r.matched_amount / r.unit_price).quantize(Decimal('0.0000000000001'), ROUND_HALF_UP)
        else:
            qty = Decimal('0')

        # 估算税额
        est_tax = (r.matched_amount * default_tax_rate).quantize(Decimal('0.01'), ROUND_HALF_UP)

        # 校验
        ok, msg = validate_tail_diff(r.matched_amount, qty, r.unit_price, est_tax, default_tax_rate)

        if ok:
            valid_results.append(r)
        else:
            invalid_results.append(r)

    return valid_results, invalid_results


def generate_sku_summaries(match_results: List[MatchResult],
                           original_stats: Dict[str, Dict[str, Decimal]]) -> List[SKUSummary]:
    """
    从匹配结果生成 SKU 汇总统计

    Args:
        match_results: 匹配结果列表
        original_stats: 原始负数单据统计 {sku_code: {'total_amount', 'total_quantity', 'goods_name'}}

    Returns:
        SKU 汇总列表
    """
    # 按 SKU 分组统计匹配结果
    sku_matched_stats: Dict[str, Dict] = defaultdict(lambda: {
        'total_amount': Decimal('0'),
        'total_quantity': Decimal('0'),
        'blue_count': set(),
        'line_count': 0,
        'remaining_amount': Decimal('0')
    })

    for r in match_results:
        sku = r.sku_code
        sku_matched_stats[sku]['total_amount'] += r.matched_amount
        sku_matched_stats[sku]['total_quantity'] += (r.matched_amount / r.unit_price).quantize(
            Decimal('0.0000000001'), ROUND_HALF_UP
        )
        sku_matched_stats[sku]['blue_count'].add((r.blue_fid, r.blue_entryid))
        sku_matched_stats[sku]['line_count'] += 1
        # 计算剩余金额
        remaining_after = r.remain_amount_before - r.matched_amount
        sku_matched_stats[sku]['remaining_amount'] += remaining_after

    # 生成汇总列表
    summaries = []
    for idx, (sku, orig_stat) in enumerate(sorted(original_stats.items()), start=1):
        matched_stat = sku_matched_stats.get(sku, {
            'total_amount': Decimal('0'),
            'total_quantity': Decimal('0'),
            'blue_count': set(),
            'line_count': 0,
            'remaining_amount': Decimal('0')
        })

        orig_amount = orig_stat['total_amount']
        orig_qty = orig_stat['total_quantity']
        avg_price = (orig_amount / orig_qty).quantize(Decimal('0.10'), ROUND_HALF_UP) if orig_qty > 0 else Decimal('0')

        summaries.append(SKUSummary(
            seq=idx,
            sku_code=sku,
            original_total_amount=orig_amount,
            original_total_quantity=orig_qty,
            original_avg_price=avg_price,
            matched_blue_count=len(matched_stat['blue_count']),
            matched_total_amount=matched_stat['total_amount'],
            matched_total_quantity=matched_stat['total_quantity'],
            matched_line_count=matched_stat['line_count'],
            remaining_blue_amount=matched_stat['remaining_amount']
        ))

    return summaries


def generate_invoice_summaries(match_results: List[MatchResult],
                               conn) -> List[InvoiceRedFlushSummary]:
    """
    从匹配结果生成整票红冲判断汇总

    Args:
        match_results: 匹配结果列表
        conn: 数据库连接

    Returns:
        整票红冲判断汇总列表
    """
    # 第一阶段：按蓝票fid分组统计本次红冲数据
    invoice_matched_stats: Dict[int, Dict] = defaultdict(lambda: {
        'blue_invoice_no': '',
        'blue_issue_date': None,
        'matched_total_amount': Decimal('0'),
        'matched_entry_ids': set()
    })

    for r in match_results:
        fid = r.blue_fid
        invoice_matched_stats[fid]['matched_total_amount'] += r.matched_amount
        invoice_matched_stats[fid]['matched_entry_ids'].add(r.blue_entryid)

        # 记录发票号码和开票日期（所有行相同，取第一个）
        if not invoice_matched_stats[fid]['blue_invoice_no']:
            invoice_matched_stats[fid]['blue_invoice_no'] = r.blue_invoice_no
            invoice_matched_stats[fid]['blue_issue_date'] = r.fissuetime

    # 计算每张票的匹配行数
    for fid, stats in invoice_matched_stats.items():
        stats['matched_line_count'] = len(stats['matched_entry_ids'])

    # 第二阶段：批量查询蓝票原始数据
    blue_fids = list(invoice_matched_stats.keys())
    invoice_original_data = load_invoice_original_data(conn, blue_fids)

    # 第三阶段：合并数据生成汇总
    summaries = []
    for idx, fid in enumerate(sorted(invoice_matched_stats.keys()), start=1):
        matched = invoice_matched_stats[fid]
        original = invoice_original_data.get(fid, {
            'original_line_count': 0,
            'original_total_amount': Decimal('0'),
            'total_remain_amount': Decimal('0')
        })

        summaries.append(InvoiceRedFlushSummary(
            seq=idx,
            blue_fid=fid,
            blue_invoice_no=matched['blue_invoice_no'],
            blue_issue_date=matched['blue_issue_date'],
            original_line_count=original['original_line_count'],
            original_total_amount=original['original_total_amount'],
            total_remain_amount=original['total_remain_amount'],
            matched_line_count=matched['matched_line_count'],
            matched_total_amount=matched['matched_total_amount']
        ))

    return summaries


def run_matching_algorithm(conn, test_limit: Optional[int] = None,
                           strategy_name: str = None,
                           seller_taxno: Optional[str] = None,
                           buyer_taxno: Optional[str] = None) -> MatchingReport:
    """
    运行匹蓝算法主流程

    Args:
        conn: 数据库连接
        test_limit: 测试模式下限制处理的负数单据数量
        strategy_name: 匹配策略名称，为 None 时使用默认策略
        seller_taxno: 销方税号（可选，需同时指定 buyer_taxno）
        buyer_taxno: 购方税号（可选，需同时指定 seller_taxno）

    Returns:
        完整的匹配报告（包含匹配结果、SKU统计、失败记录）
    """
    # 获取策略（用于日志和验证策略名称有效）
    strategy = get_strategy(strategy_name)
    strategy_name = strategy.name  # 确保使用规范化的策略名称

    # 初始化性能追踪器
    perf = PerformanceTracker()
    perf.start("总耗时")

    print("=" * 60)
    if test_limit:
        print(f"负数发票自动匹蓝算法 - 开始执行 (测试模式: 仅处理前 {test_limit} 条)")
    else:
        print("负数发票自动匹蓝算法 - 开始执行")
    log(f"使用匹配策略: {strategy.name}")
    print("=" * 60)

    # 1. 加载负数单据
    perf.start("加载负数单据")
    negative_items = load_negative_items(conn, limit=test_limit,
                                         seller_taxno=seller_taxno,
                                         buyer_taxno=buyer_taxno)
    perf.stop("加载负数单据")
    if not negative_items:
        print("没有待处理的负数单据")
        return MatchingReport(match_results=[], sku_summaries=[], failed_matches=[], invoice_summaries=[])

    # 1.1 收集原始负数单据统计（按SKU分组）
    perf.start("收集原始统计")
    original_sku_stats: Dict[str, Dict[str, Decimal]] = defaultdict(lambda: {
        'total_amount': Decimal('0'),
        'total_quantity': Decimal('0'),
        'goods_name': ''
    })
    for item in negative_items:
        sku = item.fspbm
        original_sku_stats[sku]['total_amount'] += abs(item.famount)
        original_sku_stats[sku]['total_quantity'] += abs(item.fnum)
        if not original_sku_stats[sku]['goods_name']:
            original_sku_stats[sku]['goods_name'] = item.fgoodsname
    perf.stop("收集原始统计")

    # 2. 按(销方税号, 购方税号, 商品编码, 税率)分组
    perf.start("数据分组")
    groups: Dict[Tuple[str, str, str, str], List[NegativeItem]] = defaultdict(list)
    for item in negative_items:
        key = (item.fsalertaxno, item.fbuyertaxno, item.fspbm, item.ftaxrate)
        groups[key].append(item)
    perf.stop("数据分组")

    log(f"分组数量: {len(groups)}")

    # 3. 构建蓝票池（按SKU分批加载优化）
    perf.start("批量加载蓝票")

    # 提取所有唯一的(salertaxno, buyertaxno)
    seller_buyer_pairs = set()
    for (salertaxno, buyertaxno, spbm, taxrate) in groups.keys():
        seller_buyer_pairs.add((salertaxno, buyertaxno))

    log(f"需要加载 {len(seller_buyer_pairs)} 对销购方的蓝票（SKU分批加载模式）")

    # 对于每个销购方对，按SKU分批加载
    blue_pool: Dict[Tuple[str, str, str, str], List[BlueInvoiceItem]] = {}
    batch_count = 0
    total_rows = 0

    # 准备所有批次任务
    batch_tasks = []
    BATCH_SIZE = 200  # 减小批次大小：1000 -> 200，避免单个查询数据量过大

    for salertaxno, buyertaxno in seller_buyer_pairs:
        # 提取该销购方对下的所有SKU
        sku_set = set()
        for (s, b, spbm, taxrate) in groups.keys():
            if s == salertaxno and b == buyertaxno:
                sku_set.add((spbm, taxrate))

        sku_list = list(sku_set)
        if not sku_list:
            continue
            
        log(f"  销购方对: 需要加载 {len(sku_list)} 个SKU")

        # 生成批次
        for i in range(0, len(sku_list), BATCH_SIZE):
            batch = sku_list[i:i+BATCH_SIZE]
            batch_tasks.append((salertaxno, buyertaxno, batch))

    log(f"  共生成 {len(batch_tasks)} 个加载批次，准备并发加载...")
    
    # 使用线程池并发加载
    # IO密集型任务，但需要控制数据库并发度，避免查询相互阻塞
    # 降低并发度：32 -> 4，避免数据库负载过高
    max_workers = min(4, os.cpu_count() or 4) 
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_batch = {
            executor.submit(load_batch_worker, task): task 
            for task in batch_tasks
        }
        
        # 处理结果
        for future in as_completed(future_to_batch):
            salertaxno, buyertaxno, batch = future_to_batch[future]
            try:
                batch_result, elapsed = future.result()
                
                # 统计
                batch_rows = sum(len(items) for items in batch_result.values())
                total_rows += batch_rows
                batch_count += 1
                
                # 合并到总池
                for (spbm, taxrate), items in batch_result.items():
                    full_key = (salertaxno, buyertaxno, spbm, taxrate)
                    blue_pool[full_key] = items
                
                log(f"    批次加载完成: {len(batch)} SKUs, {batch_rows} 行蓝票, {elapsed:.2f}秒")
                
            except Exception as exc:
                print(f"    批次加载异常: {exc}")

    perf.stop("批量加载蓝票")
    log(f"蓝票池加载完成: {batch_count} 批次, {total_rows} 行蓝票数据, {len(blue_pool)} 组")

    # 4. 并发执行匹配
    results: List[MatchResult] = []
    matched_count = 0
    failed_count = 0
    failed_records = []  # 收集失败的负数单据

    # 准备多进程任务参数（需要序列化为dict）
    perf.start("准备匹配任务")
    match_tasks = []
    for group_key, neg_items in groups.items():
        blue_candidates = blue_pool.get(group_key, [])
        # 序列化为 dict 列表，便于跨进程传输
        neg_items_data = [negative_item_to_dict(n) for n in neg_items]
        blue_candidates_data = [blue_item_to_dict(b) for b in blue_candidates]
        # 将策略名称传递给 worker
        match_tasks.append((group_key, neg_items_data, blue_candidates_data, strategy_name))
    perf.stop("准备匹配任务")

    log(f"开始多进程匹配 {len(match_tasks)} 组...")

    # 使用多进程池并发匹配（绕过GIL，真正并行）
    perf.start("多进程匹配")
    num_workers = max(1, min(cpu_count() - 1, len(match_tasks)))
    with Pool(processes=num_workers) as pool:
        results_list = pool.map(match_group_worker, match_tasks)

    # 合并结果
    for results_data, local_matched, local_failed, failed_items_data in results_list:
        # 将 dict 转换回 MatchResult 对象
        for rd in results_data:
            results.append(MatchResult(**rd))
        matched_count += local_matched
        failed_count += local_failed
        # 收集失败记录
        for item in failed_items_data:
            failed_records.append(item)
    perf.stop("多进程匹配")

    log(f"  Phase 1 匹配完成: {len(groups)} 组, {len(results)} 条记录")

    # 5. Phase 2: 批量校验（两阶段校验优化）
    perf.start("Phase 2 批量校验")
    log("开始 Phase 2 批量校验...")
    valid_results, invalid_results = batch_validate_results(results)
    perf.stop("Phase 2 批量校验")

    if invalid_results:
        print(f"  警告: {len(invalid_results)} 条记录未通过尾差校验（已过滤）")

    # 使用校验通过的结果
    results = valid_results

    # 6. 统一重新编号（因为并发执行导致序号乱序）
    for idx, result in enumerate(results, start=1):
        result.seq = idx

    # 7. 生成 SKU 汇总统计
    perf.start("生成SKU汇总统计")
    sku_summaries = generate_sku_summaries(results, original_sku_stats)
    perf.stop("生成SKU汇总统计")

    # 8. 生成失败匹配列表
    perf.start("生成失败匹配列表")
    failed_matches = []
    for idx, item in enumerate(failed_records, start=1):
        neg_data = item['negative']
        failed_matches.append(FailedMatch(
            seq=idx,
            negative_fid=neg_data['fid'],
            negative_entryid=neg_data['fentryid'],
            negative_billno=neg_data['fbillno'],
            sku_code=neg_data['fspbm'],
            goods_name=neg_data['fgoodsname'],
            tax_rate=neg_data['ftaxrate'],
            amount=Decimal(str(neg_data['famount'])),
            quantity=Decimal(str(neg_data['fnum'])),
            tax=Decimal(str(neg_data['ftax'])),
            failed_reason=item['reason']
        ))
    perf.stop("生成失败匹配列表")

    # 9. 生成整票红冲判断汇总
    perf.start("生成整票红冲判断汇总")
    invoice_summaries = generate_invoice_summaries(results, conn)
    perf.stop("生成整票红冲判断汇总")

    # 停止总计时
    perf.stop("总耗时")

    print(f"\n匹配完成:")
    print(f"  成功: {matched_count}")
    print(f"  失败: {failed_count}")
    print(f"  生成匹配记录: {len(results)}")
    if invalid_results:
        print(f"  校验过滤: {len(invalid_results)}")

    # 打印性能摘要
    perf.print_summary()

    # 返回完整报告
    return MatchingReport(
        match_results=results,
        sku_summaries=sku_summaries,
        failed_matches=failed_matches,
        invoice_summaries=invoice_summaries
    )


def print_statistics(results: List[MatchResult]):
    """打印匹配统计信息"""
    if not results:
        return

    total_amount = sum(r.matched_amount for r in results)
    unique_blues = len(set((r.blue_fid, r.blue_entryid) for r in results))
    unique_skus = len(set(r.sku_code for r in results))

    print(f"\n统计信息:")
    print(f"  总红冲金额: {total_amount:,.2f}")
    print(f"  使用蓝票行数: {unique_blues}")
    print(f"  涉及SKU数: {unique_skus}")


def aggregate_results(raw_results: List[MatchResult]) -> List[MatchResult]:
    """
    聚合匹配结果
    规则: 按 (blue_fid, blue_entryid) 进行合并
    验证: 合并后的总金额和总税额必须再次满足尾差校验
    """
    start_time = time.time()

    log("\n正在聚合匹配结果...")

    # Key: (blue_fid, blue_entryid)
    # Value: List[MatchResult]
    grouped: Dict[Tuple[int, int], List[MatchResult]] = defaultdict(list)
    
    for res in raw_results:
        grouped[(res.blue_fid, res.blue_entryid)].append(res)
        
    aggregated_results: List[MatchResult] = []
    new_seq = 0
    
    # 聚合后尾差校验的统计
    tail_diff_warnings = 0

    for (fid, entry_id), group in grouped.items():
        first_item = group[0]

        # 1. 汇总金额
        total_amount = sum(item.matched_amount for item in group)

        # 2. 汇总反算数量 (用总金额/单价重新计算)
        unit_price = first_item.unit_price
        if unit_price > 0:
            total_qty = (total_amount / unit_price).quantize(Decimal('0.0000000000001'), ROUND_HALF_UP)
        else:
            total_qty = Decimal('0')

        # 3. 获取税率（从 MatchResult 中获取）
        tax_rate = first_item.tax_rate if first_item.tax_rate else Decimal('0.13')

        # 4. 聚合后尾差重新校验
        # 金额校验: |单价×数量-金额| ≤ 0.01
        if unit_price > 0:
            calc_amount = (total_qty * unit_price).quantize(Decimal('0.01'), ROUND_HALF_UP)
            amount_diff = abs(calc_amount - total_amount)
            if amount_diff > AMOUNT_TOLERANCE:
                tail_diff_warnings += 1
                # 仅记录警告，不阻断流程（因为单笔已校验通过）

        # 过滤零金额记录（聚合后仍需检查）
        if total_amount <= AMOUNT_TOLERANCE:
            continue

        new_seq += 1
        agg_item = MatchResult(
            seq=new_seq,
            sku_code=first_item.sku_code,
            blue_fid=fid,
            blue_entryid=entry_id,
            remain_amount_before=first_item.remain_amount_before, #用第一笔的余额
            unit_price=unit_price,
            matched_amount=total_amount,
            negative_fid=0, # 聚合后不再指向单一负数单据
            negative_entryid=0,
            blue_invoice_no=first_item.blue_invoice_no,
            goods_name=first_item.goods_name,
            tax_rate=tax_rate  # 保留税率用于后续校验
        )
        aggregated_results.append(agg_item)

    if tail_diff_warnings > 0:
        log(f"  聚合后尾差校验警告: {tail_diff_warnings} 条")

    elapsed = time.time() - start_time
    log(f"聚合完成: 原始记录 {len(raw_results)} -> 聚合后 {len(aggregated_results)}")
    log(f"  耗时: {elapsed:.2f}秒")

    return aggregated_results


def parse_arguments():
    """解析命令行参数"""
    # 获取可用策略列表
    available_strategies = list_strategies()

    parser = argparse.ArgumentParser(
        description='负数发票自动匹蓝算法',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
示例用法:
  # 全量运行（使用默认 Python 实现）
  python red_blue_matcher.py

  # 测试模式：仅处理前10条负数单据
  python red_blue_matcher.py --test-limit 10

  # 指定匹配算法
  python red_blue_matcher.py --algorithm ffd

可用算法: {', '.join(available_strategies)}
        """
    )

    parser.add_argument(
        '--test-limit',
        type=int,
        default=None,
        metavar='N',
        help='测试模式：仅处理前N条负数单据（不更新数据库状态）'
    )

    parser.add_argument(
        '--output',
        type=str,
        default='match_results.xlsx',
        metavar='FILE',
        help='输出XLSX文件路径（默认: match_results.xlsx）'
    )

    parser.add_argument(
        '--algorithm',
        type=str,
        default=None,
        metavar='NAME',
        choices=available_strategies,
        help=f'匹配算法（可选: {", ".join(available_strategies)}，默认: greedy_large）'
    )

    parser.add_argument(
        '--seller',
        type=str,
        default=None,
        metavar='TAXNO',
        help='销方税号（需同时指定 --buyer，用于临时测试）'
    )

    parser.add_argument(
        '--buyer',
        type=str,
        default=None,
        metavar='TAXNO',
        help='购方税号（需同时指定 --seller，用于临时测试）'
    )

    return parser.parse_args()


def main():
    """主函数"""
    overall_start = time.time()

    # 加载配置
    try:
        load_config()
    except Exception as e:
        print(f"❌ 配置加载失败: {e}")
        print("请检查 .env 文件是否存在且配置正确")
        print("提示: 可从 .env.example 复制并修改配置")
        sys.exit(1)

    args = parse_arguments()

    # 验证税号参数（必须同时指定）
    seller_taxno = None
    buyer_taxno = None
    if args.seller and args.buyer:
        seller_taxno = args.seller
        buyer_taxno = args.buyer
        log(f"启用税号过滤: 销方={seller_taxno}, 购方={buyer_taxno}")
    elif args.seller or args.buyer:
        log("⚠️  警告: --seller 和 --buyer 必须同时指定才生效，已忽略过滤条件")

    # 获取策略以获取规范化的算法名称
    strategy = get_strategy(args.algorithm)
    algorithm_name = strategy.name  # 确保使用规范化的策略名称

    # 配置输出（使用 ResultWriter 统一管理）
    output_config = OutputConfig(
        base_name=args.output,
        format='xlsx',
        add_timestamp=True,
        sheet_name='SKU 红冲扣除蓝票明细表',
        algorithm=algorithm_name
    )

    try:
        conn = get_db_connection()

        # 执行匹配算法（注意：test_limit模式下不会更新数据库）
        report = run_matching_algorithm(conn, test_limit=args.test_limit,
                                        strategy_name=args.algorithm,
                                        seller_taxno=seller_taxno,
                                        buyer_taxno=buyer_taxno)

        if report.match_results:
            # 执行聚合
            final_results = aggregate_results(report.match_results)

            # 导出结果（带单独计时）
            export_start = time.time()
            writer = ResultWriter(output_config)
            output_file = writer.write(final_results, report.sku_summaries, report.failed_matches, report.invoice_summaries)
            export_elapsed = time.time() - export_start
            log(f"结果已导出到: {output_file}")
            log(f"导出耗时: {export_elapsed:.2f}秒")

            # 打印统计
            print_statistics(final_results)  # 统计使用的是聚合后的数据

            if args.test_limit:
                print("\n" + "=" * 60)
                print("⚠️  测试模式运行完成")
                print("   - 数据库未被修改（负数单据状态保持不变）")
                print("   - 仅供测试和验证算法结果")
                print("=" * 60)

        conn.close()

        overall_elapsed = time.time() - overall_start
        print(f"\n🎯 总执行时间: {overall_elapsed:.2f}秒")

        print("\n算法执行完成!")

    except Exception as e:
        print(f"执行出错: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == '__main__':
    main()
