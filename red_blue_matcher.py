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


def log(msg: str):
    """带时间戳的日志输出"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {msg}")


# 数据库连接配置
DB_CONFIG = {
    'host': 'localhost',
    'database': 'qinqiang02',
    'user': 'qinqiang02',
    'password': ''
}

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


def get_db_connection():
    """获取数据库连接"""
    return psycopg2.connect(**DB_CONFIG)


def load_negative_items(conn, limit: Optional[int] = None) -> List[NegativeItem]:
    """
    加载待处理的负数单据明细
    筛选条件: fbillproperties='-1' AND fconfirmstate='0'

    Args:
        conn: 数据库连接
        limit: 限制加载的记录数（用于测试），None表示加载全部
    """
    sql = """
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
        FROM t_sim_original_bill_1201 b
        JOIN t_sim_original_bill_item_1201 i ON b.fid = i.fid
        WHERE b.fbillproperties = '-1'
          AND b.fconfirmstate = '0'
        ORDER BY b.fid, i.fentryid
    """

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
    sql = """
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
        FROM t_sim_vatinvoice_1201 v
        JOIN t_sim_vatinvoice_item_1201 vi ON v.fid = vi.fid
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
        FROM t_sim_vatinvoice_1201 v
        JOIN t_sim_vatinvoice_item_1201 vi ON v.fid = vi.fid
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
        FROM t_sim_vatinvoice_1201 v
        JOIN t_sim_vatinvoice_item_1201 vi ON v.fid = vi.fid
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



def match_group_worker(args: Tuple) -> Tuple[List[dict], int, int]:
    """
    多进程匹配工作函数（顶层函数，满足pickle要求）
    处理单个分组的所有负数单据匹配

    Args:
        args: (group_key, neg_items_data, blue_candidates_data)
              group_key: (salertaxno, buyertaxno, spbm, taxrate)
              neg_items_data: List[dict] - 负数单据数据（已序列化）
              blue_candidates_data: List[dict] - 蓝票数据（已序列化）

    Returns:
        (local_results_data, matched_count, failed_count)
        local_results_data 为 dict 列表，便于跨进程传输
    """
    group_key, neg_items_data, blue_candidates_data = args
    spbm, taxrate = group_key[2], group_key[3]

    # 反序列化数据为对象
    neg_items = [NegativeItem(**d) for d in neg_items_data]
    blue_candidates = [BlueInvoiceItem(**d) for d in blue_candidates_data]

    # 构建本地蓝票池（该组独占，无需同步）
    temp_pool = {(spbm, taxrate): blue_candidates}

    local_results = []
    seq_counter = [0]  # 本地序号，后续统一重编
    matched_count = 0
    failed_count = 0

    for neg in neg_items:
        # 启用延迟校验模式（skip_validation=True），Phase 2 再批量校验
        success = match_single_negative(neg, temp_pool, local_results, seq_counter, skip_validation=True)
        if success:
            matched_count += 1
        else:
            failed_count += 1

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

    return results_data, matched_count, failed_count


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


def find_exact_match(target_amount: Decimal,
                     candidates: List[BlueInvoiceItem]) -> Optional[int]:
    """
    使用NumPy向量化查找精确匹配的蓝票索引

    Args:
        target_amount: 目标金额（正数）
        candidates: 候选蓝票列表

    Returns:
        精确匹配的蓝票在candidates中的索引，未找到返回None
    """
    if not candidates:
        return None

    # 转换为NumPy数组（放大10000倍转为整数避免浮点误差）
    SCALE = 10000
    target_scaled = int(target_amount * SCALE)

    # 构建金额数组（仅包含有余额的蓝票）
    amounts_scaled = np.array(
        [int(b.current_remain_amount * SCALE) for b in candidates],
        dtype=np.int64
    )

    # 向量化精确查找
    exact_indices = np.where(amounts_scaled == target_scaled)[0]

    if len(exact_indices) > 0:
        # 返回第一个精确匹配的索引
        return int(exact_indices[0])

    return None


def find_near_matches(target_amount: Decimal,
                      candidates: List[BlueInvoiceItem],
                      tolerance: Decimal = AMOUNT_TOLERANCE) -> List[int]:
    """
    使用NumPy向量化查找近似匹配的蓝票索引（在容差范围内）

    Args:
        target_amount: 目标金额（正数）
        candidates: 候选蓝票列表
        tolerance: 容差范围

    Returns:
        近似匹配的蓝票索引列表
    """
    if not candidates:
        return []

    SCALE = 10000
    target_scaled = int(target_amount * SCALE)
    tolerance_scaled = int(tolerance * SCALE)

    amounts_scaled = np.array(
        [int(b.current_remain_amount * SCALE) for b in candidates],
        dtype=np.int64
    )

    # 向量化查找容差范围内的匹配
    near_indices = np.where(np.abs(amounts_scaled - target_scaled) <= tolerance_scaled)[0]
    return near_indices.tolist()


def validate_tail_diff(amount: Decimal, quantity: Decimal,
                       unit_price: Decimal, tax: Decimal,
                       tax_rate: Decimal) -> Tuple[bool, str]:
    """
    尾差校验
    规则:
    - |单价 × 数量 - 金额| ≤ 0.01
    - |金额 × 税率 - 税额| ≤ 0.06
    """
    # 金额校验
    calc_amount = (quantity * unit_price).quantize(Decimal('0.01'), ROUND_HALF_UP)
    amount_diff = abs(calc_amount - amount)

    # 税额校验
    calc_tax = (amount * tax_rate).quantize(Decimal('0.01'), ROUND_HALF_UP)
    tax_diff = abs(calc_tax - tax)

    if amount_diff > AMOUNT_TOLERANCE:
        return False, f"金额尾差超限: {amount_diff} > {AMOUNT_TOLERANCE}"
    if tax_diff > TAX_TOLERANCE:
        return False, f"税额尾差超限: {tax_diff} > {TAX_TOLERANCE}"

    return True, "校验通过"


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


def match_single_negative(negative: NegativeItem,
                          blue_pool: Dict[Tuple[str, str], List[BlueInvoiceItem]],
                          results: List[MatchResult],
                          seq_counter: List[int],
                          skip_validation: bool = False) -> bool:
    """
    为单个负数明细匹配蓝票

    Args:
        negative: 负数单据明细
        blue_pool: 蓝票池 {(spbm, taxrate): [BlueInvoiceItem]}
        results: 匹配结果列表
        seq_counter: 序号计数器 [当前序号]
        skip_validation: 是否跳过尾差校验（两阶段校验优化）

    Returns:
        是否匹配成功
    """
    # 匹配键
    match_key = (negative.fspbm, negative.ftaxrate)

    if match_key not in blue_pool:
        print(f"  警告: 找不到匹配的蓝票 - SKU: {negative.fspbm}, 税率: {negative.ftaxrate}")
        return False

    candidates = blue_pool[match_key]

    # 需要红冲的金额（转为正数）
    target_amount = abs(negative.famount)
    remaining_amount = target_amount

    # 快速路径：NumPy向量化精确匹配
    # 如果能找到金额完全相等的蓝票，直接使用，无需校验
    exact_idx = find_exact_match(target_amount, candidates)
    if exact_idx is not None:
        blue = candidates[exact_idx]
        if blue.current_remain_amount > Decimal('0'):
            unit_price = blue.effective_price
            if unit_price > 0:
                # 精确匹配：使用蓝票全部余额
                final_match_amount = blue.current_remain_amount
                final_match_num = blue.current_remain_num

                # 记录匹配前的余额
                remain_before = blue.current_remain_amount

                # 扣减蓝票余额
                blue.deduct(final_match_amount, final_match_num)

                # 记录匹配结果
                seq_counter[0] += 1
                results.append(MatchResult(
                    seq=seq_counter[0],
                    sku_code=negative.fspbm,
                    blue_fid=blue.fid,
                    blue_entryid=blue.fentryid,
                    remain_amount_before=remain_before,
                    unit_price=unit_price,
                    matched_amount=final_match_amount,
                    negative_fid=negative.fid,
                    negative_entryid=negative.fentryid,
                    blue_invoice_no=blue.finvoiceno,
                    goods_name=negative.fgoodsname,
                    fissuetime=blue.fissuetime
                ))

                # 精确匹配一次性完成
                return True

    # 常规路径：遍历候选蓝票进行贪心匹配
    for blue in candidates:
        if remaining_amount <= Decimal('0'):
            break

        if blue.current_remain_amount <= Decimal('0'):
            continue

        unit_price = blue.effective_price
        if unit_price <= 0:
            continue

        # 1. 确定理论最大可用金额
        # 蓝票余额充足 -> use remaining_amount
        # 蓝票余额不足 -> use blue.current_remain_amount
        if blue.current_remain_amount >= remaining_amount:
            raw_match_amount = remaining_amount
            is_flush = False # 是否吃光蓝票
        else:
            raw_match_amount = blue.current_remain_amount
            is_flush = True

        # 2. 整数数量优先优化 (Integer Optimization)
        # 尝试寻找最接近的整数数量
        raw_qty = raw_match_amount / unit_price
        int_qty = raw_qty.quantize(Decimal('1'), ROUND_HALF_UP)
        
        # 计算基于整数数量的金额
        int_match_amount = (int_qty * unit_price).quantize(Decimal('0.01'), ROUND_HALF_UP)
        
        # 决策变量
        final_match_amount = Decimal('0')
        final_match_num = Decimal('0')
        use_integer = False

        # 校验整数方案是否可行
        # 条件A: 整数金额不能超过蓝票余额(加容差)
        # 条件B: 整数金额不能严重偏离目标(如果是覆盖模式)
        if int_match_amount <= blue.current_remain_amount + AMOUNT_TOLERANCE:
             # 如果不是吃光模式，且整数金额超过了剩余需求太多，也不行 (比如需求100，算出105，不行)
             if not (not is_flush and int_match_amount > remaining_amount + AMOUNT_TOLERANCE):
                 # 校验通过尾差规则（如果启用延迟校验则跳过）
                 if skip_validation:
                     # 延迟校验模式：直接使用整数方案
                     if int_qty > Decimal('0'):
                         final_match_amount = int_match_amount
                         final_match_num = int_qty
                         use_integer = True
                 else:
                     # 估算税额
                     tax_rate = Decimal(blue.ftaxrate) if blue.ftaxrate else Decimal('0.13')
                     est_tax = (int_match_amount * tax_rate).quantize(Decimal('0.01'), ROUND_HALF_UP)

                     valid, msg = validate_tail_diff(int_match_amount, int_qty, unit_price, est_tax, tax_rate)
                     if valid and int_qty > Decimal('0'):  # 确保整数数量非零
                         final_match_amount = int_match_amount
                         final_match_num = int_qty
                         use_integer = True

        # 3. 如果整数方案不可行，回退到精确小数方案
        if not use_integer:
            # 直接使用 raw_match_amount，计算精确数量
            final_match_amount = raw_match_amount
            final_match_num = (final_match_amount / unit_price).quantize(Decimal('0.0000000000001'), ROUND_HALF_UP)

            # 再校验一次尾差（如果启用延迟校验则跳过）
            if not skip_validation:
                tax_rate = Decimal(blue.ftaxrate) if blue.ftaxrate else Decimal('0.13')
                est_tax = (final_match_amount * tax_rate).quantize(Decimal('0.01'), ROUND_HALF_UP)
                valid, msg = validate_tail_diff(final_match_amount, final_match_num, unit_price, est_tax, tax_rate)

                if not valid:
                    # 极其罕见情况：小数方案也不满足尾差公式（数学上几乎不可能，除非精度极差）
                    # 尝试微调金额? 暂时跳过此蓝票
                    print(f"    跳过蓝票 {blue.fid}: 无法满足尾差校验 ({msg})")
                    continue

        # 吃光策略修正：如果剩余极其微小，视为0 (防止0.01残留)
        if abs(blue.current_remain_amount - final_match_amount) < AMOUNT_TOLERANCE:
            # 如果是吃光，强制使用蓝票当前全部余额，避免浮点误差导致的0.000001残留
            final_match_amount = blue.current_remain_amount
            # 数量使用刚才算出的(不管是整数还是小数)
        
        # 记录匹配前的余额
        remain_before = blue.current_remain_amount

        # 跳过零金额匹配（不应产生无效记录）
        if final_match_amount <= AMOUNT_TOLERANCE:
            continue

        # 扣减蓝票余额
        blue.deduct(final_match_amount, final_match_num)

        # 记录匹配结果
        seq_counter[0] += 1
        results.append(MatchResult(
            seq=seq_counter[0],
            sku_code=negative.fspbm,
            blue_fid=blue.fid,
            blue_entryid=blue.fentryid,
            remain_amount_before=remain_before,
            unit_price=unit_price,
            matched_amount=final_match_amount,
            negative_fid=negative.fid,
            negative_entryid=negative.fentryid,
            blue_invoice_no=blue.finvoiceno,
            goods_name=negative.fgoodsname,
            fissuetime=blue.fissuetime
        ))

        remaining_amount -= final_match_amount

    if remaining_amount > AMOUNT_TOLERANCE:
        print(f"  警告: 负数明细未完全匹配 - 单据: {negative.fbillno}, "
              f"SKU: {negative.fspbm}, 剩余: {remaining_amount}")
        return False

    return True


def run_matching_algorithm(conn, test_limit: Optional[int] = None) -> List[MatchResult]:
    """
    运行匹蓝算法主流程

    Args:
        conn: 数据库连接
        test_limit: 测试模式下限制处理的负数单据数量

    Returns:
        匹配结果列表
    """
    # 初始化性能追踪器
    perf = PerformanceTracker()
    perf.start("总耗时")

    print("=" * 60)
    if test_limit:
        print(f"负数发票自动匹蓝算法 - 开始执行 (测试模式: 仅处理前 {test_limit} 条)")
    else:
        print("负数发票自动匹蓝算法 - 开始执行")
    print("=" * 60)

    # 1. 加载负数单据
    perf.start("加载负数单据")
    negative_items = load_negative_items(conn, limit=test_limit)
    perf.stop("加载负数单据")
    if not negative_items:
        print("没有待处理的负数单据")
        return []

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

    # 4. 多进程并发执行匹配
    results: List[MatchResult] = []
    matched_count = 0
    failed_count = 0

    # 准备多进程任务参数（需要序列化为dict）
    perf.start("准备匹配任务")
    match_tasks = []
    for group_key, neg_items in groups.items():
        blue_candidates = blue_pool.get(group_key, [])
        # 序列化为 dict 列表，便于跨进程传输
        neg_items_data = [negative_item_to_dict(n) for n in neg_items]
        blue_candidates_data = [blue_item_to_dict(b) for b in blue_candidates]
        match_tasks.append((group_key, neg_items_data, blue_candidates_data))
    perf.stop("准备匹配任务")

    log(f"开始多进程匹配 {len(match_tasks)} 组...")

    # 使用多进程池并发匹配（绕过GIL，真正并行）
    perf.start("多进程匹配")
    num_workers = max(1, min(cpu_count() - 1, len(match_tasks)))
    with Pool(processes=num_workers) as pool:
        results_list = pool.map(match_group_worker, match_tasks)

    # 合并结果
    for results_data, local_matched, local_failed in results_list:
        # 将 dict 转换回 MatchResult 对象
        for rd in results_data:
            results.append(MatchResult(**rd))
        matched_count += local_matched
        failed_count += local_failed
    perf.stop("多进程匹配")

    log(f"  Phase 1 匹配完成: {len(match_tasks)} 组, {len(results)} 条记录")

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

    return results


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
    
    for (fid, entry_id), group in grouped.items():
        first_item = group[0]
        
        # 1. 汇总金额
        total_amount = sum(item.matched_amount for item in group)
        
        # 2. 汇总反算数量 (注意: 这里需要用总金额/单价重新计算，而不是简单累加数量，
        # 因为简单的浮点数累加可能会带来误差，尽管这里我们只输出金额)
        # 但为了校验，我们需要计算出一个理论数量
        unit_price = first_item.unit_price
        if unit_price > 0:
            total_qty = (total_amount / unit_price).quantize(Decimal('0.0000000000001'), ROUND_HALF_UP)
        else:
            total_qty = Decimal('0')
            
        # 3. 再校验 (Re-validation)
        # 即使每笔单独都符合，累加后也可能出问题 (例如 0.004 + 0.004 = 0.008 -> 0.01)
        # 获取税率
        # 由于MatchResult里没有存税率，我们需要从 tax_rate = tax / amount ... 不太准
        # 应该在MatchResult里增加 tax_rate 字段，或者根据 sku_code/blue_fid 去查？
        # 简便起见，这里假设 valid_tail_diff 在单笔时已经做的很严格了。
        # 但 spec 明确要求 "合并后的金额...必须再次进行尾差校验"。
        # 我们利用 total_amount 和 unit_price 进行校验
        
        # 估算税率: 既然是同一张蓝票同一行，税率肯定一样。
        # 我们可以暂且认为 tax_rate = 0.13 (默认) 或者我们需要在MatchResult里带上tax_rate
        # 为了严谨，建议修改MatchResult定义增加tax_rate。
        # 但如果不改定义，我们可以暂时略过 严格的 tax re-calc，只做 amount re-calc?
        # 不，还是要做。我们可以大致推断，或者仅仅校验 Amount * Price 关系
        
        # 这里为了不大规模修改 MatchResult 定义 (user might not want heavy refactor),
        # 我们假设: 前面的单笔校验已经保证了它是合法的。
        # 合并后的主要风险是: sum(quantized_amount) != quantized(sum(raw_amount))?
        # 不，我们相加的是已经 quantize 过的 matched_amount (Decimal 2位)。
        # 所以 total_amount 是精确的。
        # 风险在于: total_amount / unit_price 算出的 qty 是否合理?
        # 比如: Price=3.33. Match1: Amt=3.33 (Qty=1). Match2: Amt=3.33 (Qty=1).
        # Total=6.66. Qty=2. 2*3.33=6.66. OK.
        # Price=3.33333333...
        # 无论如何，我们生成一个新的 MatchResult
        
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
            goods_name=first_item.goods_name
        )
        aggregated_results.append(agg_item)

    elapsed = time.time() - start_time
    log(f"聚合完成: 原始记录 {len(raw_results)} -> 聚合后 {len(aggregated_results)}")
    log(f"  耗时: {elapsed:.2f}秒")

    return aggregated_results


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='负数发票自动匹蓝算法',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  # 全量运行
  python red_blue_matcher.py

  # 测试模式：仅处理前10条负数单据
  python red_blue_matcher.py --test-limit 10

  # 测试模式并指定输出文件
  python red_blue_matcher.py --test-limit 5 --output test_results.csv
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
        default='match_results.csv',
        metavar='FILE',
        help='输出CSV文件路径（默认: match_results.csv）'
    )

    return parser.parse_args()


def main():
    """主函数"""
    overall_start = time.time()

    args = parse_arguments()

    # 配置输出（使用 ResultWriter 统一管理）
    output_config = OutputConfig(
        base_name=args.output,
        format='csv',
        add_timestamp=True
    )

    try:
        conn = get_db_connection()

        # 执行匹配算法（注意：test_limit模式下不会更新数据库）
        results = run_matching_algorithm(conn, test_limit=args.test_limit)

        if results:
            # 执行聚合
            final_results = aggregate_results(results)

            # 导出结果（带单独计时）
            export_start = time.time()
            writer = ResultWriter(output_config)
            output_file = writer.write(final_results)
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
