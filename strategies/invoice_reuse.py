#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
发票复用匹配策略 (Invoice Reuse Strategy)

移植自 Java tax-redflush-service-java 的 batchMatchTempStrategy 算法。

算法特点：
1. 商品稀缺度排序：按候选蓝票行数升序、总金额升序（稀缺商品优先处理）
2. 发票复用：优先使用已匹配过的发票（减少发票数量）
3. 保留 Python 版的整数数量优化和尾差校验

来源：Java RedFlushService.batchMatchTempStrategy()
"""

from decimal import Decimal, ROUND_HALF_UP
from typing import List, Dict, Tuple, Set, Optional

from .base import MatchingStrategy
from .greedy_large import (
    AMOUNT_TOLERANCE,
    TAX_TOLERANCE,
    find_exact_match,
    validate_tail_diff
)


class InvoiceReuseStrategy(MatchingStrategy):
    """
    发票复用匹配策略

    核心算法逻辑：
    1. set_blue_pool(): 计算每个 SKU 的候选统计（行数、总金额）
    2. pre_process_negatives(): 按稀缺度排序（行数少、金额小优先）
    3. match_single_negative(): 发票复用 + 整数优化 + 尾差校验

    与 Java 版的差异：
    - 保留 Python 版的整数数量优化和尾差校验
    - 使用 NumPy 向量化精确匹配

    跨SKU发票复用优化：
    - _preferred_invoices 状态在 reset_preferred_invoices() 中重置
    - pre_process_negatives() 不再清空状态，支持跨SKU累积
    """

    def __init__(self):
        super().__init__()
        # 分组内状态
        # _preferred_invoices: 跨SKU共享，在 reset_preferred_invoices() 中重置
        # _sku_candidate_stats: 在 set_blue_pool() 中重置
        self._preferred_invoices: Set[int] = set()
        self._sku_candidate_stats: Dict[Tuple[str, str], Tuple[int, Decimal]] = {}

    @property
    def name(self) -> str:
        return "invoice_reuse"

    def reset_preferred_invoices(self) -> None:
        """
        重置发票复用状态

        在每个销购方组开始处理时调用，确保不同销购方之间的发票复用状态隔离。
        同一销购方下的多个SKU会共享发票复用状态（不调用此方法）。
        """
        self._preferred_invoices.clear()

    def set_blue_pool(
        self,
        blue_pool: Dict[Tuple[str, str], List]
    ) -> None:
        """
        设置蓝票池上下文，计算候选统计

        Args:
            blue_pool: 蓝票池 {(spbm, taxrate): [BlueInvoiceItem]}
        """
        self._sku_candidate_stats.clear()

        for (spbm, taxrate), candidates in blue_pool.items():
            # 统计有效候选（余额 > 0）
            valid_candidates = [
                b for b in candidates
                if b.current_remain_amount > Decimal('0')
            ]
            count = len(valid_candidates)
            total_amount = sum(
                b.current_remain_amount for b in valid_candidates
            )
            self._sku_candidate_stats[(spbm, taxrate)] = (count, total_amount)

    def pre_process_negatives(
        self,
        negatives: List
    ) -> List:
        """
        预处理负数单据：按稀缺度排序

        排序规则（与 Java 版一致）：
        1. 候选蓝票行数升序（稀缺商品优先）
        2. 候选蓝票总金额升序

        注意：不再清空 _preferred_invoices，以支持跨SKU发票复用。
        发票复用状态由 reset_preferred_invoices() 在销购方组开始时重置。

        Args:
            negatives: 原始负数单据列表

        Returns:
            排序后的负数单据列表
        """
        # 【改进】不再清空发票复用状态，支持跨SKU累积
        # self._preferred_invoices.clear()  # 移除此行

        def sort_key(neg):
            """排序键：(候选行数, 候选总金额)"""
            key = (neg.fspbm, neg.ftaxrate)
            # 找不到统计信息的放到最后处理
            count, total = self._sku_candidate_stats.get(
                key, (999999, Decimal('999999999'))
            )
            return (count, total)

        return sorted(negatives, key=sort_key)

    def match_single_negative(
        self,
        negative,
        blue_pool: Dict[Tuple[str, str], List],
        results: List,
        seq_counter: List[int],
        skip_validation: bool = False
    ) -> Tuple[bool, str]:
        """
        为单个负数明细匹配蓝票（发票复用版）

        核心逻辑：
        1. 重排序候选：已用发票优先
        2. 按 (fid, fentryid) 去重
        3. 贪心匹配 + 整数数量优化 + 尾差校验
        4. 记录已用发票

        Args:
            negative: 负数单据明细
            blue_pool: 蓝票池 {(spbm, taxrate): [BlueInvoiceItem]}
            results: 匹配结果列表
            seq_counter: 序号计数器 [当前序号]
            skip_validation: 是否跳过尾差校验

        Returns:
            (是否匹配成功, 失败原因)
        """
        # 延迟导入避免循环依赖
        from red_blue_matcher import MatchResult

        # 匹配键
        match_key = (negative.fspbm, negative.ftaxrate)

        if match_key not in blue_pool:
            reason = f"找不到匹配的蓝票 - SKU: {negative.fspbm}, 税率: {negative.ftaxrate}"
            return False, reason

        candidates = blue_pool[match_key]

        # 需要红冲的金额（转为正数）
        target_amount = abs(negative.famount)
        remaining_amount = target_amount

        # ========== 发票复用：重排序候选 ==========
        # 已用发票的候选放前面，其他的放后面
        # 同时按 (fid, fentryid) 去重
        preferred = []
        others = []
        seen_items: Set[Tuple[int, int]] = set()

        for blue in candidates:
            item_key = (blue.fid, blue.fentryid)
            if item_key in seen_items:
                continue
            seen_items.add(item_key)

            if blue.fid in self._preferred_invoices:
                preferred.append(blue)
            else:
                others.append(blue)

        # 合并：已用发票在前（保持原有的金额降序）
        sorted_candidates = preferred + others

        # ========== 快速路径：精确匹配 ==========
        exact_idx = find_exact_match(target_amount, sorted_candidates)
        if exact_idx is not None:
            blue = sorted_candidates[exact_idx]
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

                    # 记录已用发票
                    self._preferred_invoices.add(blue.fid)

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
                        fissuetime=blue.fissuetime,
                        tax_rate=Decimal(blue.ftaxrate) if blue.ftaxrate else Decimal('0.13')
                    ))

                    return True, ""

        # ========== 常规路径：贪心匹配 ==========
        for blue in sorted_candidates:
            if remaining_amount <= AMOUNT_TOLERANCE:
                break

            if blue.current_remain_amount <= Decimal('0'):
                continue

            unit_price = blue.effective_price
            if unit_price <= 0:
                continue

            # 1. 确定理论最大可用金额
            if blue.current_remain_amount >= remaining_amount:
                raw_match_amount = remaining_amount
                is_flush = False  # 是否吃光蓝票
            else:
                raw_match_amount = blue.current_remain_amount
                is_flush = True

            # 2. 整数数量优先优化
            raw_qty = raw_match_amount / unit_price
            int_qty = raw_qty.quantize(Decimal('1'), ROUND_HALF_UP)
            int_match_amount = (int_qty * unit_price).quantize(Decimal('0.01'), ROUND_HALF_UP)

            # 决策变量
            final_match_amount = Decimal('0')
            final_match_num = Decimal('0')
            use_integer = False

            # 校验整数方案是否可行
            if int_match_amount <= blue.current_remain_amount + AMOUNT_TOLERANCE:
                if not (not is_flush and int_match_amount > remaining_amount + AMOUNT_TOLERANCE):
                    if skip_validation:
                        if int_qty > Decimal('0'):
                            final_match_amount = int_match_amount
                            final_match_num = int_qty
                            use_integer = True
                    else:
                        tax_rate = Decimal(blue.ftaxrate) if blue.ftaxrate else Decimal('0.13')
                        est_tax = (int_match_amount * tax_rate).quantize(Decimal('0.01'), ROUND_HALF_UP)
                        valid, msg = validate_tail_diff(int_match_amount, int_qty, unit_price, est_tax, tax_rate)
                        if valid and int_qty > Decimal('0'):
                            final_match_amount = int_match_amount
                            final_match_num = int_qty
                            use_integer = True

            # 3. 如果整数方案不可行，回退到精确小数方案
            if not use_integer:
                final_match_amount = raw_match_amount
                final_match_num = (final_match_amount / unit_price).quantize(
                    Decimal('0.0000000000001'), ROUND_HALF_UP
                )

                if not skip_validation:
                    tax_rate = Decimal(blue.ftaxrate) if blue.ftaxrate else Decimal('0.13')
                    est_tax = (final_match_amount * tax_rate).quantize(Decimal('0.01'), ROUND_HALF_UP)
                    valid, msg = validate_tail_diff(final_match_amount, final_match_num, unit_price, est_tax, tax_rate)
                    if not valid:
                        continue

            # 4. 吃光策略修正
            if abs(blue.current_remain_amount - final_match_amount) < AMOUNT_TOLERANCE:
                final_match_amount = blue.current_remain_amount

            # 记录匹配前的余额
            remain_before = blue.current_remain_amount

            # 跳过零金额匹配
            if final_match_amount <= AMOUNT_TOLERANCE:
                continue

            # 扣减蓝票余额
            blue.deduct(final_match_amount, final_match_num)

            # 记录已用发票（核心：发票复用）
            self._preferred_invoices.add(blue.fid)

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
                fissuetime=blue.fissuetime,
                tax_rate=Decimal(blue.ftaxrate) if blue.ftaxrate else Decimal('0.13')
            ))

            remaining_amount -= final_match_amount

        if remaining_amount > AMOUNT_TOLERANCE:
            reason = f"负数明细未完全匹配 - 单据: {negative.fbillno}, SKU: {negative.fspbm}, 剩余: {remaining_amount}"
            return False, reason

        return True, ""
