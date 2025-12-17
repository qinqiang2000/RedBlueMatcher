#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
发票复用匹配策略 - Java兼容版 (Invoice Reuse Java-Compatible Strategy)

完全模拟 Java tax-redflush-service-java 的 batchMatchTempStrategy 算法。

核心差异（相比Python原生版本）：
1. 不维护蓝票余额，通过seenItemIds去重确保每行只用一次
2. 每条匹配记录对应一条蓝票行（不聚合）
3. 无"完全匹配"检查，允许部分匹配以正确更新matched_by_product

来源：Java RedFlushService.batchMatchTempStrategy()
"""

from decimal import Decimal, ROUND_HALF_UP
from typing import List, Dict, Tuple, Set, Optional

from .base import MatchingStrategy


class InvoiceReuseJavaStrategy(MatchingStrategy):
    """
    发票复用匹配策略 - Java兼容版

    核心算法逻辑（与Java完全一致）：
    1. 按itemId去重，每条蓝票行只使用一次
    2. 优先复用已匹配过的发票（跨SKU）
    3. 按稀缺度排序SKU处理顺序
    4. 无"完全匹配"检查，允许部分匹配
    """

    def __init__(self):
        super().__init__()
        # 分组内状态
        self._preferred_invoices: Set[int] = set()  # 已使用的发票fid集合
        self._sku_candidate_stats: Dict[str, Tuple[int, Decimal]] = {}  # 只按spbm分组，不含税率

    @property
    def name(self) -> str:
        return "invoice_reuse_java"

    def reset_preferred_invoices(self) -> None:
        """
        重置发票复用状态（每个销购方组开始时调用）
        """
        self._preferred_invoices.clear()

    def set_blue_pool(
        self,
        blue_pool: Dict[str, List]
    ) -> None:
        """
        设置蓝票池上下文，计算候选统计

        完全模仿Java：只按spbm分组，不区分税率
        使用剩余可红冲金额(fitemremainredamount)计算统计
        """
        self._sku_candidate_stats.clear()

        for spbm, candidates in blue_pool.items():
            # 统计候选数量和总金额（使用剩余金额fitemremainredamount）
            count = len(candidates)
            total_amount = sum(
                b.fitemremainredamount
                for b in candidates
                if b.fitemremainredamount and b.fitemremainredamount > Decimal('0')
            )
            self._sku_candidate_stats[spbm] = (count, total_amount)

    def pre_process_negatives(
        self,
        negatives: List
    ) -> List:
        """
        预处理负数单据：按稀缺度排序

        完全模仿Java：只按spbm排序，不考虑税率
        """
        def sort_key(neg):
            count, total = self._sku_candidate_stats.get(
                neg.fspbm, (999999, Decimal('999999999'))
            )
            return (count, total)

        return sorted(negatives, key=sort_key)

    def match_single_negative(
        self,
        negative,
        blue_pool: Dict[str, List],
        results: List,
        seq_counter: List[int],
        skip_validation: bool = False
    ) -> Tuple[bool, str]:
        """
        为单个负数明细匹配蓝票（Java兼容版）

        完全模仿Java batchMatchTempStrategy()：
        1. 只按spbm匹配，不区分税率
        2. 构建候选集合：优先复用的发票 + 常规候选
        3. 按itemId去重（每个SKU内部局部，同一itemId可被不同SKU复用）
        4. 顺序遍历候选直到填满目标金额
        5. 每条蓝票行在当前SKU内只使用一次（use = min(候选金额, 剩余目标)）
        """
        from red_blue_matcher import MatchResult

        match_key = negative.fspbm  # 只用spbm，不含税率

        if match_key not in blue_pool:
            reason = f"找不到匹配的蓝票 - SKU: {negative.fspbm}"
            return False, reason

        candidates = blue_pool[match_key]

        # 需要红冲的金额（转为正数）
        target_amount = abs(negative.famount)
        remaining = target_amount

        # ========== Java风格：构建候选集合 ==========
        # 1. 优先复用的发票中的候选（按金额升序，与Java matchOnInvoices一致）
        # 2. 常规候选（按金额降序，与Java matchByTaxAndProduct一致）
        # 3. 按itemId去重（每个SKU内部局部，与Java一致）

        # 创建局部 seen_item_ids（与 Java 一致，每个 SKU 内部去重，不影响其他 SKU）
        seen_item_ids = set()

        preferred_candidates = []
        other_candidates = []

        def get_amount(b):
            """获取候选金额：使用剩余可红冲金额"""
            return b.fitemremainredamount

        for blue in candidates:
            # 跳过已使用的蓝票行（当前SKU内去重）
            if blue.fentryid in seen_item_ids:
                continue

            # 跳过无效候选
            amount = get_amount(blue)
            if amount <= Decimal('0'):
                continue

            if blue.fid in self._preferred_invoices:
                preferred_candidates.append(blue)
            else:
                other_candidates.append(blue)

        # Java: preferredInvoices中的候选按金额升序（小的先用）
        preferred_candidates.sort(key=get_amount)
        # Java: 常规候选按金额降序（大的先用）
        other_candidates.sort(key=get_amount, reverse=True)

        # 合并候选：优先的在前
        sorted_candidates = preferred_candidates + other_candidates

        # ========== Java风格：顺序遍历候选直到填满 ==========
        for blue in sorted_candidates:
            if remaining <= Decimal('0.001'):
                break

            # 再次检查是否已使用（防止并发问题）
            if blue.fentryid in seen_item_ids:
                continue

            # Java: use = min(候选金额, 剩余目标)
            candidate_amount = get_amount(blue)
            use_amount = min(candidate_amount, remaining)

            if use_amount <= Decimal('0'):
                continue

            # 标记该蓝票行已使用
            seen_item_ids.add(blue.fentryid)

            # 记录已用发票（用于跨SKU复用）
            self._preferred_invoices.add(blue.fid)

            # 获取单价
            unit_price = blue.effective_price if blue.effective_price > 0 else (
                candidate_amount / blue.fitemremainrednum
                if blue.fitemremainrednum and blue.fitemremainrednum > 0
                else (use_amount if use_amount > 0 else Decimal('1'))  # 避免除零
            )

            # 记录匹配结果
            seq_counter[0] += 1
            results.append(MatchResult(
                seq=seq_counter[0],
                sku_code=negative.fspbm,
                blue_fid=blue.fid,
                blue_entryid=blue.fentryid,
                remain_amount_before=candidate_amount,
                unit_price=unit_price,
                matched_amount=use_amount,
                negative_fid=negative.fid,
                negative_entryid=negative.fentryid,
                blue_invoice_no=blue.finvoiceno,
                goods_name=negative.fgoodsname,
                fissuetime=blue.fissuetime,
                tax_rate=Decimal(blue.ftaxrate) if blue.ftaxrate else Decimal('0.13')
            ))

            remaining -= use_amount

        # 无论是否完全匹配，都接受结果（Java风格）
        # Java版本在循环内立即累加matchedByProduct，无需"完全匹配"检查
        if remaining >= target_amount:
            # 没有找到任何匹配
            reason = f"找不到可用的蓝票 - 单据: {negative.fbillno}, SKU: {negative.fspbm}"
            return False, reason

        # 即使未完全匹配，也返回成功（允许matched_by_product更新）
        return True, ""
