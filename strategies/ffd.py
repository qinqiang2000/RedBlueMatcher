#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FFD (First Fit Decreasing) 策略

算法特点：
- 负数降序排序：按金额绝对值从大到小处理负数发票
- 首个充足匹配：优先使用第一个金额充足的蓝票（即最大的可用蓝票）
- 避免碎片化：防止大蓝票被精确匹配到小负数，减少红票数量

理论依据：
基于 Bin Packing 问题的 First Fit Decreasing (FFD) 算法
- FFD 是经典的贪心策略，能有效最小化 bin 数量（红票数量）
- 通过降序处理和大额优先匹配，减少碎片化
"""

from decimal import Decimal, ROUND_HALF_UP
from typing import List, Dict, Tuple, Optional
import numpy as np

from .base import MatchingStrategy
from .greedy_large import (
    AMOUNT_TOLERANCE,
    TAX_TOLERANCE,
    validate_tail_diff
)


def find_first_sufficient_match(target_amount: Decimal,
                                 candidates: List) -> Optional[int]:
    """
    向量化查找第一个金额 >= target 的蓝票索引

    由于 candidates 已按金额降序排列，返回的是最大的充足蓝票
    这与 find_exact_match 不同：
    - find_exact_match: amount == target → 可能选择小额蓝票
    - find_first_sufficient_match: amount >= target → 总是选择最大的充足蓝票

    Args:
        target_amount: 目标金额（正数）
        candidates: 候选蓝票列表（已按金额降序排列）

    Returns:
        第一个充足蓝票的索引，未找到返回None
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

    # 向量化查找：第一个 >= target 的蓝票
    sufficient_indices = np.where(amounts_scaled >= target_scaled)[0]

    if len(sufficient_indices) > 0:
        # 返回第一个充足匹配的索引（即最大的可用蓝票）
        return int(sufficient_indices[0])

    return None


class FFDStrategy(MatchingStrategy):
    """
    FFD (First Fit Decreasing) 匹配策略

    算法逻辑：
    1. 预处理：按负数金额绝对值降序排序
    2. 快速路径：查找第一个充足的蓝票（而非精确匹配）
       - 只使用目标金额，而非蓝票全部余额
       - 保留大蓝票的剩余部分供后续匹配
    3. 常规路径：如无单个充足蓝票，则多票组合（复用 GreedyLargeStrategy 逻辑）
    """

    @property
    def name(self) -> str:
        return "ffd"

    def pre_process_negatives(self, negatives: List) -> List:
        """
        预处理负数发票：按金额绝对值降序排序

        这是 FFD 算法的第一步，确保大额负数优先匹配

        Args:
            negatives: 负数发票明细列表

        Returns:
            排序后的负数发票列表
        """
        return sorted(negatives, key=lambda x: abs(x.famount), reverse=True)

    def match_single_negative(
        self,
        negative,
        blue_pool: Dict[Tuple[str, str], List],
        results: List,
        seq_counter: List[int],
        skip_validation: bool = False
    ) -> Tuple[bool, str]:
        """
        为单个负数明细匹配蓝票（FFD 策略）

        核心区别：
        1. 快速路径：使用首个充足匹配，而非精确匹配
        2. 只消耗目标金额，而非蓝票全部余额
        3. 保留大蓝票剩余部分供后续使用

        Args:
            negative: 负数单据明细
            blue_pool: 蓝票池 {(spbm, taxrate): [BlueInvoiceItem]}
            results: 匹配结果列表
            seq_counter: 序号计数器 [当前序号]
            skip_validation: 是否跳过尾差校验（两阶段校验优化）

        Returns:
            (是否匹配成功, 失败原因)
        """
        # 延迟导入避免循环依赖
        from red_blue_matcher import MatchResult

        # 匹配键
        match_key = (negative.fspbm, negative.ftaxrate)

        if match_key not in blue_pool:
            reason = f"找不到匹配的蓝票 - SKU: {negative.fspbm}, 税率: {negative.ftaxrate}"
            print(f"  警告: {reason}")
            return False, reason

        candidates = blue_pool[match_key]

        # 需要红冲的金额（转为正数）
        target_amount = abs(negative.famount)
        remaining_amount = target_amount

        # 快速路径：首个充足匹配（FFD 核心逻辑）
        # 查找第一个金额 >= target 的蓝票（即最大的充足蓝票）
        sufficient_idx = find_first_sufficient_match(target_amount, candidates)
        if sufficient_idx is not None:
            blue = candidates[sufficient_idx]
            if blue.current_remain_amount > Decimal('0'):
                unit_price = blue.effective_price
                if unit_price > 0:
                    # 【关键区别】只使用目标金额，而非蓝票全部余额
                    # 这样可以保留大蓝票的剩余部分供后续匹配
                    final_match_amount = target_amount
                    final_match_num = (final_match_amount / unit_price).quantize(
                        Decimal('0.0000000000001'), ROUND_HALF_UP
                    )

                    # 尾差校验（如果启用）
                    if not skip_validation:
                        tax_rate = Decimal(blue.ftaxrate) if blue.ftaxrate else Decimal('0.13')
                        est_tax = (final_match_amount * tax_rate).quantize(Decimal('0.01'), ROUND_HALF_UP)
                        valid, msg = validate_tail_diff(
                            final_match_amount, final_match_num, unit_price, est_tax, tax_rate
                        )

                        if not valid:
                            # 快速路径失败，回退到常规路径
                            print(f"    FFD快速路径尾差校验失败，回退到常规路径: {msg}")
                            # 清空 sufficient_idx，强制进入常规路径
                            sufficient_idx = None

                    # 如果快速路径有效，执行匹配
                    if sufficient_idx is not None:
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

                        # FFD 快速路径一次性完成
                        return True, ""

        # 常规路径：遍历候选蓝票进行贪心匹配
        # 复用 GreedyLargeStrategy 的多票组合逻辑
        for blue in candidates:
            if remaining_amount <= Decimal('0'):
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

            # 计算基于整数数量的金额
            int_match_amount = (int_qty * unit_price).quantize(Decimal('0.01'), ROUND_HALF_UP)

            # 决策变量
            final_match_amount = Decimal('0')
            final_match_num = Decimal('0')
            use_integer = False

            # 校验整数方案是否可行
            if int_match_amount <= blue.current_remain_amount + AMOUNT_TOLERANCE:
                if not (not is_flush and int_match_amount > remaining_amount + AMOUNT_TOLERANCE):
                    # 校验通过尾差规则
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
                        print(f"    跳过蓝票 {blue.fid}: 无法满足尾差校验 ({msg})")
                        continue

            # 吃光策略修正
            if abs(blue.current_remain_amount - final_match_amount) < AMOUNT_TOLERANCE:
                final_match_amount = blue.current_remain_amount

            # 记录匹配前的余额
            remain_before = blue.current_remain_amount

            # 跳过零金额匹配
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
            reason = f"负数明细未完全匹配 - 单据: {negative.fbillno}, SKU: {negative.fspbm}, 剩余: {remaining_amount}"
            print(f"  警告: {reason}")
            return False, reason

        return True, ""
