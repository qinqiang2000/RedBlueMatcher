#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
贪心大额优先匹配策略 (Greedy Large Strategy)

算法特点：
- 优先精确匹配：使用 NumPy 向量化查找金额完全匹配的蓝票
- 贪心消耗：按蓝票金额从大到小消耗
- 整数数量优先：尽量使红冲数量为整数

这是原始的默认算法，从 red_blue_matcher.py 提取而来。
"""

from decimal import Decimal, ROUND_HALF_UP
from typing import List, Dict, Tuple, Optional
import numpy as np

from .base import MatchingStrategy

# 尾差容差
AMOUNT_TOLERANCE = Decimal('0.01')
TAX_TOLERANCE = Decimal('0.06')


def find_exact_match(target_amount: Decimal,
                     candidates: List) -> Optional[int]:
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
                      candidates: List,
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
    - |单价 × 数量 - 金额| <= 0.01
    - |金额 × 税率 - 税额| <= 0.06
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


class GreedyLargeStrategy(MatchingStrategy):
    """
    贪心大额优先匹配策略

    算法逻辑：
    1. 快速路径：NumPy 向量化精确匹配
    2. 常规路径：遍历候选蓝票进行贪心匹配
       - 优先使用大额蓝票
       - 整数数量优先优化
       - 吃光策略：如果剩余极小则清零
    """

    @property
    def name(self) -> str:
        return "greedy_large"

    def match_single_negative(
        self,
        negative,
        blue_pool: Dict[Tuple[str, str], List],
        results: List,
        seq_counter: List[int],
        skip_validation: bool = False
    ) -> Tuple[bool, str]:
        """
        为单个负数明细匹配蓝票

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
                    return True, ""

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
                is_flush = False  # 是否吃光蓝票
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
            reason = f"负数明细未完全匹配 - 单据: {negative.fbillno}, SKU: {negative.fspbm}, 剩余: {remaining_amount}"
            print(f"  警告: {reason}")
            return False, reason

        return True, ""
