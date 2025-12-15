#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
匹配算法策略基类

定义匹配算法的抽象接口，所有具体算法实现需继承此基类。
"""

from abc import ABC, abstractmethod
from decimal import Decimal
from typing import List, Dict, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from red_blue_matcher import NegativeItem, BlueInvoiceItem, MatchResult


class MatchingStrategy(ABC):
    """
    匹配算法策略基类

    所有匹配算法必须实现此接口:
    - name: 策略名称
    - match_single_negative(): 核心匹配逻辑

    可选覆盖:
    - pre_process_negatives(): 预处理负数单据（如排序）
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """
        策略名称（用于日志和配置）

        Returns:
            策略的唯一标识名称
        """
        pass

    @abstractmethod
    def match_single_negative(
        self,
        negative: 'NegativeItem',
        blue_pool: Dict[Tuple[str, str], List['BlueInvoiceItem']],
        results: List['MatchResult'],
        seq_counter: List[int],
        skip_validation: bool = False
    ) -> Tuple[bool, str]:
        """
        为单个负数明细匹配蓝票

        Args:
            negative: 负数单据明细
            blue_pool: 蓝票池 {(spbm, taxrate): [BlueInvoiceItem]}
            results: 匹配结果列表（直接追加结果）
            seq_counter: 序号计数器 [当前序号]
            skip_validation: 是否跳过尾差校验（两阶段校验优化）

        Returns:
            (是否匹配成功, 失败原因)
        """
        pass

    def pre_process_negatives(
        self,
        negatives: List['NegativeItem']
    ) -> List['NegativeItem']:
        """
        预处理负数单据列表（可选）

        子类可覆盖此方法进行预处理，如按金额排序等。
        默认实现：不做任何处理，直接返回原列表。

        Args:
            negatives: 原始负数单据列表

        Returns:
            处理后的负数单据列表
        """
        return negatives

    def set_blue_pool(
        self,
        blue_pool: Dict[Tuple[str, str], List['BlueInvoiceItem']]
    ) -> None:
        """
        设置蓝票池上下文（可选）

        在批量匹配开始前调用，允许策略访问完整的蓝票池信息。
        子类可覆盖此方法进行预计算，如统计候选数量等。
        默认实现：不做任何操作。

        Args:
            blue_pool: 蓝票池 {(spbm, taxrate): [BlueInvoiceItem]}
        """
        pass

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.name}')"
