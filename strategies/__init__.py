#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
匹配算法策略模块

提供策略基类、具体策略实现和工厂函数。

使用方式:
    from strategies import get_strategy, list_strategies

    # 获取策略实例
    strategy = get_strategy('greedy_large')

    # 列出所有可用策略
    available = list_strategies()
"""

from .base import MatchingStrategy
from .greedy_large import GreedyLargeStrategy
from .ffd import FFDStrategy

# 策略注册表
STRATEGIES = {
    'greedy_large': GreedyLargeStrategy,
    'ffd': FFDStrategy,
}

# 默认策略
DEFAULT_STRATEGY = 'greedy_large'


def get_strategy(name: str = None) -> MatchingStrategy:
    """
    根据名称获取策略实例

    Args:
        name: 策略名称，为 None 时使用默认策略

    Returns:
        策略实例

    Raises:
        ValueError: 策略名称不存在
    """
    if name is None:
        name = DEFAULT_STRATEGY

    if name not in STRATEGIES:
        available = ', '.join(STRATEGIES.keys())
        raise ValueError(f"未知策略: '{name}'。可用策略: {available}")

    return STRATEGIES[name]()


def list_strategies() -> list:
    """
    列出所有可用策略

    Returns:
        策略名称列表
    """
    return list(STRATEGIES.keys())


def register_strategy(name: str, strategy_class: type) -> None:
    """
    注册新策略

    Args:
        name: 策略名称
        strategy_class: 策略类（必须继承自 MatchingStrategy）

    Raises:
        TypeError: 策略类未继承 MatchingStrategy
        ValueError: 策略名称已存在
    """
    if not issubclass(strategy_class, MatchingStrategy):
        raise TypeError(f"策略类必须继承自 MatchingStrategy")

    if name in STRATEGIES:
        raise ValueError(f"策略 '{name}' 已存在")

    STRATEGIES[name] = strategy_class


__all__ = [
    'MatchingStrategy',
    'GreedyLargeStrategy',
    'FFDStrategy',
    'get_strategy',
    'list_strategies',
    'register_strategy',
    'STRATEGIES',
    'DEFAULT_STRATEGY',
]
