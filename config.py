#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库配置模块
管理数据库连接和表名配置，支持多环境切换
"""

import os
import sys
from dataclasses import dataclass
from typing import Dict, Optional
from dotenv import load_dotenv


@dataclass
class DatabaseConfig:
    """数据库连接配置"""
    host: str
    port: int
    database: str
    user: str
    password: str

    def to_dict(self) -> Dict[str, any]:
        """转换为 psycopg2.connect() 所需的字典格式"""
        return {
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'user': self.user,
            'password': self.password
        }


class TableConfig:
    """表名配置类 - 动态构造表名"""

    def __init__(self, suffix: str = ""):
        """
        初始化表名配置

        Args:
            suffix: 表名后缀（包含下划线，如 "_1201"），默认为空字符串
        """
        self.suffix = suffix

    @property
    def original_bill(self) -> str:
        """负数单据主表"""
        return f"t_sim_original_bill{self.suffix}"

    @property
    def original_bill_item(self) -> str:
        """负数单据明细表"""
        return f"t_sim_original_bill_item{self.suffix}"

    @property
    def vatinvoice(self) -> str:
        """蓝票主表"""
        return f"t_sim_vatinvoice{self.suffix}"

    @property
    def vatinvoice_item(self) -> str:
        """蓝票明细表"""
        return f"t_sim_vatinvoice_item{self.suffix}"


# 模块级别的配置单例
_db_config: Optional[DatabaseConfig] = None
_table_config: Optional[TableConfig] = None
_config_loaded: bool = False


def load_config(env: Optional[str] = None) -> None:
    """
    加载配置文件

    优先级:
    1. .env.{ENV} (如果 ENV 环境变量存在)
    2. .env (默认)

    Args:
        env: 可选的环境名称（dev/test/prod），如果不提供则从 ENV 环境变量读取

    Raises:
        ValueError: 配置缺少必需字段或格式错误
        FileNotFoundError: 配置文件不存在
    """
    global _db_config, _table_config, _config_loaded

    # 如果已加载，直接返回
    if _config_loaded:
        return

    # 确定环境
    if env is None:
        env = os.getenv('ENV', None)

    # 加载配置文件
    if env:
        env_file = f".env.{env}"
        if os.path.exists(env_file):
            load_dotenv(env_file, override=True)
            print(f"已加载配置文件: {env_file}")
        else:
            # 尝试加载默认 .env
            if os.path.exists('.env'):
                load_dotenv('.env')
                print(f"警告: {env_file} 不存在，已加载默认 .env 文件")
            else:
                raise FileNotFoundError(
                    f"配置文件 {env_file} 和 .env 都不存在。"
                    f"请创建配置文件或从 .env.example 复制。"
                )
    else:
        # 加载默认 .env
        if os.path.exists('.env'):
            load_dotenv('.env')
            print("已加载配置文件: .env")
        else:
            raise FileNotFoundError(
                "配置文件 .env 不存在。请创建配置文件或从 .env.example 复制。"
            )

    # 读取数据库配置（必需字段）
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD', '')

    # 验证必需字段
    missing_fields = []
    if not db_host:
        missing_fields.append('DB_HOST')
    if not db_name:
        missing_fields.append('DB_NAME')
    if not db_user:
        missing_fields.append('DB_USER')

    if missing_fields:
        raise ValueError(
            f"配置缺少必需字段: {', '.join(missing_fields)}\n"
            f"请在配置文件中设置这些环境变量。"
        )

    # 验证端口号
    try:
        db_port_int = int(db_port)
        if db_port_int <= 0 or db_port_int > 65535:
            raise ValueError(f"DB_PORT 必须在 1-65535 范围内，当前值: {db_port}")
    except ValueError as e:
        raise ValueError(f"DB_PORT 格式错误: {e}")

    # 读取表名后缀（可选字段）
    table_suffix = os.getenv('TABLE_SUFFIX', '')

    # 创建配置对象
    _db_config = DatabaseConfig(
        host=db_host,
        port=db_port_int,
        database=db_name,
        user=db_user,
        password=db_password
    )

    _table_config = TableConfig(suffix=table_suffix)
    _config_loaded = True

    print(f"配置加载成功:")
    print(f"  数据库: {db_name}@{db_host}:{db_port_int}")
    print(f"  表后缀: '{table_suffix}'" if table_suffix else "  表后缀: (无)")


def get_db_config() -> Dict[str, any]:
    """
    获取数据库连接配置（字典格式）

    Returns:
        数据库连接参数字典，可直接传递给 psycopg2.connect(**config)

    Raises:
        RuntimeError: 配置未加载时调用
    """
    if not _config_loaded or _db_config is None:
        raise RuntimeError(
            "配置未加载。请先调用 load_config() 加载配置。"
        )

    return _db_config.to_dict()


def get_tables() -> TableConfig:
    """
    获取表名配置对象

    Returns:
        TableConfig 实例，包含所有表名的属性访问器

    Raises:
        RuntimeError: 配置未加载时调用
    """
    if not _config_loaded or _table_config is None:
        raise RuntimeError(
            "配置未加载。请先调用 load_config() 加载配置。"
        )

    return _table_config


def reset_config() -> None:
    """重置配置（主要用于测试）"""
    global _db_config, _table_config, _config_loaded
    _db_config = None
    _table_config = None
    _config_loaded = False


# 辅助函数：显示当前配置信息（用于调试）
def print_config() -> None:
    """打印当前配置信息（用于调试）"""
    if not _config_loaded:
        print("配置尚未加载")
        return

    print("=" * 60)
    print("当前配置信息")
    print("=" * 60)

    if _db_config:
        print(f"数据库配置:")
        print(f"  Host: {_db_config.host}")
        print(f"  Port: {_db_config.port}")
        print(f"  Database: {_db_config.database}")
        print(f"  User: {_db_config.user}")
        print(f"  Password: {'*' * len(_db_config.password) if _db_config.password else '(空)'}")

    if _table_config:
        print(f"\n表名配置:")
        print(f"  后缀: '{_table_config.suffix}'" if _table_config.suffix else "  后缀: (无)")
        print(f"  负数单据主表: {_table_config.original_bill}")
        print(f"  负数单据明细表: {_table_config.original_bill_item}")
        print(f"  蓝票主表: {_table_config.vatinvoice}")
        print(f"  蓝票明细表: {_table_config.vatinvoice_item}")

    print("=" * 60)


if __name__ == '__main__':
    """配置测试入口"""
    try:
        print("正在测试配置加载...\n")
        load_config()
        print_config()

        print("\n配置测试成功!")

    except Exception as e:
        print(f"\n配置测试失败: {e}", file=sys.stderr)
        sys.exit(1)
