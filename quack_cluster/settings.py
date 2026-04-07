# quack_cluster/settings.py
# ============================================================
# 设置（Settings）- 系统的配置中心
# ============================================================
# 这个文件包含 Quack-Cluster 的所有可配置参数
#
# 为什么需要单独的设置文件？
# 1. 集中管理配置，避免散落在各处
# 2. 支持从 config.yaml 配置文件读取
# 3. 支持环境变量覆盖配置文件（优先级更高）
# 4. 提供默认值，确保系统可正常运行
#
# 配置文件加载顺序（优先级从高到低）：
# 1. 环境变量（如果设置了同名环境变量）
# 2. config.yaml 配置文件
# 3. 代码中的默认值
#
# 主要配置项：
# - DATA_DIR: 数据文件存放目录
# - NUM_SHUFFLE_PARTITIONS: 分区连接的分区数
# - BROADCAST_THRESHOLD_MB: 广播连接的大小阈值
# - MAX_RETRIES: 任务失败重试次数
# - CACHE_TTL_SECONDS: 查询缓存的生存时间
# - DUCKLAKE_*: DuckLake 相关配置

import os
import yaml
from pathlib import Path
from typing import Optional
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict
from datetime import datetime, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# ============================================================
# 配置文件加载
# ============================================================
# 查找 config.yaml 配置文件
# 搜索路径：项目根目录 -> 上级目录 -> 当前目录
def find_config_file() -> Optional[Path]:
    """查找 config.yaml 配置文件的位置"""
    possible_paths = [
        Path(__file__).parent.parent / "config.yaml",      # 项目根目录
        Path(__file__).parent.parent.parent / "config.yaml",  # 上级目录
        Path("config.yaml"),  # 当前目录
    ]

    for path in possible_paths:
        if path.exists():
            return path
    return None


def load_config_from_yaml() -> dict:
    """
    从 config.yaml 加载配置

    Returns:
        包含所有配置项的字典
    """
    config_path = find_config_file()
    if config_path:
        print(f"📄 正在从 {config_path} 加载配置...")
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            print("✅ 配置文件加载成功！")
            return config or {}
    else:
        print("⚠️ 未找到 config.yaml 配置文件，将使用默认值。")
        return {}


# 加载 YAML 配置
_yaml_config = load_config_from_yaml()


# ============================================================
# 配置模型类
# ============================================================
class Settings(BaseSettings):
    """
    Quack-Cluster 配置类

    使用 Pydantic Settings 的好处：
    - 自动验证配置类型
    - 自动转换环境变量（如 DATABASE_URL → database_url）
    - 从 config.yaml 加载配置
    - 提供清晰的默认值

    配置项说明：
    """

    # ============================================================
    # 数据目录配置
    # ============================================================
    # 数据文件存放的目录路径
    # 建议使用绝对路径，避免路径问题
    DATA_DIR: str = _yaml_config.get("data_dir", "/app/sample_data")

    # ============================================================
    # 分区连接配置
    # ============================================================
    # 分区连接（Shuffle Join）将数据分成多少个分区
    #
    # 工作原理：
    # - 假设 NUM_SHUFFLE_PARTITIONS = 4
    # - 数据按 HASH(key) % 4 分成 4 个分区
    # - 相同哈希值的数据在同一个分区
    #
    # 如何选择值：
    # - 更多分区 = 更细粒度 = 更好的并行度
    # - 更多分区 = 更多 Workers = 更多资源消耗
    # - 通常 4-16 个分区比较合适
    NUM_SHUFFLE_PARTITIONS: int = _yaml_config.get("num_shuffle_partitions", 4)

    # ============================================================
    # 广播连接配置
    # ============================================================
    # 小于这个大小的表会使用广播连接
    #
    # 工作原理：
    # - 如果表小于 BROADCAST_THRESHOLD_MB MB
    # - Planner 会选择广播连接而不是分区连接
    #
    # 如何选择值：
    # - 取决于 Worker 的内存大小
    # - 太大会导致 Worker 内存不足
    # - 太小会让本应分区连接的查询使用了广播
    # - 通常 100-500 MB 比较合适
    BROADCAST_THRESHOLD_MB: int = _yaml_config.get("broadcast_threshold_mb", 0)

    # ============================================================
    # 容错配置
    # ============================================================
    # 任务失败时的最大重试次数
    #
    # 工作原理：
    # - 当 Worker 任务失败时，Executor 会自动重试
    # - 最多重试 MAX_RETRIES 次
    # - 如果仍然失败，查询将报错
    #
    # 为什么需要重试？
    # - 分布式系统中，临时故障是常见的
    # - 网络抖动、节点负载高等都可能导致任务失败
    # - 重试可以提高系统的容错能力
    MAX_RETRIES: int = _yaml_config.get("max_retries", 3)

    # ============================================================
    # 缓存配置
    # ============================================================
    # 查询结果缓存的生存时间（秒）
    #
    # 工作原理：
    # - 相同 SQL 的查询结果会被缓存
    # - 缓存会在 TTL 秒后过期
    # - 下次查询需要重新执行
    #
    # 如何选择值：
    # - 数据变化频繁 → 较短的 TTL
    # - 数据相对稳定 → 较长的 TTL
    # - 5 分钟（300 秒）是一个合理的默认值
    CACHE_TTL_SECONDS: int = _yaml_config.get("cache_ttl_seconds", 300)

    # ============================================================
    # DuckLake 配置（可选）
    # ============================================================
    # DuckLake 是一个基于 DuckDB 的数据目录扩展，支持 Iceberg 风格的表管理
    # 如果启用，系统会通过 DuckLake 访问数据，而不是直接读取文件

    # 是否启用 DuckLake
    # - True: 使用 DuckLake 目录管理表和数据
    # - False: 直接读取本地文件（默认行为）
    DUCKLAKE_ENABLED: bool = _yaml_config.get("ducklake_enabled", False)

    # DuckLake 目录名称（在 DuckDB 中 attach 后的别名）
    DUCKLAKE_CATALOG_NAME: str = _yaml_config.get("ducklake_catalog_name", "ducklake_catalog")

    # PostgreSQL 连接字符串（用于 DuckLake 元数据存储）
    # 格式: host:port
    # 示例: postgres:5432 或 localhost:5432
    DUCKLAKE_POSTGRES_CONN: str = _yaml_config.get("ducklake_postgres_conn", "")

    # S3 数据文件路径（存放 Parquet 等数据文件的位置）
    # 格式: s3://bucket-name/path/
    # 示例: s3://my-data-lake/warehouse/
    DUCKLAKE_S3_PATH: str = _yaml_config.get("ducklake_s3_path", "")

    # ============================================================
    # API 配置
    # ============================================================
    API_HOST: str = _yaml_config.get("api_host", "0.0.0.0")
    API_PORT: int = _yaml_config.get("api_port", 8000)

    # ============================================================
    # Ray 集群配置
    # ============================================================
    RAY_DASHBOARD_HOST: str = _yaml_config.get("ray_dashboard_host", "0.0.0.0")

    # ============================================================
    # Pydantic Settings 配置
    # ============================================================
    model_config = SettingsConfigDict(
        env_file='.env',           # 从 .env 文件读取配置
        env_file_encoding='utf-8', # .env 文件编码
        extra='ignore'            # 忽略未知的配置项
    )


# 全局设置实例
# 在代码中通过 settings.XXX 访问配置
settings = Settings()


# ============================================================
# 示例数据生成函数
# ============================================================
def generate_sample_data():
    """
    生成示例数据文件

    这个函数创建测试用的 Parquet、CSV 和 JSON 文件：
    - users.parquet / users_nw.parquet: 用户表
    - orders.parquet / orders_nw.parquet: 订单表
    - products_nw.parquet: 产品表
    - data_part_1.parquet / data_part_2.parquet: 分片数据
    - userscs.csv: CSV 格式用户表
    - ordersjs.json: JSON Lines 格式订单表

    为什么需要生成数据？
    - 用户第一次运行系统时没有数据
    - 这些示例数据可以快速验证系统是否正常工作
    - 也是测试各种查询功能的基础

    使用方式：
    make data  # 在 Docker 中运行
    pdm run generate-data  # 本地运行
    """
    print(f"正在生成示例数据到 '{settings.DATA_DIR}' 目录...")
    os.makedirs(settings.DATA_DIR, exist_ok=True)

    # ============================================================
    # 用户表（Parquet 格式）
    # ============================================================
    users_df = pd.DataFrame({
        'user_id': [1, 2, 3, 4],
        'name': ['Alice', 'Bob', 'Charlie', 'David'],
        'city': ['New York', 'Los Angeles', 'Chicago', 'Houston']
    })
    users_table = pa.Table.from_pandas(users_df)
    pq.write_table(users_table, os.path.join(settings.DATA_DIR, 'users.parquet'))

    # ============================================================
    # 订单表（Parquet 格式）
    # ============================================================
    orders_df = pd.DataFrame({
        'order_id': range(101, 108),
        'user_id': [1, 2, 1, 3, 2, 2, 4],
        'amount': [150, 200, 50, 300, 250, 180, 100]
    })
    orders_table = pa.Table.from_pandas(orders_df)
    pq.write_table(orders_table, os.path.join(settings.DATA_DIR, 'orders.parquet'))

    # ============================================================
    # 分片数据表（用于测试并行扫描）
    # ============================================================
    df1 = pd.DataFrame({'id': [1, 2, 3], 'product': ['A', 'B', 'A'], 'sales': [100, 150, 200]})
    df2 = pd.DataFrame({'id': [4, 5, 6], 'product': ['B', 'C', 'A'], 'sales': [250, 300, 120]})
    pq.write_table(pa.Table.from_pandas(df1), os.path.join(settings.DATA_DIR, 'data_part_1.parquet'))
    pq.write_table(pa.Table.from_pandas(df2), os.path.join(settings.DATA_DIR, 'data_part_2.parquet'))
    print("✅ 示例数据已生成。")

    # ============================================================
    # 用户表（新版本，Northwind 风格）
    # ============================================================
    users_df = pd.DataFrame({
        'user_id': [101, 102, 103, 104, 105, 106],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank'],
        'city': ['New York', 'Los Angeles', 'New York', 'Chicago', 'Los Angeles', 'Chicago'],
        'join_date': pd.to_datetime(['2024-01-15', '2024-02-20', '2024-03-10', '2024-04-05', '2024-05-12', '2024-06-18'])
    })
    pq.write_table(pa.Table.from_pandas(users_df), os.path.join(settings.DATA_DIR, 'users_nw.parquet'))

    # ============================================================
    # 产品表
    # ============================================================
    products_df = pd.DataFrame({
        'product_id': [1, 2, 3, 4, 5],
        'product_name': ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Webcam'],
        'category': ['Electronics', 'Accessories', 'Accessories', 'Electronics', 'Accessories'],
        'unit_price': [1200.00, 25.00, 75.00, 300.00, 50.00]
    })
    pq.write_table(pa.Table.from_pandas(products_df), os.path.join(settings.DATA_DIR, 'products_nw.parquet'))

    # ============================================================
    # 订单表（新版本，带详细信息）
    # ============================================================
    order_data = {
        'order_id': range(1, 16),
        'user_id': [101, 102, 101, 103, 104, 102, 105, 101, 103, 106, 102, 104, 105, 103, 101],
        'product_id': [1, 3, 2, 4, 1, 5, 2, 4, 3, 1, 2, 2, 5, 1, 5],
        'quantity': [1, 2, 1, 1, 1, 3, 2, 1, 1, 1, 1, 1, 2, 1, 4],
        'order_date': [
            datetime(2025, 1, 20), datetime(2025, 1, 22), datetime(2025, 2, 5),
            datetime(2025, 2, 10), datetime(2025, 3, 1), datetime(2025, 3, 12),
            datetime(2025, 3, 15), datetime(2025, 4, 2), datetime(2025, 4, 8),
            datetime(2025, 4, 20), datetime(2025, 5, 5), datetime(2025, 5, 15),
            datetime(2025, 6, 1), datetime(2025, 6, 10), datetime(2025, 6, 25)
        ]
    }
    orders_df = pd.DataFrame(order_data)

    # 与产品表关联计算订单金额
    orders_df = pd.merge(orders_df, products_df[['product_id', 'unit_price']], on='product_id')
    orders_df['amount'] = orders_df['quantity'] * orders_df['unit_price']
    orders_df.drop(columns=['unit_price'], inplace=True)

    pq.write_table(pa.Table.from_pandas(orders_df), os.path.join(settings.DATA_DIR, 'orders_nw.parquet'))

    # ============================================================
    # 用户表（CSV 格式）
    # ============================================================
    users_df = pd.DataFrame({
        'user_id': [101, 102, 103, 104, 105, 106],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank'],
        'city': ['New York', 'Los Angeles', 'New York', 'Chicago', 'Los Angeles', 'Chicago']
    })
    users_df.to_csv(os.path.join(settings.DATA_DIR, 'userscs.csv'), index=False)
    print("✅ 已生成 'users.csv'")

    # ============================================================
    # 订单表（JSON Lines 格式）
    # ============================================================
    order_data = {
        'order_id': range(1, 16),
        'user_id': [101, 102, 101, 103, 104, 102, 105, 101, 103, 106, 102, 104, 105, 103, 101],
        'product_id': [1, 3, 2, 4, 1, 5, 2, 4, 3, 1, 2, 2, 5, 1, 5],
        'quantity': [1, 2, 1, 1, 1, 3, 2, 1, 1, 1, 1, 1, 2, 1, 4],
        'order_date': [
            datetime(2025, 1, 20), datetime(2025, 1, 22), datetime(2025, 2, 5),
            datetime(2025, 2, 10), datetime(2025, 3, 1), datetime(2025, 3, 12),
            datetime(2025, 3, 15), datetime(2025, 4, 2), datetime(2025, 4, 8),
            datetime(2025, 4, 20), datetime(2025, 5, 5), datetime(2025, 5, 15),
            datetime(2025, 6, 1), datetime(2025, 6, 10), datetime(2025, 6, 25)
        ]
    }
    orders_df = pd.DataFrame(order_data)
    # DuckDB 的 read_json_auto 最适合 JSON Lines 格式（每行一个 JSON 对象）
    orders_df.to_json(
        os.path.join(settings.DATA_DIR, 'ordersjs.json'),
        orient='records',
        lines=True,  # JSON Lines 格式
        date_format='iso'  # ISO 日期格式
    )
    print("✅ 已生成 'orders.json'")
    print("\n示例数据生成完成！")
    print("用户和订单示例数据已生成。")
