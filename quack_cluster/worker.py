# quack_cluster/worker.py
# ============================================================
# DuckDBWorker（工作器）- 分布式计算的执行者
# ============================================================
# 什么是 Ray Actor？
# - Ray Actor 是 Ray 框架中的一个概念
# - 与普通函数不同，Actor 有状态，可以保持内部状态
# - 每个 Worker 是一个独立的进程，有自己的 DuckDB 连接
#
# 职责说明：
# 1. 作为 Ray Actor 运行在分布式节点上
# 2. 执行具体的数据查询和计算任务
# 3. 每个 Worker 有自己的 DuckDB 实例，用于处理本地数据
#
# 为什么每个 Worker 有自己的 DuckDB？
# - DuckDB 是嵌入式数据库，没有服务器进程
# - 每个 Worker 独立运行，数据不会相互干扰
# - 适合"分享不变数据"的计算模式
#
# Worker 的工作方式：
# - 接收 Executor 分发的任务
# - 读取指定的数据文件
# - 执行 SQL 查询
# - 返回结果给 Executor
#
# 支持的任务类型：
# 1. query() - 扫描查询（过滤、简单聚合）
# 2. partition_by_key() - 按键分区（用于分区 JOIN）
# 3. join_partitions() - 连接分区（执行 JOIN）
# 4. run_join_task() - 广播连接任务

import ray
import duckdb
import pyarrow as pa
from typing import List, Dict
import traceback
import os
import logging as logger
import time
from .settings import settings


@ray.remote
class DuckDBWorker:
    """
    DuckDB 工作器 - 在 Ray 集群中执行 SQL 任务

    每个 Worker 是一个独立的 Ray Actor：
    - 拥有自己的 DuckDB 内存数据库连接
    - 可以执行多种类型的任务
    - 任务失败时可以触发重试

    重要概念：
    - 每个 Worker 独立运行，不知道其他 Workers 的存在
    - Workers 之间通过 Executor 协调，不直接通信
    - 数据通过 Ray 的对象存储进行传输
    """

    def __init__(self):
        """
        初始化 Worker

        创建 Worker 时：
        1. 建立 DuckDB 内存数据库连接
        2. 内存数据库 ':memory:' 表示所有数据存储在 RAM 中
        3. read_only=False 允许写入临时表等
        4. 如果配置了 DuckLake，初始化 DuckLake 扩展和目录
        """
        self.con = duckdb.connect(database=':memory:', read_only=False)
        print("✅ DuckDBWorker Actor 已初始化。")

        # 如果启用了 DuckLake，初始化 DuckLake 扩展和目录
        if settings.DUCKLAKE_ENABLED:
            self._initialize_ducklake()

    def _initialize_ducklake(self):
        """
        初始化 DuckLake 扩展和目录

        DuckLake 是 DuckDB 的一个核心扩展，提供 Iceberg 风格的数据目录功能：
        - 集中管理表元数据
        - 支持时间旅行查询
        - 支持 S3 等对象存储

        初始化步骤：
        1. 安装并加载 DuckLake 核心扩展
        2. 安装并加载 PostgreSQL 扩展（用于元数据存储）
        3. 安装并加载 S3 扩展（用于数据文件访问）
        4. 加载 AWS 凭证
        5. 挂载 DuckLake 目录数据库
        6. 切换默认数据库上下文到 DuckLake
        """
        try:
            print("🔧 正在初始化 DuckLake...")

            # 1. 安装并加载 DuckLake 核心扩展
            print("   📦 安装 DuckLake 扩展...")
            self.con.execute("INSTALL ducklake;")
            self.con.execute("LOAD ducklake;")
            print("   ✅ DuckLake 核心扩展加载成功！")

            # 2. 安装并加载 PostgreSQL 扩展（用于连接元数据目录）
            print("   📦 安装 PostgreSQL 扩展...")
            self.con.execute("INSTALL postgres;")
            self.con.execute("LOAD postgres;")
            print("   ✅ PostgreSQL 扩展加载成功！")

            # 3. 安装并加载 S3 相关扩展
            print("   📦 安装 S3 扩展...")
            self.con.execute("INSTALL httpfs;")
            self.con.execute("LOAD httpfs;")
            self.con.execute("INSTALL aws;")
            self.con.execute("LOAD aws;")
            print("   ✅ S3 扩展加载成功！")

            # 4. 加载 AWS 凭证（从环境变量或配置文件）
            print("   🔐 加载 AWS 凭证...")
            self.con.execute("CALL load_aws_credentials();")
            print("   ✅ AWS 凭证加载成功！")

            # 5. 挂载 DuckLake 目录数据库
            # 格式: ducklake:postgres:dbname=xxx host=xxx user=xxx password=xxx
            print(f"   🗄️  挂载 DuckLake 目录: {settings.DUCKLAKE_CATALOG_NAME}")
            attach_query = f"""
            ATTACH 'ducklake:postgres:dbname=ducklake_catalog host={settings.DUCKLAKE_POSTGRES_CONN}'
            AS {settings.DUCKLAKE_CATALOG_NAME}
            (DATA_PATH '{settings.DUCKLAKE_S3_PATH}');
            """
            self.con.execute(attach_query)
            print(f"   ✅ DuckLake 目录 '{settings.DUCKLAKE_CATALOG_NAME}' 挂载成功！")

            # 6. 切换默认数据库上下文到 DuckLake
            self.con.execute(f"USE {settings.DUCKLAKE_CATALOG_NAME};")
            print(f"   ✅ 已切换到 DuckLake 目录: {settings.DUCKLAKE_CATALOG_NAME}")

            print("✅ DuckLake 初始化完成！")

        except Exception as e:
            # 如果 DuckLake 初始化失败，记录警告但不影响 Worker 基本功能
            print(f"⚠️  DuckLake 初始化失败：{e}")
            print("   Worker 将使用标准文件模式继续运行。")
            logger.warning(f"DuckLake 初始化失败: {e}")

    def run_join_task(self, join_sql: str, broadcast_table: pa.Table, broadcast_table_name: str) -> pa.Table:
        """
        执行广播连接任务

        适用场景：
        - 小表已经整个加载到内存
        - 大表需要分区读取，每个分区与内存中的小表连接

        工作流程：
        1. 将广播的小表注册为 DuckDB 临时表
        2. 执行 JOIN SQL
        3. 返回结果

        Args:
            join_sql: 要执行的 JOIN SQL（包含占位符）
            broadcast_table: 广播的小表数据（PyArrow 格式）
            broadcast_table_name: 广播表的别名

        Returns:
            JOIN 结果的 PyArrow 表
        """
        try:
            print(f"✅ Worker 收到广播表 '{broadcast_table_name}'，共 {broadcast_table.num_rows} 行。")
            # 将 PyArrow 表注册为 DuckDB 临时表
            # 注册后可以在 SQL 中像普通表一样使用
            self.con.register(broadcast_table_name, broadcast_table)
            print(f"✅ Worker 已注册表 '{broadcast_table_name}'。")

            print(f"✅ Worker 执行 JOIN：{join_sql}")
            result = self.con.execute(join_sql).fetch_arrow_table()
            print(f"✅ Worker JOIN 产生 {result.num_rows} 行结果。")
            return result
        except Exception as e:
            print("🔥🔥🔥 JOIN WORKER 失败！🔥🔥🔥")
            print(f"   - JOIN SQL: {join_sql}")
            print(f"   - 广播表名称: {broadcast_table_name}")
            print(f"   - 异常: {traceback.format_exc()}")
            return pa.Table.from_pydict({})

    def query(self, sql_query: str, file_paths: List[str]) -> pa.Table:
        """
        执行数据扫描查询

        适用场景：
        - 读取一个或多个数据文件
        - 执行过滤、投影等操作
        - 简单的聚合查询

        工作流程：
        1. 格式化 SQL，替换文件路径占位符
        2. 执行查询
        3. 返回结果

        为什么使用模板？
        - SQL 中的 {files} 占位符会被实际的文件列表替换
        - 文件列表格式：[file1.parquet, file2.parquet]
        - DuckDB 可以直接读取多个文件作为一张表

        Args:
            sql_query: SQL 查询模板（如 "SELECT * FROM read_parquet({files}) WHERE ..."）
            file_paths: 要读取的文件路径列表

        Returns:
            查询结果的 PyArrow 表
        """
        query_to_execute = ""
        try:
            # 安全地格式化文件路径
            # 将 ['a.parquet', 'b.parquet'] 转换为 ['a.parquet', 'b.parquet']（在 SQL 中表示）
            formatted_paths = ", ".join([f"'{path}'" for path in file_paths])
            query_to_execute = sql_query.format(files=f"[{formatted_paths}]")

            # 执行查询并返回 Arrow 格式结果
            result = self.con.execute(query_to_execute).fetch_arrow_table()
            return result
        except Exception as e:
            print("🔥🔥🔥 QUERY WORKER 失败！🔥🔥🔥")
            print(f"   - 查询模板: {sql_query}")
            print(f"   - 格式化后: {query_to_execute}")
            print(f"   - 异常: {e}")
            return pa.Table.from_pydict({})

    def partition_by_key(
        self,
        file_path: str,
        key_column: str,
        num_partitions: int,
        reader_function: str,
        where_sql: str = ""
    ) -> Dict[int, pa.Table]:
        """
        按连接键对数据进行分区（Map 阶段）

        适用场景：
        - 分区连接（Shuffle Join）的 Map 阶段
        - 将数据按连接键的哈希值分成 N 个分区

        工作原理：
        1. 读取数据文件
        2. 添加一个分区 ID 列：HASH(key_column) % num_partitions
        3. 按分区 ID 分离数据
        4. 返回每个分区的数据

        为什么需要分区？
        - 确保相同键值的数据在同一个分区
        - 这样 JOIN 时，相同键的数据会在同一个 Worker 处理

        Args:
            file_path: 要读取的文件路径
            key_column: 用于分区的键列名
            num_partitions: 分区总数
            reader_function: DuckDB 读取函数（read_parquet 等）
            where_sql: 可选的 WHERE 条件（用于条件下推）

        Returns:
            字典，键为分区 ID（0 到 num_partitions-1），值为该分区的数据表
        """
        logger.info(f"👷 Worker 正在对 '{file_path}' 进行分区，使用键：'{key_column}'...")

        if where_sql:
            logger.info(f"   - 应用过滤器：{where_sql}")

        # 构建分区查询
        # 1. 创建一个临时视图，包含分区 ID 列
        # 2. HASH(key) % num_partitions 决定每行属于哪个分区
        base_query = f"""
        CREATE OR REPLACE TEMP VIEW partitioned_view AS
        SELECT *, HASH({key_column}) % {num_partitions} AS __partition_id
        FROM {reader_function}('{file_path}')
        {where_sql}
        """
        self.con.execute(base_query)

        # 按分区 ID 分离数据
        partitions = {}
        for i in range(num_partitions):
            # 选择属于当前分区的所有行
            # EXCLUDE(__partition_id) 移除分区 ID 列（不需要在结果中）
            result = self.con.execute(
                f"SELECT * EXCLUDE (__partition_id) FROM partitioned_view WHERE __partition_id = {i}"
            ).fetch_arrow_table()

            # 只保留有数据的分区
            if result.num_rows > 0:
                partitions[i] = result

        return partitions

    def join_partitions(
        self,
        left_partitions: List[pa.Table],
        right_partitions: List[pa.Table],
        join_sql: str,
        left_alias: str,
        right_alias: str
    ) -> pa.Table:
        """
        在分区的数据上执行 JOIN（Reduce 阶段）

        适用场景：
        - 分区连接（Shuffle Join）的 Reduce 阶段
        - 接收两边相同分区的数据，执行本地 JOIN

        工作流程：
        1. 将左表的所有分区合并为一个表
        2. 将右表的所有分区合并为一个表
        3. 在合并后的表上执行 JOIN
        4. 返回 JOIN 结果

        为什么合并后再 JOIN？
        - Shuffle 保证相同键的数据在同一分区
        - 所以每个分区可以独立进行 JOIN

        Args:
            left_partitions: 左表该分区的数据列表
            right_partitions: 右表该分区的数据列表
            join_sql: 要执行的 JOIN SQL
            left_alias: 左表的别名
            right_alias: 右表的别名

        Returns:
            JOIN 结果的 PyArrow 表
        """
        print(f"🤝 Worker 正在连接分区...")

        # 合并分区数据
        left_table = pa.concat_tables(left_partitions) if left_partitions else pa.Table.from_pydict({})
        right_table = pa.concat_tables(right_partitions) if right_partitions else pa.Table.from_pydict({})

        print(f"   - 左侧有 {left_table.num_rows} 行。")
        print(f"   - 右侧有 {right_table.num_rows} 行。")

        # 处理空表情况，避免 JOIN 出错
        if left_table.num_rows == 0 or right_table.num_rows == 0:
            return pa.Table.from_pydict({})

        # 注册临时表
        self.con.register(f"{left_alias}_local", left_table)
        self.con.register(f"{right_alias}_local", right_table)

        # 执行 JOIN
        return self.con.execute(join_sql).fetch_arrow_table()
