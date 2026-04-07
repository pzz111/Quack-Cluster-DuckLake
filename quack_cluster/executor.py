# quack_cluster/executor.py
# ============================================================
# Executor（执行器）- 查询引擎的"肌肉"
# ============================================================
# 职责说明：
# 1. 接收 Planner 生成的执行计划
# 2. 与 Ray 集群交互，创建和管理 Worker 任务
# 3. 分发任务给 Workers（Ray Actors），并行处理数据
# 4. 收集 Workers 的部分结果
# 5. 在协调器上进行最终聚合（如 GROUP BY、ORDER BY、LIMIT）
#
# 为什么 Executor 重要？
# - Planner 只负责"做什么"，Executor 负责"怎么做"
# - Executor 需要高效地协调分布式任务
# - 需要处理失败重试、结果聚合等复杂逻辑
#
# 容错机制：
# - 如果 Worker 任务失败，Executor 会自动重试（最多 MAX_RETRIES 次）
# - 这确保了系统在部分节点故障时仍能正常运行
#
# 三种执行策略：
# 1. _execute_local：本地执行，不需要分布式
# 2. _execute_scan：单表扫描，多 Workers 并行读取不同文件
# 3. _execute_join：分区连接，Map-Shuffle-Reduce 三阶段
# 4. _execute_broadcast_join：广播连接，小表广播到所有 Workers

import asyncio
import glob
import os
from collections import defaultdict
import duckdb
import pyarrow as pa
from sqlglot import exp, parse_one
from sqlglot.errors import ParseError
from .execution_plan import (
    BasePlan, LocalExecutionPlan, DistributedScanPlan,
    DistributedShuffleJoinPlan, DistributedBroadcastJoinPlan
)
from .worker import DuckDBWorker
from .settings import settings
import logging
from typing import List, Callable, Any, Coroutine


logger = logging.getLogger(__name__)


class Executor:
    """
    查询执行器 - 负责分布式执行查询计划

    主要方法：
    - execute(): 主入口，根据计划类型调用对应的执行方法
    - _execute_local(): 执行本地计划
    - _execute_scan(): 执行分布式扫描计划
    - _execute_join(): 执行分区连接计划
    - _execute_broadcast_join(): 执行广播连接计划
    - _execute_with_retries(): 带重试的任务执行辅助方法
    """

    @staticmethod
    async def _execute_with_retries(
        initial_inputs: List[Any],
        task_function: Callable[[Any], Coroutine]
    ) -> List[Any]:
        """
        带重试机制的分布式任务执行器

        为什么需要重试？
        - 在分布式系统中，任务可能因为各种原因失败：
          网络抖动、节点暂时不可用、资源竞争等
        - 重试机制可以提高系统的容错性和稳定性

        工作原理：
        1. 尝试执行所有任务
        2. 收集成功和失败的任务
        3. 如果有失败的任务，等待后重试
        4. 重复直到所有任务成功或达到最大重试次数

        Args:
            initial_inputs: 任务输入列表，每个元素会被 task_function 处理
            task_function: 任务创建函数，接收一个输入，返回一个 Ray task（协程）

        Returns:
            所有任务的成功结果列表

        Raises:
            Exception: 如果所有重试都失败后抛出异常
        """
        inputs_to_process = list(initial_inputs)  # 待处理的任务
        all_results = []  # 成功的结果
        retries = 0  # 当前重试次数

        # 只要有待处理的任务且未超过最大重试次数，就继续循环
        while inputs_to_process and retries <= settings.MAX_RETRIES:
            if retries > 0:
                logger.warning(f"🔁 重试 {len(inputs_to_process)} 个失败的任务...（第 {retries} 次重试 / 最多 {settings.MAX_RETRIES} 次）")
                await asyncio.sleep(1)  # 等待 1 秒后再试

            # 为每个待处理的输入创建任务
            tasks = [task_function(inp) for inp in inputs_to_process]

            # 建立任务到输入的映射，用于追踪失败的任务
            task_to_input_map = {tasks[i]: inputs_to_process[i] for i in range(len(tasks))}

            # 并行执行所有任务，return_exceptions=True 会在任务失败时返回异常对象而不是抛出
            task_results = await asyncio.gather(*tasks, return_exceptions=True)

            # 分离成功和失败的任务
            failed_inputs = []
            for i, res in enumerate(task_results):
                if isinstance(res, Exception):
                    # 任务失败，记录并准备重试
                    failed_input = task_to_input_map[tasks[i]]
                    logger.error(f"‼️ 任务失败！输入：'{failed_input}'，错误：{res}")
                    failed_inputs.append(failed_input)
                else:
                    # 任务成功，收集结果
                    all_results.append(res)

            # 更新待处理任务列表为失败的任务
            inputs_to_process = failed_inputs
            if inputs_to_process:
                retries += 1

        # 如果还有失败的任务，说明超过了最大重试次数
        if inputs_to_process:
            error_message = f"🛑 查询在 {settings.MAX_RETRIES} 次重试后仍然失败。无法处理的输入：{inputs_to_process}"
            logger.critical(error_message)
            raise Exception(error_message)

        return all_results

    @staticmethod
    async def execute(plan: BasePlan, con: duckdb.DuckDBPyConnection) -> pa.Table:
        """
        执行计划的主入口

        根据计划的类型，选择不同的执行策略

        Args:
            plan: 执行计划对象
            con: DuckDB 连接（用于协调器上的最终聚合）

        Returns:
            查询结果的 PyArrow 表
        """
        if isinstance(plan, LocalExecutionPlan):
            return await Executor._execute_local(plan, con)
        elif isinstance(plan, DistributedScanPlan):
            return await Executor._execute_scan(plan, con)
        elif isinstance(plan, DistributedShuffleJoinPlan):
            return await Executor._execute_join(plan, con)
        elif isinstance(plan, DistributedBroadcastJoinPlan):
            return await Executor._execute_broadcast_join(plan, con)
        else:
            raise NotImplementedError(f"不支持的执行计划类型：'{plan.plan_type}'。")

    @staticmethod
    async def _execute_local(plan: LocalExecutionPlan, con: duckdb.DuckDBPyConnection) -> pa.Table:
        """
        执行本地计划（不需要分布式）

        适用场景：
        - 查询不涉及任何数据文件（如 SELECT 1 + 1）
        - 所有数据都已在协调器的内存表中

        工作流程：
        直接在协调器的 DuckDB 连接上执行 SQL

        Args:
            plan: 本地执行计划
            con: DuckDB 连接

        Returns:
            查询结果的 PyArrow 表
        """
        logger.info(f"执行本地计划：{plan.query_ast.sql()}")
        return con.execute(plan.query_ast.sql(dialect="duckdb")).fetch_arrow_table()

    @staticmethod
    async def _execute_scan(plan: DistributedScanPlan, final_con: duckdb.DuckDBPyConnection) -> pa.Table:
        """
        执行分布式扫描计划（单表查询）

        适用场景：
        - 查询只涉及一个表
        - 没有 JOIN

        工作流程（三阶段）：
        1. 查找表的所有数据文件
        2. 为每个文件创建一个 Worker 任务，并行扫描
        3. 收集结果，在协调器上进行最终聚合

        为什么这样做？
        - 每个文件可以独立扫描，互不依赖
        - 并行扫描多个文件可以大大提高吞吐量

        Args:
            plan: 分布式扫描计划
            final_con: DuckDB 连接

        Returns:
            查询结果的 PyArrow 表
        """
        logger.info(f"执行分布式扫描，表：'{plan.table_name}'")

        # 第一步：查找表的所有数据文件
        file_pattern = os.path.join(settings.DATA_DIR, f"{plan.table_name}.*")
        all_files = glob.glob(file_pattern)
        if not all_files:
            raise FileNotFoundError(f"找不到表 '{plan.table_name}'。搜索模式：'{file_pattern}'")

        # 第二步：定义扫描任务创建函数
        def create_scan_task(file_path: str):
            """为单个文件创建扫描任务"""
            actor = DuckDBWorker.remote()  # 创建 Worker Actor
            return actor.query.remote(plan.worker_query_template, [file_path])

        # 第三步：使用容错机制并行扫描所有文件
        partial_results = await Executor._execute_with_retries(all_files, create_scan_task)

        # 过滤掉空结果
        valid_results = [r for r in partial_results if r is not None and r.num_rows > 0]
        if not valid_results:
            return pa.Table.from_pydict({})

        # 第四步：合并所有部分结果
        combined_arrow_table = pa.concat_tables(valid_results)
        final_con.register("combined_arrow_table", combined_arrow_table)

        # 第五步：在协调器上进行最终聚合
        # 移除 WHERE 子句（已在 Workers 上应用），将表替换为合并结果
        final_query_ast = plan.query_ast.copy()
        if final_query_ast.find(exp.Where):
            final_query_ast.find(exp.Where).pop()
        final_query_ast.find(exp.Table).replace(parse_one("combined_arrow_table"))
        final_query = final_query_ast.sql(dialect="duckdb")

        logger.info(f"协调器执行最终扫描聚合：{final_query}")
        return final_con.execute(final_query).fetch_arrow_table()

    @staticmethod
    async def _execute_join(plan: DistributedShuffleJoinPlan, final_con: duckdb.DuckDBPyConnection) -> pa.Table:
        """
        执行分区连接计划（两表 JOIN）

        这是最复杂的执行策略，用于两个大表的连接

        工作流程（三阶段 Map-Shuffle-Reduce）：

        阶段 1 - Map（分区）：
        - 读取左表和右表的数据
        - 按连接键的哈希值对数据进行分区
        - 相同键的数据进入同一个分区

        阶段 2 - Shuffle & Reduce（合并）：
        - 将分区后的数据发送到对应的 Workers
        - 每个 Worker 收到一个分区的左右数据
        - Worker 在本地执行 JOIN

        阶段 3 - Final Aggregation（最终聚合）：
        - 收集所有 Workers 的部分结果
        - 在协调器上执行 GROUP BY、ORDER BY、LIMIT 等

        为什么需要分区？
        - 如果直接把两表数据发送到同一个 Worker，可能导致内存溢出
        - 分区后，每个 Worker 只需要处理自己分区的数据

        Args:
            plan: 分区连接计划
            final_con: DuckDB 连接

        Returns:
            查询结果的 PyArrow 表
        """
        logger.info(f"执行分区连接：'{plan.left_table_name}' JOIN '{plan.right_table_name}'")

        # ============================================================
        # 阶段 1：Map - 按键分区
        # ============================================================
        left_files = glob.glob(os.path.join(settings.DATA_DIR, f"{plan.left_table_name}.*"))
        right_files = glob.glob(os.path.join(settings.DATA_DIR, f"{plan.right_table_name}.*"))
        if not left_files or not right_files:
            raise FileNotFoundError("缺少 JOIN 表的数据文件。")

        # 定义分区任务创建函数
        def create_partition_task(file_info: tuple):
            """为单个文件创建分区任务"""
            file_path, join_key, reader_func, where_sql = file_info
            actor = DuckDBWorker.remote()
            # 调用 Worker 的 partition_by_key 方法，按键分区
            return actor.partition_by_key.remote(
                file_path, join_key, settings.NUM_SHUFFLE_PARTITIONS, reader_func, where_sql
            )

        # 准备分区任务输入（包含 WHERE 条件）
        left_inputs = [(f, plan.left_join_key, plan.left_reader_function, plan.left_where_sql) for f in left_files]
        right_inputs = [(f, plan.right_join_key, plan.right_reader_function, plan.right_where_sql) for f in right_files]

        if plan.left_where_sql:
            logger.info(f"下推过滤器到左表：{plan.left_where_sql}")
        if plan.right_where_sql:
            logger.info(f"下推过滤器到右表：{plan.right_where_sql}")

        # 并行执行左右两边的分区任务
        left_partitions_list, right_partitions_list = await asyncio.gather(
            Executor._execute_with_retries(left_inputs, create_partition_task),
            Executor._execute_with_retries(right_inputs, create_partition_task)
        )

        # 整理分区结果，按分区 ID 分组
        # partitions_for_joiners[partition_id]['left'] = 左表该分区的数据
        # partitions_for_joiners[partition_id]['right'] = 右表该分区的数据
        partitions_for_joiners = defaultdict(lambda: defaultdict(list))
        for partitioned_dict in left_partitions_list:
            for p_id, table in partitioned_dict.items():
                partitions_for_joiners[p_id]['left'].append(table)
        for partitioned_dict in right_partitions_list:
            for p_id, table in partitioned_dict.items():
                partitions_for_joiners[p_id]['right'].append(table)
        logger.info("✅ 分区阶段完成。")

        # ============================================================
        # 阶段 2：Reduce - 执行 JOIN
        # ============================================================
        # 为每个分区 ID 创建 JOIN 任务
        join_inputs = [
            (i, partitions_for_joiners[i]['left'], partitions_for_joiners[i]['right'])
            for i in range(settings.NUM_SHUFFLE_PARTITIONS) if i in partitions_for_joiners
        ]

        def create_join_task(join_input: tuple):
            """为分区对创建 JOIN 任务"""
            partition_id, left_parts, right_parts = join_input
            actor = DuckDBWorker.remote()
            return actor.join_partitions.remote(
                left_parts, right_parts, plan.worker_join_sql,
                plan.left_table_alias, plan.right_table_alias
            )

        # 执行所有 JOIN 任务
        partial_results_list = await Executor._execute_with_retries(join_inputs, create_join_task)

        # 过滤并合并结果
        valid_partials = [tbl for tbl in partial_results_list if tbl.num_rows > 0]
        if not valid_partials:
            return pa.Table.from_pydict({})

        partial_results_table = pa.concat_tables(valid_partials)
        final_con.register("partial_results", partial_results_table)

        # ============================================================
        # 阶段 3：最终聚合
        # ============================================================
        # 在协调器上执行 GROUP BY、窗口函数、ORDER BY、LIMIT
        final_agg_query = "SELECT * FROM partial_results"
        parsed_query = plan.query_ast

        # 处理聚合查询
        if parsed_query.find(exp.AggFunc):
            final_selects = []
            final_group_bys = []

            # 提取 GROUP BY 列
            if group_by_node := parsed_query.find(exp.Group):
                for expr in group_by_node.expressions:
                    final_group_bys.append(expr.alias_or_name)

            # 找出窗口函数别名
            window_function_aliases = set()
            if plan.final_select_sql:
                final_select_ast = parse_one(plan.final_select_sql, read="duckdb")
                for expr in final_select_ast.expressions:
                    if expr.find(exp.Window):
                        window_function_aliases.add(expr.alias_or_name)

            # 构建最终 SELECT
            final_selects.extend(final_group_bys)
            for expr in parsed_query.find(exp.Select).expressions:
                alias = expr.alias_or_name
                # 跳过已在 GROUP BY 中或为窗口函数的列
                if alias in final_group_bys or alias in window_function_aliases:
                    continue
                # 聚合函数
                if expr.find(exp.AggFunc):
                    final_selects.append(f"SUM({alias}) AS {alias}")

            final_agg_query = f"SELECT {', '.join(final_selects)} FROM partial_results"
            if final_group_bys:
                final_agg_query += f" GROUP BY {', '.join(final_group_bys)}"

        logger.info(f"协调器执行最终聚合：{final_agg_query}")
        aggregated_results_table = final_con.execute(final_agg_query).fetch_arrow_table()

        # ============================================================
        # 阶段 4：窗口函数
        # ============================================================
        final_table = aggregated_results_table
        if plan.final_select_sql:
            logger.info("协调器应用窗口函数...")
            final_con.register("aggregated_results", aggregated_results_table)

            # 直接使用 Planner 生成的窗口函数 SQL
            window_query = plan.final_select_sql

            logger.info(f"执行窗口函数查询：{window_query}")
            final_table = final_con.execute(window_query).fetch_arrow_table()

        # ============================================================
        # 阶段 5：排序和限制
        # ============================================================
        final_con.register("final_results_before_sort", final_table)
        final_ordered_query = "SELECT * FROM final_results_before_sort"

        # 处理 ORDER BY
        if order_by := parsed_query.find(exp.Order):
            rewritten_order = order_by.copy()
            for col in rewritten_order.find_all(exp.Column):
                col.set('table', None)  # 移除表前缀
            final_ordered_query += f" {rewritten_order.sql()}"

        # 处理 LIMIT
        if limit := parsed_query.find(exp.Limit):
            final_ordered_query += f" {limit.sql()}"

        logger.info(f"协调器应用排序和限制：{final_ordered_query}")
        return final_con.execute(final_ordered_query).fetch_arrow_table()

    @staticmethod
    async def _execute_broadcast_join(plan: DistributedBroadcastJoinPlan, final_con: duckdb.DuckDBPyConnection) -> pa.Table:
        """
        执行广播连接计划（小表 JOIN 大表）

        适用场景：
        - 一个表很小（小于 BROADCAST_THRESHOLD_MB）
        - 另一个表很大

        工作流程：
        1. 协调器读取整个小表到内存
        2. 将小表广播到所有 Workers
        3. 每个 Worker 读取大表的一个分区，与小表进行连接
        4. 收集结果，在协调器上进行最终聚合

        为什么这样做？
        - 无需数据 shuffle（分区），减少了网络传输
        - 小表复制到所有 Worker，比 shuffle 大表更高效

        Args:
            plan: 广播连接计划
            final_con: DuckDB 连接

        Returns:
            查询结果的 PyArrow 表
        """
        logger.info(f"执行广播连接：小表='{plan.small_table_name}'，大表='{plan.large_table_name}'")

        # ============================================================
        # 步骤 1：协调器读取小表到内存
        # ============================================================
        small_table_files = glob.glob(os.path.join(settings.DATA_DIR, f"{plan.small_table_name}.*"))
        if not small_table_files:
            raise FileNotFoundError(f"找不到小表 '{plan.small_table_name}' 的数据文件。")

        # 使用临时连接读取小表
        temp_con = duckdb.connect(database=':memory:')
        file_list_sql = ", ".join([f"'{f}'" for f in small_table_files])
        broadcast_table_arrow = temp_con.execute(
            f"SELECT * FROM {plan.small_table_reader_func}([{file_list_sql}])"
        ).fetch_arrow_table()
        temp_con.close()
        logger.info(f"✅ 已准备好广播的小表 '{plan.small_table_name}'（{broadcast_table_arrow.num_rows} 行）。")

        # ============================================================
        # 步骤 2：查找大表文件并创建 Worker 任务
        # ============================================================
        large_table_files = glob.glob(os.path.join(settings.DATA_DIR, f"{plan.large_table_name}.*"))
        if not large_table_files:
            raise FileNotFoundError(f"找不到大表 '{plan.large_table_name}' 的数据文件。")

        def create_broadcast_task(file_path: str):
            """
            为大表的每个文件创建广播连接任务

            Worker 将：
            1. 接收广播的小表
            2. 读取大表的一个文件
            3. 在本地执行 JOIN
            """
            actor = DuckDBWorker.remote()
            # 填充 Worker SQL 中的文件占位符
            task_sql = plan.worker_join_sql.format(file_path=file_path)
            return actor.run_join_task.remote(
                task_sql,
                broadcast_table_arrow,  # 广播的小表
                plan.small_table_alias
            )

        # ============================================================
        # 步骤 3：并行执行所有 JOIN 任务
        # ============================================================
        partial_results = await Executor._execute_with_retries(large_table_files, create_broadcast_task)

        # ============================================================
        # 步骤 4：收集结果并进行最终聚合
        # ============================================================
        valid_results = [r for r in partial_results if r is not None and r.num_rows > 0]
        if not valid_results:
            return pa.Table.from_pydict({})

        # 合并所有部分结果
        combined_arrow_table = pa.concat_tables(valid_results)
        final_con.register("combined_arrow_table", combined_arrow_table)

        # 在协调器上进行最终处理
        final_query_ast = plan.query_ast.copy()

        # 移除 JOIN 子句（已在 Workers 上执行）
        if join_node := final_query_ast.find(exp.Join):
            join_node.pop()

        # 将 FROM 表替换为合并结果
        if from_table := final_query_ast.find(exp.Table):
            from_table.replace(exp.to_table("combined_arrow_table"))
        else:
            raise ValueError("在最终查询中找不到要替换的表。")

        # 移除 WHERE 子句（已在 Workers 上应用）
        if where_node := final_query_ast.find(exp.Where):
            where_node.pop()

        # 移除所有列的表前缀
        for col in final_query_ast.find_all(exp.Column):
            col.set('table', None)

        final_query = final_query_ast.sql(dialect="duckdb")
        logger.info(f"协调器执行最终聚合/排序：{final_query}")

        return final_con.execute(final_query).fetch_arrow_table()
