# quack_cluster/planner.py
# ============================================================
# Planner（规划器）- 查询引擎的"大脑"
# ============================================================
# 职责说明：
# 1. 接收 Coordinator 传来的 SQL 抽象语法树（AST）
# 2. 分析 AST，理解查询要做什么
# 3. 决定最佳的执行策略（本地执行、分布式扫描、广播连接、分区连接）
# 4. 生成执行计划（ExecutionPlan），告诉 Executor 应该如何执行
#
# 为什么 Planner 很重要？
# - 同一个查询可以用不同的方式执行
# - Planner 需要根据数据大小、查询复杂度等因素选择最优策略
# - 例如：小表 JOIN 大表应该用广播连接，大表 JOIN 大表应该用分区连接
#
# 执行策略选择逻辑：
# 1. 如果查询不涉及任何数据文件（如 SELECT 1+1）→ LocalExecutionPlan
# 2. 如果查询只有一个表且没有 JOIN → DistributedScanPlan
# 3. 如果查询有 JOIN：
#    - 小表 + 大表（小于阈值）→ DistributedBroadcastJoinPlan（广播连接）
#    - 大表 + 大表 → DistributedShuffleJoinPlan（分区连接）

import os
import glob
from sqlglot import exp, parse_one
from sqlglot.errors import ParseError
from .execution_plan import (
    BasePlan, LocalExecutionPlan, DistributedScanPlan,
    DistributedShuffleJoinPlan, DistributedBroadcastJoinPlan
)
from .settings import settings
from typing import Tuple, List, Optional


class Planner:
    """
    查询规划器 - 分析 AST 并选择最优执行策略

    主要方法：
    - create_plan(): 主入口，根据 AST 返回执行计划
    - _discover_table_files_and_reader(): 查找数据文件并确定读取方式
    - _get_table_size_mb(): 计算表的大小（用于决定广播连接）
    """

    @staticmethod
    def _discover_table_files_and_reader(table_name: str) -> Tuple[str, List[str]]:
        """
        发现表对应的数据文件，并确定使用哪个 DuckDB 函数来读取

        查找顺序（按优先级）：
        1. .parquet 文件（列式存储，查询最快）
        2. .csv 文件（需要 DuckDB 的 read_csv_auto 自动检测格式）
        3. .json 文件（使用 read_json_auto）

        支持两种文件命名方式：
        - 单文件：table_name.extension（如 users.parquet）
        - 分片文件：table_name.*.extension（如 data_part_1.parquet, data_part_2.parquet）

        Args:
            table_name: 表名（不含扩展名）

        Returns:
            Tuple[读取函数名, 文件路径列表]
            - 读取函数名：'read_parquet'、'read_csv_auto' 或 'read_json_auto'
            - 文件路径列表：匹配的所有文件

        Raises:
            FileNotFoundError: 如果找不到任何支持格式的数据文件
        """
        # 按优先级尝试不同的文件格式
        for ext, reader_func in [
            ("parquet", "read_parquet"),      # Parquet 优先（列式存储，查询最快）
            ("csv", "read_csv_auto"),         # CSV 次之（自动检测格式）
            ("json", "read_json_auto")        # JSON 最后
        ]:
            # 首先尝试分片文件模式：table_name.*.extension
            # 例如：data_part_*.parquet 会匹配 data_part_1.parquet, data_part_2.parquet
            file_pattern = os.path.join(settings.DATA_DIR, f"{table_name}.*.{ext}")
            files = glob.glob(file_pattern)

            # 如果没找到分片文件，尝试单文件模式
            if not files:
                file_pattern_single = os.path.join(settings.DATA_DIR, f"{table_name}.{ext}")
                files = glob.glob(file_pattern_single)

            # 找到文件，立即返回
            if files:
                return reader_func, files

        # 所有格式都没找到，抛出明确的错误
        raise FileNotFoundError(
            f"找不到表 '{table_name}' 的数据文件！"
            f"支持的格式：.parquet、.csv、.json。"
            f"请检查文件是否存在于 {settings.DATA_DIR} 目录中。"
        )

    @staticmethod
    def _get_table_size_mb(files: List[str]) -> float:
        """
        计算表的总大小（以 MB 为单位）

        用于决定使用哪种连接策略：
        - 小表（小于 BROADCAST_THRESHOLD_MB）可以使用广播连接
        - 大表需要使用分区连接

        Args:
            files: 文件路径列表

        Returns:
            表的总大小（MB）
        """
        total_bytes = sum(os.path.getsize(f) for f in files)
        return total_bytes / (1024 * 1024)

    @staticmethod
    def create_plan(query_ast: exp.Expression, registered_tables: set) -> BasePlan:
        """
        创建执行计划的主入口函数

        这个函数分析 SQL 抽象语法树，并决定使用哪种执行策略。

        分析步骤：
        1. 找出查询中使用的所有表
        2. 如果所有表都已在内存中注册（CTE/子查询结果）→ 本地执行
        3. 如果有 JOIN：
           a. 检查是否可以优化为广播连接
           b. 否则使用分区连接
        4. 如果只是单表查询 → 分布式扫描

        Args:
            query_ast: SQL 语句的抽象语法树
            registered_tables: 已注册在协调器内存中的表集合

        Returns:
            执行计划对象（LocalExecutionPlan、DistributedScanPlan、
            DistributedShuffleJoinPlan 或 DistributedBroadcastJoinPlan）
        """
        # 找出查询中引用的所有表
        all_tables_in_query = [t.this.name for t in query_ast.find_all(exp.Table)]

        # ============================================================
        # 情况 1：本地执行（不需要读取文件）
        # ============================================================
        # 条件：查询不涉及任何表 OR 所有涉及的表都已在内存中注册
        # 示例：SELECT 1 + 1、SELECT * FROM previously_registered_cte
        if not all_tables_in_query or all(t in registered_tables for t in all_tables_in_query):
            return LocalExecutionPlan(
                query_ast=query_ast
            )

        # ============================================================
        # 情况 2：处理 JOIN 查询
        # ============================================================
        if join_expression := query_ast.find(exp.Join):
            # 检查是否是 CROSS JOIN（笛卡尔积），暂不支持
            if isinstance(join_expression, exp.Join) and join_expression.args.get('kind') == 'CROSS':
                raise NotImplementedError("分布式 CROSS JOIN 尚未完全实现。")

            # 提取左右表的表达式
            left_table_expr = query_ast.find(exp.Table)          # FROM 子句中的表
            right_table_expr = join_expression.this              # JOIN 子句中的表

            left_table_name = left_table_expr.this.name
            left_table_alias = left_table_expr.alias_or_name
            right_table_name = right_table_expr.this.name
            right_table_alias = right_table_expr.alias_or_name

            # 查找两个表的数据文件
            left_reader_func, left_files = Planner._discover_table_files_and_reader(left_table_name)
            right_reader_func, right_files = Planner._discover_table_files_and_reader(right_table_name)

            # 计算两个表的大小
            left_size_mb = Planner._get_table_size_mb(left_files)
            right_size_mb = Planner._get_table_size_mb(right_files)

            # ============================================================
            # 广播连接决策
            # ============================================================
            # 广播连接：把小表完整地发送到每个 Worker，与大表的每个分区进行连接
            # 优点：无需数据 shuffle（网络传输），适合小表
            # 缺点：小表会被复制到每个 Worker，不适合大表

            small_table_side: Optional[str] = None

            # 检查右表是否足够小可以使用广播连接
            if right_size_mb <= settings.BROADCAST_THRESHOLD_MB:
                small_table_side = 'right'
            # 检查左表是否足够小可以使用广播连接
            elif left_size_mb <= settings.BROADCAST_THRESHOLD_MB:
                small_table_side = 'left'

            # ============================================================
            # 使用广播连接
            # ============================================================
            if small_table_side:
                print(f"✅ Planner 选择广播连接。小表：'{small_table_side}'")

                # 确定大表和小表（根据检测结果）
                if small_table_side == 'right':
                    large_name, large_alias, large_reader = left_table_name, left_table_alias, left_reader_func
                    small_name, small_alias, small_reader = right_table_name, right_table_alias, right_reader_func
                else:  # left is small
                    large_name, large_alias, large_reader = right_table_name, right_table_alias, right_reader_func
                    small_name, small_alias, small_reader = left_table_name, left_table_alias, left_reader_func

                # 复制 AST 用于生成 Worker 执行的 SQL
                worker_query_ast = query_ast.copy()

                # 简化 SELECT 列表为 SELECT *，Worker 只需返回所有列
                select_clause = worker_query_ast.find(exp.Select)
                select_clause.set('expressions', [exp.Star()])

                # 移除 Group By、Order By、Limit - 这些在协调器最终聚合时处理
                if worker_query_ast.find(exp.Group): worker_query_ast.find(exp.Group).pop()
                if worker_query_ast.find(exp.Order): worker_query_ast.find(exp.Order).pop()
                if worker_query_ast.find(exp.Limit): worker_query_ast.find(exp.Limit).pop()

                # 将 JOIN ON 转换为 JOIN USING
                # 原因：避免输出中出现重复的连接键列
                # 例如：ON a.id = b.id → USING(id)
                join_node = worker_query_ast.find(exp.Join)
                on_clause = join_node.args.get('on')

                if on_clause and isinstance(on_clause.find(exp.EQ), exp.EQ):
                    # 提取键名（假设左右两边的键名相同）
                    join_key_name = on_clause.find(exp.EQ).left.name

                    # 移除旧的 ON 子句
                    join_node.set('on', None)

                    # 设置新的 USING 子句
                    join_node.set('using', exp.Tuple(expressions=[exp.to_identifier(join_key_name)]))

                # 替换表引用
                # 大表：替换为读取函数占位符（Worker 会填充实际文件路径）
                # 小表：替换为别名（因为小表会被广播到 Worker 内存中）
                for table_expr in worker_query_ast.find_all(exp.Table):
                    if table_expr.this.name == large_name:
                        # 大表使用读取函数，如 read_parquet('{file_path}')
                        table_expr.replace(parse_one(f"{large_reader}('{{file_path}}') AS {large_alias}"))
                    elif table_expr.this.name == small_name:
                        # 小表使用别名（已作为广播表注册）
                        table_expr.replace(exp.to_table(small_alias))

                return DistributedBroadcastJoinPlan(
                    query_ast=query_ast,
                    large_table_name=large_name,
                    large_table_alias=large_alias,
                    large_table_reader_func=large_reader,
                    small_table_name=small_name,
                    small_table_alias=small_alias,
                    small_table_reader_func=small_reader,
                    worker_join_sql=worker_query_ast.sql(dialect="duckdb")
                )

            # ============================================================
            # 分区连接（Shuffle Join）- 默认的连接策略
            # ============================================================
            # 用于两个大表的连接
            # 原理：
            # 1. Map 阶段：按连接键哈希分区，两边相同键的数据会进入相同分区
            # 2. Shuffle 阶段：把每个分区发送到对应的 Worker
            # 3. Reduce 阶段：每个 Worker 连接收到的分区数据

            print("✅ Planner 选择分区连接（Shuffle Join）。")

            # 提取连接键
            join_on_clause = join_expression.args.get('on')
            join_using_clause = join_expression.args.get('using')

            if not join_on_clause and not join_using_clause:
                raise ValueError("不支持的 JOIN 类型：没有找到 ON 或 USING 子句。")

            if join_on_clause:
                # ON 子句：如 ON a.id = b.id
                eq_expr = join_on_clause.find(exp.EQ)
                if not eq_expr: raise ValueError("ON 子句中找不到有效的相等（=）条件。")
                left_key = eq_expr.left.name
                right_key = eq_expr.right.name
            else:
                # USING 子句：如 USING(id)
                key_name = join_using_clause.expressions[0].this.name
                left_key = right_key = key_name

            # ============================================================
            # 分离 WHERE 条件
            # ============================================================
            # 将 WHERE 条件分离到左右两边，分别下推到对应的 Workers
            # 这是一种优化：减少网络传输的数据量
            left_where_parts = []
            right_where_parts = []
            left_where_sql = ""
            right_where_sql = ""

            if where_clause := query_ast.find(exp.Where):
                def process_condition(condition):
                    """处理单个条件，将其分配到对应的表"""
                    cleaned_condition = condition.copy()
                    # 移除列的表前缀（如 user.name → name）
                    for col in cleaned_condition.find_all(exp.Column):
                        col.set('table', None)

                    first_col = condition.find(exp.Column)
                    if first_col:
                        table_of_col = first_col.table
                        if table_of_col == left_table_alias:
                            left_where_parts.append(cleaned_condition.sql(dialect="duckdb"))
                        elif table_of_col == right_table_alias:
                            right_where_parts.append(cleaned_condition.sql(dialect="duckdb"))

                if isinstance(where_clause.this, exp.Connector):
                    # AND/OR 连接的条件
                    for condition in where_clause.this.expressions:
                        process_condition(condition)
                else:
                    # 单一条件
                    process_condition(where_clause.this)

                if left_where_parts:
                    left_where_sql = f"WHERE {' AND '.join(left_where_parts)}"
                if right_where_parts:
                    right_where_sql = f"WHERE {' AND '.join(right_where_parts)}"

            # ============================================================
            # 窗口函数处理
            # ============================================================
            # 窗口函数（如 SUM() OVER()）需要在数据分区后单独处理
            # 因为聚合已经在 Workers 上执行，窗口函数需要在协调器上基于聚合结果计算
            final_select_sql = None
            original_select_clause = query_ast.find(exp.Select)

            if any(expr.find(exp.Window) for expr in original_select_clause.expressions):
                # 创建用于最终阶段的 AST
                final_query_ast = query_ast.copy()

                # 移除已处理的子句
                if final_query_ast.find(exp.Where): final_query_ast.find(exp.Where).pop()
                if final_query_ast.find(exp.Group): final_query_ast.find(exp.Group).pop()
                if final_query_ast.find(exp.Join): final_query_ast.find(exp.Join).pop()

                # 将表替换为聚合结果表
                final_query_ast.find(exp.Table).replace(parse_one("aggregated_results"))

                # 创建聚合表达式到别名 的映射
                # 例如：SUM(o.amount) AS total_spent
                agg_to_alias_map = {
                    expr.this.sql(dialect="duckdb"): expr.alias
                    for expr in original_select_clause.expressions
                    if isinstance(expr, exp.Alias) and expr.find(exp.AggFunc)
                }

                # 将所有聚合函数替换为列引用（因为聚合已完成）
                for agg_func in final_query_ast.find_all(exp.AggFunc):
                    agg_sql = agg_func.sql(dialect="duckdb")
                    if agg_sql in agg_to_alias_map:
                        agg_func.replace(exp.column(agg_to_alias_map[agg_sql]))

                # 移除列的表前缀（因为现在是扁平表）
                for col in final_query_ast.find_all(exp.Column):
                    col.set('table', None)

                final_select_sql = final_query_ast.sql(dialect="duckdb")

            # ============================================================
            # 生成 Workers 执行的 SQL
            # ============================================================
            query_for_workers = query_ast.copy()

            # 如果有窗口函数，只保留非窗口表达式
            if final_select_sql:
                select_clause_for_worker = query_for_workers.find(exp.Select)
                new_expressions = []
                for expr in select_clause_for_worker.expressions:
                    if not expr.find(exp.Window):
                        new_expressions.append(expr)
                select_clause_for_worker.set('expressions', new_expressions)

            # 移除协调器将处理的子句
            if query_for_workers.find(exp.Where): query_for_workers.find(exp.Where).pop()
            if query_for_workers.find(exp.Order): query_for_workers.find(exp.Order).pop()
            if query_for_workers.find(exp.Limit): query_for_workers.find(exp.Limit).pop()

            # 替换表为本地临时表（Workers 会注册这些表）
            query_for_workers.find(exp.Table).replace(parse_one(f"{left_table_alias}_local AS {left_table_alias}"))
            query_for_workers.find(exp.Join).this.replace(parse_one(f"{right_table_alias}_local AS {right_table_alias}"))
            worker_sql_str = query_for_workers.sql(dialect="duckdb")

            return DistributedShuffleJoinPlan(
                query_ast=query_ast,
                left_table_name=left_table_name,
                left_table_alias=left_table_alias,
                left_join_key=left_key,
                right_table_name=right_table_name,
                right_table_alias=right_table_alias,
                right_join_key=right_key,
                worker_join_sql=worker_sql_str,
                left_reader_function=left_reader_func,
                right_reader_function=right_reader_func,
                left_where_sql=left_where_sql,
                right_where_sql=right_where_sql,
                final_select_sql=final_select_sql
            )

        # ============================================================
        # 情况 3：单表查询（分布式扫描）
        # ============================================================
        # 查询只涉及一个表，没有 JOIN
        # 策略：将表的各个文件分发给 Workers 并行扫描
        else:
            table_name = all_tables_in_query[0]
            reader_func, _ = Planner._discover_table_files_and_reader(table_name)

            # 提取 WHERE 条件（如果有）
            where_str = ""
            if where_clause := query_ast.find(exp.Where):
                where_str = f" {where_clause.sql(dialect='duckdb')}"

            # 生成 Worker 的查询模板
            # {files} 会被实际的文件列表替换
            worker_template = f"SELECT * FROM {reader_func}({{files}}){where_str}"

            return DistributedScanPlan(
                query_ast=query_ast,
                table_name=table_name,
                worker_query_template=worker_template,
            )
