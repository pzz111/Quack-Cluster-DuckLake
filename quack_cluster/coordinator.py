# quack_cluster/coordinator.py
# ============================================================
# Coordinator（协调器）- 整个系统的主入口点
# ============================================================
# 职责说明：
# 1. 作为 FastAPI 应用的主入口，接收用户的 HTTP 请求
# 2. 解析 SQL 语句，将其转换为抽象语法树（AST）
# 3. 处理复杂的 SQL 结构（CTE、UNION、子查询）
# 4. 协调 Planner 和 Executor 完成查询执行
# 5. 提供查询缓存功能，加快重复查询的速度
# 6. 提供 /explain 端点，可视化执行计划

import ray
import asyncio
import uuid
import logging
import duckdb
import pyarrow as pa
import pyarrow.ipc as ipc
import time
import hashlib
import graphviz
from fastapi import FastAPI, HTTPException, Response, Query, status
from pydantic import BaseModel
from sqlglot import exp, parse_one
from sqlglot.errors import ParseError
from contextlib import asynccontextmanager
from typing import Optional, Any, Dict, Tuple, List

from .planner import Planner
from .executor import Executor
from .settings import settings
from .execution_plan import BasePlan, DistributedScanPlan, DistributedShuffleJoinPlan, LocalExecutionPlan

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================
# 查询缓存类 - 提高重复查询的性能
# ============================================================
# 为什么需要缓存？
# 当用户多次执行相同的 SQL 查询时，如果每次都重新执行，会浪费计算资源。
# 缓存将查询结果存储在内存中，一段时间内（TTL）再次查询时直接返回缓存结果。
class QueryCache:
    """
    内存缓存类，支持 TTL（生存时间）和线程安全。

    工作原理：
    1. 使用字典存储缓存数据
    2. 每个缓存项有过期时间
    3. 获取缓存时检查是否过期
    4. 设置缓存时指定 TTL 秒数
    """
    def __init__(self):
        self._cache: Dict[str, Any] = {}  # 存储缓存的查询结果
        self._ttl: Dict[str, float] = {}  # 存储每个缓存项的过期时间戳
        self._lock = asyncio.Lock()  # 异步锁，保证线程安全

    async def get(self, key: str) -> Optional[Any]:
        """
        获取缓存的查询结果

        Args:
            key: 缓存的键（通过 SQL 语句和格式计算得出的哈希值）

        Returns:
            如果缓存存在且未过期，返回缓存的数据；否则返回 None
        """
        async with self._lock:
            if key in self._cache:
                if time.time() < self._ttl[key]:
                    logger.info(f"✅ 缓存命中！key: {key[:10]}...")
                    return self._cache[key]
                else:
                    logger.info(f"缓存已过期！key: {key[:10]}...")
                    del self._cache[key]
                    del self._ttl[key]
            return None

    async def set(self, key: str, value: Any, ttl_seconds: int):
        """
        设置缓存的查询结果

        Args:
            key: 缓存的键
            value: 要缓存的查询结果数据
            ttl_seconds: 缓存的生存时间（秒）
        """
        async with self._lock:
            self._cache[key] = value
            self._ttl[key] = time.time() + ttl_seconds
            logger.info(f"已设置缓存，key: {key[:10]}...，TTL: {ttl_seconds}秒")


# 全局缓存实例
query_cache = QueryCache()


# ============================================================
# FastAPI 应用生命周期管理
# ============================================================
# lifespan 上下文管理器：在应用启动时初始化 Ray，在应用关闭时清理资源
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI 生命周期管理器

    应用启动时：
    1. 连接到 Ray 集群（address="auto" 表示自动发现集群）
    2. 使用 "quack-cluster" 命名空间隔离资源
    3. 忽略重复初始化错误（ignore_reinit_error=True）

    应用关闭时：
    记录断开连接日志
    """
    ray.init(address="auto", namespace="quack-cluster", ignore_reinit_error=True)
    logger.info("✅ Ray 连接成功！")
    yield
    logger.info("Ray 正在断开连接...")


# 创建 FastAPI 应用实例
app = FastAPI(title="Quack-Cluster", lifespan=lifespan)


# ============================================================
# 请求和响应模型
# ============================================================
class QueryRequest(BaseModel):
    """
    查询请求模型

    用户发送的 JSON 请求体应包含：
    - sql: 要执行的 SQL 查询语句
    """
    sql: str


# ============================================================
# 执行计划可视化 - 帮助理解查询如何执行
# ============================================================

def visualize_plan(plan: BasePlan) -> bytes:
    """
    使用 Graphviz 生成执行计划的可视化图表（PNG 格式）

    这个函数帮助开发者理解 Quack-Cluster 如何执行查询：
    - LocalExecutionPlan：简单查询，直接在协调器执行
    - DistributedScanPlan：分布式扫描， workers 并行读取数据
    - DistributedShuffleJoinPlan：分布式连接，先分区再合并

    Args:
        plan: 执行计划对象（由 Planner 生成）

    Returns:
        PNG 格式的图片字节数据
    """
    dot = graphviz.Digraph(comment='Query Execution Plan')
    dot.attr('node', shape='box', style='rounded,filled', fillcolor='lightblue')
    dot.attr(rankdir='TB', label=f'Execution Plan: {plan.plan_type.upper()}', labelloc='t', fontsize='20')

    # 定义不同类型节点的配色方案
    coordinator_color = 'skyblue'   # 协调器节点 - 天蓝色
    worker_color = 'lightgreen'     # 工作节点 - 浅绿色
    data_color = 'khaki'            # 数据节点 - 浅黄色

    # 节点属性字典
    coordinator_node_attrs = {'fillcolor': coordinator_color}
    worker_node_attrs = {'fillcolor': worker_color}
    data_node_attrs = {'shape': 'parallelogram', 'fillcolor': data_color}

    # ============================================================
    # 根据不同计划类型生成不同的可视化流程图
    # ============================================================

    if isinstance(plan, LocalExecutionPlan):
        # 本地执行计划：最简单的情况
        # 流程：协调器 → 执行 SQL → 返回结果
        dot.node('Coordinator', 'Coordinator\n(本地执行)', **coordinator_node_attrs)
        dot.node('SQL', f"执行 SQL:\n{plan.query_ast.sql(pretty=True)}", shape='note', fillcolor='white')
        dot.node('Result', '最终结果', shape='ellipse', fillcolor='lightgray')
        dot.edge('Coordinator', 'SQL')
        dot.edge('SQL', 'Result')

    elif isinstance(plan, DistributedScanPlan):
        # 分布式扫描计划：单表查询
        # 流程：数据文件 → Workers 并行扫描过滤 → 协调器聚合 → 返回结果
        with dot.subgraph(name='cluster_workers') as c:
            c.attr(label='阶段 1: 并行扫描（Workers）')
            c.attr(style='filled', color='lightgrey')
            c.node('Worker_Scan', f"扫描并过滤 Workers (1..N)\n模板: {plan.worker_query_template}", **worker_node_attrs)
            c.node('Data_Files', f"'{plan.table_name}' 的数据文件", **data_node_attrs)
            c.edge('Data_Files', 'Worker_Scan')

        with dot.subgraph(name='cluster_coordinator') as c:
            c.attr(label='阶段 2: 最终聚合（协调器）')
            c.attr(style='filled', color='lightgrey')
            final_query = plan.query_ast.copy()
            if final_query.find(exp.Where): final_query.find(exp.Where).pop()
            final_query.find(exp.Table).replace(parse_one("combined_arrow_table"))
            c.node('Coordinator_Agg', f"协调器\n最终聚合\n{final_query.sql(pretty=True)}", **coordinator_node_attrs)

        dot.node('Result', '最终结果', shape='ellipse', fillcolor='lightgray')
        dot.edge('Worker_Scan', 'Coordinator_Agg', label='部分结果')
        dot.edge('Coordinator_Agg', 'Result')

    elif isinstance(plan, DistributedShuffleJoinPlan):
        # 分布式 Shuffle Join 计划：多表连接（最复杂）
        # 流程分为三个阶段：

        # 阶段 1: 按 key 分区（Map）
        with dot.subgraph(name='cluster_partition') as c:
            c.attr(label='阶段 1: 按 Key 分区（Map）')
            c.attr(style='filled', color='lightgrey')
            c.node('Left_Data', f"左表 '{plan.left_table_name}' 的文件", **data_node_attrs)
            c.node('Right_Data', f"右表 '{plan.right_table_name}' 的文件", **data_node_attrs)
            c.node('Partition_L', f"分区 Workers (左)\nKey: {plan.left_join_key}", **worker_node_attrs)
            c.node('Partition_R', f"分区 Workers (右)\nKey: {plan.right_join_key}", **worker_node_attrs)
            c.edge('Left_Data', 'Partition_L')
            c.edge('Right_Data', 'Partition_R')

        # 阶段 2: 连接分区（Reduce）
        with dot.subgraph(name='cluster_join') as c:
            c.attr(label='阶段 2: 连接分区（Reduce）')
            c.attr(style='filled', color='lightgrey')
            c.node('Joiner', f"Joiner Workers (1..{settings.NUM_SHUFFLE_PARTITIONS})\nSQL: {plan.worker_join_sql}", **worker_node_attrs)

        # 阶段 3: 最终聚合（协调器）
        with dot.subgraph(name='cluster_final_agg') as c:
            c.attr(label='阶段 3: 最终聚合（协调器）')
            c.attr(style='filled', color='lightgrey')
            c.node('Final_Agg', '协调器\n最终聚合、排序、限制', **coordinator_node_attrs)

        dot.node('Result', '最终结果', shape='ellipse', fillcolor='lightgray')

        dot.edge('Partition_L', 'Joiner', label=f'打乱后的分区 (左)')
        dot.edge('Partition_R', 'Joiner', label=f'打乱后的分区 (右)')
        dot.edge('Joiner', 'Final_Agg', label='部分连接结果')
        dot.edge('Final_Agg', 'Result')

    # 渲染为 PNG 格式的字节数据
    return dot.pipe(format='png')


# ============================================================
# 核心查询解析和执行逻辑
# ============================================================
async def resolve_and_execute(query_ast: exp.Expression, con: duckdb.DuckDBPyConnection) -> pa.Table:
    """
    递归解析并执行 SQL 查询的核心函数

    这个函数是查询处理的"大脑"，它按照特定的顺序处理 SQL 的不同部分：

    处理顺序：
    1. CTE（公用表表达式）- WITH 子句
    2. UNION（合并查询）
    3. 子查询
    4. 简单查询

    为什么需要递归？
    - CTE 和子查询需要先执行，得到结果后再用于主查询
    - 递归可以处理多层嵌套的 CTE 和子查询

    Args:
        query_ast: SQL 语句的抽象语法树（由 SQLGlot 解析生成）
        con: DuckDB 数据库连接（用于执行本地查询）

    Returns:
        查询结果的 PyArrow 表
    """
    if not query_ast:
        raise ValueError("收到了空的 AST 节点，无法执行。")

    # ============================================================
    # 情况 1：处理 WITH 子句（CTE）
    # ============================================================
    # CTE 允许你定义一个临时结果集，然后在主查询中引用它
    # 例如：WITH temp AS (SELECT * FROM table) SELECT * FROM temp
    if with_clause := query_ast.args.get('with'):
        query_body = query_ast.copy()
        query_body.set('with', None)  # 移除 WITH 子句，先处理 CTE

        # 递归处理每个 CTE
        for cte in with_clause.expressions:
            logger.info(f"➡️ 正在解析 CTE：'{cte.alias}'")
            # 执行 CTE 子查询
            cte_result_table = await resolve_and_execute(cte.this, con)
            # 将 CTE 结果注册为临时表
            con.register(cte.alias, cte_result_table)
            logger.info(f"✅ CTE '{cte.alias}' 已注册为内存表。")

        # CTE 处理完毕后，递归处理主查询体
        return await resolve_and_execute(query_body, con)

    # ============================================================
    # 情况 2：处理 UNION（合并查询）
    # ============================================================
    # UNION 将多个查询的结果合并为一个
    # DISTINCT 会去除重复行，ALL 会保留所有行（包括重复）
    if isinstance(query_ast, exp.Union):
        logger.info(f"➡️ 正在解析 UNION（{'DISTINCT' if query_ast.args.get('distinct') else 'ALL'}）")

        # 左右两部分查询并行执行
        left_result, right_result = await asyncio.gather(
            resolve_and_execute(query_ast.this, con),
            resolve_and_execute(query_ast.expression, con)
        )

        # 合并结果
        combined_table = pa.concat_tables([left_result, right_result])

        # 如果需要去重，执行 DISTINCT
        if query_ast.args.get('distinct'):
            logger.info("✨ 为 UNION 操作在协调器上执行 DISTINCT 去重。")
            con.register("union_combined_view", combined_table)
            final_result_table = con.execute("SELECT DISTINCT * FROM union_combined_view").fetch_arrow_table()
            con.unregister("union_combined_view")
            return final_result_table

        return combined_table

    # ============================================================
    # 情况 3：处理子查询
    # ============================================================
    # 子查询是嵌套在主查询中的查询，可以出现在：
    # - FROM 子句中（如：SELECT * FROM (SELECT ...) AS subq）
    # - JOIN 条件中（如：JOIN (SELECT ...) ON ...）
    # - WHERE IN 子句中（如：WHERE id IN (SELECT id FROM ...)）
    for subquery in query_ast.find_all(exp.Subquery):
        subquery_alias = subquery.alias_or_name or f"_subquery_{uuid.uuid4().hex[:8]}"
        logger.info(f"➡️ 正在解析子查询：'{subquery_alias}'")

        # 递归执行子查询
        subquery_result_table = await resolve_and_execute(subquery.this, con)

        # 根据子查询出现的位置，以不同方式处理结果
        if isinstance(subquery.parent, (exp.From, exp.Join)):
            # 子查询在 FROM/JOIN 中：注册为临时表
            con.register(subquery_alias, subquery_result_table)
            subquery.replace(exp.to_table(subquery_alias))
        elif isinstance(subquery.parent, exp.In):
            # 子查询在 WHERE IN 中：将结果转换为值列表
            if subquery_result_table.num_columns > 0 and subquery_result_table.num_rows > 0:
                values = subquery_result_table.columns[0].to_pylist()
                # 将值转换为 SQL 字面量
                literal_expressions = [exp.Literal.string(v) if isinstance(v, str) else exp.Literal.number(v) for v in values]
                subquery.replace(exp.Tuple(expressions=literal_expressions))
            else:
                # 子查询结果为空
                subquery.replace(exp.Tuple(expressions=[exp.Null()]))

    # ============================================================
    # 情况 4：简单查询 - 交给 Planner 和 Executor 处理
    # ============================================================
    logger.info(f"✅ AST 已简化，可以开始规划：{query_ast.sql(dialect='duckdb')}")

    # 查询协调器中已注册的表（可能是之前的 CTE 或子查询结果）
    registered_tables = {row[0] for row in con.execute("PRAGMA show_tables;").fetchall()}

    # 创建执行计划
    plan = Planner.create_plan(query_ast, registered_tables)
    logger.info(f"✅ 计划已创建：{plan.plan_type}")

    # 执行计划
    return await Executor.execute(plan, con)


# ============================================================
# API 端点：解释查询计划
# ============================================================
@app.post("/explain")
async def explain_query(request: QueryRequest):
    """
    分析 SQL 查询，生成执行计划并返回可视化的计划图

    这个端点用于帮助开发者理解查询将如何执行：
    - 单表查询会被识别为分布式扫描
    - 多表查询会被识别为分布式连接
    - 无表查询会在本地执行

    Returns:
        PNG 格式的执行计划可视化图片
    """
    logger.info(f"为查询生成 EXPLAIN 计划：{request.sql}")

    try:
        # 1. 解析 SQL 为 AST
        try:
            parsed_query = parse_one(request.sql, read="duckdb")
        except ParseError as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"SQL 语法错误：{e}")

        # 2. 创建执行计划
        # 注意：在 /explain 上下文中，我们假设没有预注册的表
        plan = Planner.create_plan(parsed_query, registered_tables=set())
        logger.info(f"✅ 为 EXPLAIN 创建的计划：{plan.plan_type}")

        # 3. 生成可视化
        image_bytes = visualize_plan(plan)

        # 4. 返回图片响应
        return Response(content=image_bytes, media_type="image/png")

    except FileNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except (ValueError, NotImplementedError) as e:
        logger.exception("查询规划时发生错误")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.exception("EXPLAIN 发生未处理的错误")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"内部错误：{e}")


# ============================================================
# API 端点：执行查询
# ============================================================
@app.post("/query")
async def execute_query(
    request: QueryRequest,
    format: Optional[str] = Query("json", enum=["json", "arrow"])
):
    """
    主要的 SQL 查询执行端点

    工作流程：
    1. 检查缓存，如果命中直接返回
    2. 解析 SQL 语句
    3. 创建 DuckDB 内存连接
    4. 递归解析并执行查询（处理 CTE、UNION、子查询）
    5. 将结果格式化为 JSON 或 Arrow 格式返回
    6. 将结果存入缓存

    Args:
        request: 包含 SQL 语句的请求体
        format: 返回格式，"json" 或 "arrow"（Arrow 格式更适合大数据量）

    Returns:
        查询结果（JSON 格式或 Arrow 格式）
    """
    # 计算缓存键（基于 SQL 和格式的哈希值）
    cache_key = hashlib.sha256(f"{request.sql}:{format}".encode()).hexdigest()
    cached_data = await query_cache.get(cache_key)

    # 缓存命中，直接返回
    if cached_data:
        content, media_type = cached_data
        if media_type == "application/json":
            return content
        else:
            return Response(content=content, media_type=media_type)

    logger.info(f"缓存未命中，key: {cache_key[:10]}... 执行查询。")

    try:
        # 解析 SQL 语句
        try:
            parsed_query = parse_one(request.sql, read="duckdb")
        except ParseError as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"SQL 语法错误：{e}")

        # 创建 DuckDB 内存数据库连接
        main_con = duckdb.connect(database=':memory:')

        # 递归解析并执行查询
        final_arrow_table = await resolve_and_execute(parsed_query, main_con)

        if final_arrow_table is None:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="查询执行没有返回结果。")

        # 根据请求的格式返回结果
        if format == "arrow":
            # Arrow 格式：流式传输，更高效，适合大数据量
            sink = pa.BufferOutputStream()
            with ipc.new_stream(sink, final_arrow_table.schema) as writer:
                writer.write_table(final_arrow_table)
            content = sink.getvalue().to_pybytes()
            media_type = "application/vnd.apache.arrow.stream"
            await query_cache.set(cache_key, (content, media_type), settings.CACHE_TTL_SECONDS)
            return Response(content=content, media_type=media_type)
        else:
            # JSON 格式：人类可读，适合小数据量
            result_dict = {"result": final_arrow_table.to_pandas().to_dict(orient="records")}
            await query_cache.set(cache_key, (result_dict, "application/json"), settings.CACHE_TTL_SECONDS)
            return result_dict

    except HTTPException as http_exc:
        raise http_exc
    except FileNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except (ValueError, NotImplementedError, duckdb.BinderException, duckdb.ParserException, TypeError) as e:
        logger.exception("查询规划或执行时发生错误")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.exception("发生未处理的错误")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"内部错误：{e}")
