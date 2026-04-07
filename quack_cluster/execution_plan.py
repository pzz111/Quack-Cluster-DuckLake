# quack_cluster/execution_plan.py
# ============================================================
# 执行计划（Execution Plan）- 组件之间的"契约"
# ============================================================
# 什么是 Execution Plan？
# - 执行计划是一个数据结构，包含执行查询所需的所有信息
# - 由 Planner 生成，由 Executor 消费
# - 类似建筑蓝图：描述要做什么，但不做如何做
#
# 为什么需要单独的 Plan 文件？
# 1. 分离关注点：规划逻辑和执行逻辑分开
# 2. 类型安全：使用 Pydantic 验证数据结构
# 3. 清晰接口：Planner 和 Executor 通过 Plan 通信，不直接依赖
#
# 设计原则：
# - Plan 是"声明式"的：只描述要做什么，不描述怎么做
# - Plan 是"不可变的"：创建后不能修改
# - Plan 是"自包含的"：包含执行所需的所有信息
#
# 四种执行计划类型：
# 1. LocalExecutionPlan - 本地执行，不需要分布式
# 2. DistributedScanPlan - 分布式扫描，单表查询
# 3. DistributedShuffleJoinPlan - 分区连接，两大表 JOIN
# 4. DistributedBroadcastJoinPlan - 广播连接，小表 JOIN 大表

from pydantic import BaseModel
from typing import List, Tuple, Any, Literal, Optional
from sqlglot import exp, parse_one
from sqlglot.errors import ParseError


# ============================================================
# 基类：所有执行计划的父类
# ============================================================
class BasePlan(BaseModel):
    """
    执行计划的基类

    所有具体计划类型都继承此类。

    属性：
    - plan_type: 计划类型标识符（'local'、'scan'、'join'、'broadcast_join'）
    - query_ast: SQL 的抽象语法树（原始或修改后用于最终聚合）

    为什么需要 Pydantic？
    - 自动验证数据完整性
    - 提供类型提示，方便 IDE 自动完成
    - 在 Plan 创建时检查必填字段
    """

    plan_type: str  # 计划类型标识符
    query_ast: exp.Expression  # SQL 抽象语法树

    class Config:
        arbitrary_types_allowed = True  # 允许 SQLGlot 的 Expression 类型


# ============================================================
# 本地执行计划
# ============================================================
class LocalExecutionPlan(BasePlan):
    """
    本地执行计划 - 最简单的查询

    适用场景：
    - 查询不涉及任何数据文件（SELECT 1 + 1）
    - 所有数据都已注册在协调器的内存中（CTE、子查询的结果）

    执行方式：
    - 直接在协调器的 DuckDB 连接上执行
    - 不需要 Workers 参与

    示例：
    - SELECT 42 * 2
    - WITH cte AS (SELECT 1) SELECT * FROM cte
    """

    plan_type: Literal["local"] = "local"  # 固定为 "local"


# ============================================================
# 分布式扫描计划
# ============================================================
class DistributedScanPlan(BasePlan):
    """
    分布式扫描计划 - 单表查询

    适用场景：
    - 查询只涉及一个表
    - 没有 JOIN 操作
    - 示例：SELECT * FROM orders WHERE amount > 100

    执行流程：
    1. 找到表的所有数据文件
    2. 为每个文件创建一个 Worker 任务
    3. Workers 并行扫描文件，应用 WHERE 条件
    4. 协调器合并结果，执行最终聚合

    优化点：
    - WHERE 条件下推到 Workers（减少网络传输）
    - 并行扫描多个文件（提高吞吐量）
    """

    plan_type: Literal["scan"] = "scan"  # 固定为 "scan"

    table_name: str  # 要查询的表名
    worker_query_template: str  # Worker 执行的 SQL 模板（包含 {files} 占位符）


# ============================================================
# 分布式 Shuffle Join 计划
# ============================================================
class DistributedShuffleJoinPlan(BasePlan):
    """
    分布式分区连接计划 - 两个大表的 JOIN

    适用场景：
    - 两个表都很大，无法使用广播连接
    - 需要分区以避免内存溢出

    执行流程（三阶段）：

    阶段 1 - Map（分区）：
    - 读取左右两表的数据
    - 按 JOIN 键的哈希值分区
    - 相同键的数据进入相同分区

    阶段 2 - Shuffle & Reduce（连接）：
    - 将分区数据发送到对应 Workers
    - 每个 Worker 连接收到的分区

    阶段 3 - Final Aggregation（最终聚合）：
    - 合并所有部分结果
    - 执行 GROUP BY、ORDER BY、LIMIT

    为什么需要分区？
    - 大表直接广播会内存溢出
    - 分区后每个 Worker 只处理一部分数据
    """

    plan_type: Literal["join"] = "join"  # 固定为 "join"

    # 左表信息
    left_table_name: str  # 左表名
    left_table_alias: str  # 左表别名（如 FROM orders AS o）
    left_join_key: str  # 左表的连接键
    left_reader_function: str  # DuckDB 读取函数（read_parquet 等）
    left_where_sql: str  # 下推到左表的 WHERE 条件

    # 右表信息
    right_table_name: str  # 右表名
    right_table_alias: str  # 右表别名
    right_join_key: str  # 右表的连接键
    right_reader_function: str  # DuckDB 读取函数
    right_where_sql: str  # 下推到右表的 WHERE 条件

    # 连接信息和 Worker SQL
    worker_join_sql: str  # Workers 执行的 JOIN SQL

    # 窗口函数支持
    final_select_sql: Optional[str] = None  # 最终聚合时的 SELECT SQL（用于窗口函数）


# ============================================================
# 分布式 Broadcast Join 计划
# ============================================================
class DistributedBroadcastJoinPlan(BasePlan):
    """
    分布式广播连接计划 - 小表 JOIN 大表

    适用场景：
    - 一个表很小（小于 BROADCAST_THRESHOLD_MB）
    - 另一个表很大

    执行流程：
    1. 协调器读取整个小表到内存
    2. 将小表广播到所有 Workers
    3. 每个 Worker 读取大表的一个分区
    4. Worker 本地执行 JOIN
    5. 协调器合并结果

    为什么广播连接更快？
    - 无需数据分区（Shuffle）
    - 小表只传输一次，之后所有 JOIN 本地完成
    - 网络传输量最小

    何时使用？
    - 小表 + 大表（表大小差异明显）
    - 小表能够适应 Worker 的内存
    """

    plan_type: Literal["broadcast_join"] = "broadcast_join"  # 固定为 "broadcast_join"

    # 大表信息（需要分区扫描）
    large_table_name: str  # 大表名
    large_table_alias: str  # 大表别名
    large_table_reader_func: str  # DuckDB 读取函数

    # 小表信息（广播到所有 Workers）
    small_table_name: str  # 小表名
    small_table_alias: str  # 小表别名
    small_table_reader_func: str  # DuckDB 读取函数

    # Worker 执行的 SQL
    # 大表部分会被替换为实际文件路径
    # 小表部分会被替换为广播的表别名
    worker_join_sql: str
