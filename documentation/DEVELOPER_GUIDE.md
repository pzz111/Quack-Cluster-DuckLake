# Quack-Cluster 开发者指南

欢迎加入 Quack-Cluster 团队！本文档是帮助开发者深入理解代码工作流程、修改代码或向系统添加新功能的实用指南。

---

## 1. 查询执行流程（详细演练）

为了真正理解系统工作原理，让我们从头到尾跟踪几个示例查询。

### 演练 1：简单（本地）查询

此查询不需要磁盘数据，可以立即执行。
**SQL**: `SELECT 42 * 2;`

1. **Coordinator**：`execute_query` 接收请求。`parse_one` 将 SQL 转换为 AST。
2. **Coordinator**：`resolve_and_execute` 被调用。由于没有 CTE、UNION 或子查询，直接进入规划逻辑。
3. **Planner**：`create_plan` 被调用。检测到此查询没有 `FROM` 子句（没有表）。
4. **Planner**：返回一个只包含原始 AST 的 `LocalExecutionPlan`。
5. **Executor**：`execute` 被调用。看到计划类型是 `local`，调用 `_execute_local`。
6. **Executor**：`_execute_local` 使用 DuckDB 连接直接运行 SQL `SELECT 42 * 2;` 并立即返回结果。
7. **Coordinator**：结果（Arrow 表）被接收并格式化为 JSON/Arrow 用于响应。

### 演练 2：分布式扫描查询

此查询读取单个表、过滤并执行聚合。
**SQL**: `SELECT passenger_count, COUNT(*) FROM "yellow_tripdata.parquet" WHERE PULocationID > 100 GROUP BY 1;`

1. **Coordinator**：与上述类似，AST 被简化并交给 Planner。
2. **Planner**：`create_plan` 被调用。检测到一个表（`yellow_tripdata.parquet`）且没有 JOIN。
3. **Planner**：返回一个包含以下内容的 `DistributedScanPlan`：
   - `table_name`：`"yellow_tripdata.parquet"`
   - `worker_query_template`：`"SELECT * FROM read_parquet({files}) WHERE PULocationID > 100"`
   - `query_ast`：原始 AST，用于最终聚合。
4. **Executor**：`execute` 调用 `_execute_scan`。
5. **Executor**：`_execute_scan` 找到匹配 `yellow_tripdata.parquet*` 的所有文件，为每个文件创建一个 Ray Worker，并使用上述模板调用 `worker.query.remote()`。所有 Workers 并行操作过滤数据。
6. **Executor**：`asyncio.gather` 收集所有 Workers 的结果（过滤后的 Arrow 表）。这些结果被连接（`pa.concat_tables`）成一个名为 `combined_arrow_table` 的大表。
7. **Executor**：修改原始 AST：移除 `WHERE` 子句（因为 Workers 已经应用了），并将表名替换为 `combined_arrow_table`。最终查询变为：`SELECT passenger_count, COUNT(*) FROM combined_arrow_table GROUP BY 1;`。
8. **Executor**：在协调器的 DuckDB 连接上运行最终查询以产生最终聚合。
9. **Coordinator**：结果返回给用户。

### 演练 3：分布式 Shuffle Join 查询

这是最复杂的流程。
**SQL**: `SELECT c.c_name, o.o_orderstatus FROM customer AS c JOIN orders AS o ON c.c_custkey = o.o_custkey;`

1. **Coordinator**：AST 被简化并传递给 Planner。
2. **Planner**：`create_plan` 检测到 `JOIN`。
3. **Planner**：返回一个 `DistributedShuffleJoinPlan`，包含：
   - 左右表信息（`customer`、`orders`）。
   - 别名信息（`c`、`o`）。
   - 连接键信息（`c_custkey`、`o_custkey`）。
   - `worker_join_sql`：Workers 运行的修改版查询，例如：`SELECT c.c_name, o.o_orderstatus FROM c_local AS c JOIN o_local AS o ON c.c_custkey = o.o_custkey;`
4. **Executor**：`execute` 调用 `_execute_join`。这开始一个三阶段流程：
   - **阶段 1：Map/分区**：
     - `Executor` 找到 `customer` 和 `orders` 的所有文件。
     - 为每个文件调用 `worker.partition_by_key.remote()`。
     - 每个 Worker 读取数据并根据 `HASH(join_key) % N` 将其分成 `N` 个分区。所有具有相同连接键的行保证进入相同 ID 的分区。
   - **阶段 2：Shuffle & Reduce/Join**：
     - `Executor` 从所有 Workers 收集所有分区（这是发生在协调器内存中的"shuffle"）。数据按分区 ID 分组。
     - 对于每个分区 ID `i`，`Executor` 获取所有左右分区，然后调用 `worker.join_partitions.remote()`。
     - 接收此任务的 Worker 将所有左分区连接成一个名为 `c_local` 的表，将所有右分区连接成一个名为 `o_local` 的表，然后在这些两个表上执行 `worker_join_sql`。
   - **阶段 3：最终化**：
     - `Executor` 收集每个 Worker 的连接结果。
     - 这些结果被连接成一个名为 `partial_results` 的最终表。
     - 如果原始查询有聚合、`ORDER BY` 或 `LIMIT` 子句，`Executor` 将对 `partial_results` 应用它们。
5. **Coordinator**：最终结果返回给用户。

---

## 2. 如何添加新功能（案例研究：广播连接）

此架构使添加功能变得容易。让我们看看如何添加 **广播连接** 策略，当一个表非常小时它很高效。

#### 步骤 1：定义新计划

打开 `quack_cluster/execution_plan.py` 并添加新类：

```python
class DistributedBroadcastJoinPlan(BasePlan):
    plan_type: Literal["broadcast_join"] = "broadcast_join"
    large_table_name: str
    small_table_name: str
    small_table_alias: str
    # 添加其他相关字段（连接键等）
```

#### 步骤 2：更新 Planner

打开 `quack_cluster/planner.py`。在 `create_plan` 方法中，在 `Shuffle Join` 逻辑之前添加新逻辑：

```python
# ... 在 create_plan 中，在 LocalExecutionPlan 检查之后
if join_expression := query_ast.find(exp.Join):
    # 检查表大小（如使用 'os.path.getsize'）
    is_left_small = check_if_table_is_small("left_table_name")

    if is_left_small:
        # 新逻辑：创建广播连接计划
        return DistributedBroadcastJoinPlan(
            # ... 填写所有必需字段 ...
        )

    # ... 现有的 Shuffle Join 逻辑 ...
```

#### 步骤 3：实现执行

打开 `quack_cluster/executor.py`。

1. 在 `execute` 方法中添加 `elif isinstance(plan, DistributedBroadcastJoinPlan):`。
2. 创建新方法 `async def _execute_broadcast_join(self, plan, con)`：
   - 将小表（`small_table_name`）读取到协调器内存中作为单个 Arrow 表。
   - 为大表的每个文件（`large_table_name`）创建一个 Worker。
   - 调用新的 Worker 方法（如 `worker.join_with_broadcast.remote()`）并将小 Arrow 表作为参数发送到*每个*调用。Ray 将高效地序列化和发送此对象。
   - 收集结果并执行任何最终聚合。

#### 步骤 4：添加 Worker 能力

打开 `quack_cluster/worker.py` 并添加新方法：

```python
def join_with_broadcast(self, large_table_file: str, broadcast_table: pa.Table, broadcast_alias: str, join_sql: str) -> pa.Table:
    # 注册从内存接收的小表
    self.con.register(broadcast_alias, broadcast_table)

    # 读取大文件并执行连接
    query = join_sql.format(large_table_file=large_table_file) # 根据需要修改 SQL
    return self.con.execute(query).fetch_arrow_table()
```

通过这四个步骤，您已经集成了一种新的连接策略，同时没有破坏现有流程。
