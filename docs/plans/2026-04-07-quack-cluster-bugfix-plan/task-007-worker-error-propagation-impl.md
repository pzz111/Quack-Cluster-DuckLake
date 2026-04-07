# Task 007: 错误处理 - Worker 错误传播实现

## 目标

修改 Worker 的错误处理逻辑，使用 WorkerError 异常类传播错误，而不是返回空表。

## BDD 场景

```gherkin
Feature: Worker 错误传播
  作为系统开发者
  我希望 Worker 错误被正确传播
  以便能够诊断和修复问题

  Scenario: Worker 查询失败时抛出异常
    Given Ray 集群正在运行
    And Worker 已初始化
    When Worker 执行无效查询
    Then Worker 应该抛出 WorkerError 异常
    And 异常应该包含错误类别
    And 异常应该包含原始查询
    And 异常应该包含文件路径
    And 异常不应该返回空表

  Scenario: 文件 I/O 错误分类为可重试
    Given Ray 集群正在运行
    And Worker 已初始化
    When Worker 尝试读取不存在的文件
    Then Worker 应该抛出 WorkerError
    And 错误类别应该是 "network"
    And 错误应该被标记为可重试

  Scenario: SQL 语法错误分类为不可重试
    Given Ray 集群正在运行
    And Worker 已初始化
    When Worker 执行语法错误的 SQL
    Then Worker 应该抛出 WorkerError
    And 错误类别应该是 "logic"
    And 错误应该被标记为不可重试
```

## 依赖

**depends-on**: ["001", "006"]

## 涉及文件

- `quack_cluster/worker.py` - 修改 query(), run_join_task(), partition_by_key(), join_partitions() 方法

## 实现步骤

### 1. 导入 WorkerError

在 [worker.py](quack_cluster/worker.py) 顶部添加：

```python
from .execution_plan import WorkerError, ErrorCategory
```

### 2. 修改 query() 方法

在 [worker.py:183-224](quack_cluster/worker.py#L183-L224) 修改：

```python
def query(self, sql_query: str, file_paths: List[str]) -> pa.Table:
    """执行数据扫描查询"""
    query_to_execute = ""
    try:
        formatted_paths = ", ".join([f"'{path}'" for path in file_paths])
        query_to_execute = sql_query.format(files=f"[{formatted_paths}]")
        result = self.con.execute(query_to_execute).fetch_arrow_table()
        
        # 空结果是合法的
        if result.num_rows == 0:
            logger.info(f"Query returned 0 rows (valid empty result)")
        
        return result
        
    except duckdb.IOException as e:
        # 文件 I/O 错误 - 可重试
        raise WorkerError(
            f"Failed to read data files: {e}",
            ErrorCategory.NETWORK,
            query_to_execute,
            file_paths,
            e
        )
    except duckdb.BinderException as e:
        # SQL 绑定错误 - 不可重试
        raise WorkerError(
            f"Invalid query binding: {e}",
            ErrorCategory.LOGIC,
            query_to_execute,
            file_paths,
            e
        )
    except duckdb.ParserException as e:
        # SQL 解析错误 - 不可重试
        raise WorkerError(
            f"Query parsing failed: {e}",
            ErrorCategory.LOGIC,
            query_to_execute,
            file_paths,
            e
        )
    except Exception as e:
        # 未知错误 - 保守起见不重试
        raise WorkerError(
            f"Worker query failed: {e}",
            ErrorCategory.LOGIC,
            query_to_execute,
            file_paths,
            e
        )
```

### 3. 修改 run_join_task() 方法

在 [worker.py:144-181](quack_cluster/worker.py#L144-L181) 修改：

```python
def run_join_task(self, join_sql: str, broadcast_table: pa.Table, broadcast_table_name: str) -> pa.Table:
    """执行广播连接任务"""
    try:
        print(f"✅ Worker 收到广播表 '{broadcast_table_name}'，共 {broadcast_table.num_rows} 行。")
        self.con.register(broadcast_table_name, broadcast_table)
        print(f"✅ Worker 已注册表 '{broadcast_table_name}'。")
        
        print(f"✅ Worker 执行 JOIN：{join_sql}")
        result = self.con.execute(join_sql).fetch_arrow_table()
        print(f"✅ Worker JOIN 产生 {result.num_rows} 行结果。")
        return result
        
    except duckdb.BinderException as e:
        raise WorkerError(
            f"JOIN binding failed: {e}",
            ErrorCategory.LOGIC,
            join_sql,
            None,
            e
        )
    except Exception as e:
        raise WorkerError(
            f"JOIN task failed: {e}",
            ErrorCategory.LOGIC,
            join_sql,
            None,
            e
        )
```

### 4. 修改 partition_by_key() 方法

添加错误处理：

```python
def partition_by_key(
    self,
    file_path: str,
    key_column: str,
    num_partitions: int,
    reader_function: str,
    where_sql: str = ""
) -> Dict[int, pa.Table]:
    """按连接键对数据进行分区（Map 阶段）"""
    try:
        logger.info(f"👷 Worker 正在对 '{file_path}' 进行分区，使用键：'{key_column}'...")
        
        if where_sql:
            logger.info(f"   - 应用过滤器：{where_sql}")
        
        base_query = f"""
        CREATE OR REPLACE TEMP VIEW partitioned_view AS
        SELECT *, HASH({key_column}) % {num_partitions} AS __partition_id
        FROM {reader_function}('{file_path}')
        {where_sql}
        """
        self.con.execute(base_query)
        
        partitions = {}
        for i in range(num_partitions):
            result = self.con.execute(
                f"SELECT * EXCLUDE (__partition_id) FROM partitioned_view WHERE __partition_id = {i}"
            ).fetch_arrow_table()
            
            if result.num_rows > 0:
                partitions[i] = result
        
        return partitions
        
    except duckdb.IOException as e:
        raise WorkerError(
            f"Failed to read file for partitioning: {e}",
            ErrorCategory.NETWORK,
            base_query,
            [file_path],
            e
        )
    except Exception as e:
        raise WorkerError(
            f"Partitioning failed: {e}",
            ErrorCategory.LOGIC,
            base_query,
            [file_path],
            e
        )
```

### 5. 修改 join_partitions() 方法

添加错误处理：

```python
def join_partitions(
    self,
    left_partitions: List[pa.Table],
    right_partitions: List[pa.Table],
    join_sql: str,
    left_alias: str,
    right_alias: str
) -> pa.Table:
    """在分区的数据上执行 JOIN（Reduce 阶段）"""
    try:
        print(f"🤝 Worker 正在连接分区...")
        
        left_table = pa.concat_tables(left_partitions) if left_partitions else pa.Table.from_pydict({})
        right_table = pa.concat_tables(right_partitions) if right_partitions else pa.Table.from_pydict({})
        
        print(f"   - 左侧有 {left_table.num_rows} 行。")
        print(f"   - 右侧有 {right_table.num_rows} 行。")
        
        if left_table.num_rows == 0 or right_table.num_rows == 0:
            # 空分区是合法的
            return pa.Table.from_pydict({})
        
        self.con.register(f"{left_alias}_local", left_table)
        self.con.register(f"{right_alias}_local", right_table)
        
        return self.con.execute(join_sql).fetch_arrow_table()
        
    except Exception as e:
        raise WorkerError(
            f"Partition join failed: {e}",
            ErrorCategory.LOGIC,
            join_sql,
            None,
            e
        )
```

## 验证

运行测试（应该通过）：

```bash
pytest tests/test_worker_error_propagation.py -v
```

预期输出：
```
PASSED test_worker_raises_error_on_invalid_query
PASSED test_worker_error_contains_context
PASSED test_io_error_is_retryable
PASSED test_logic_error_is_not_retryable
PASSED test_empty_result_does_not_raise_error
```

## 提交信息

```
fix: propagate Worker errors instead of returning empty tables (GREEN)

- Replace silent failures with WorkerError exceptions
- Classify errors by category (NETWORK, LOGIC, etc.)
- Include rich context (query, files, original error)
- Distinguish empty results from query failures
- Update all Worker methods: query, run_join_task, partition_by_key, join_partitions

Closes: Task 006 (tests now pass)
Related: docs/plans/2026-04-07-quack-cluster-code-review-and-improvements
```

## 注意事项

1. **错误分类准确性**: 确保 DuckDB 异常正确映射到 ErrorCategory
2. **空结果处理**: 空表是合法结果，不应该抛出异常
3. **上下文信息**: 所有异常包含足够的调试信息
4. **向后兼容**: Executor 需要更新以处理 WorkerError（在后续任务中）
