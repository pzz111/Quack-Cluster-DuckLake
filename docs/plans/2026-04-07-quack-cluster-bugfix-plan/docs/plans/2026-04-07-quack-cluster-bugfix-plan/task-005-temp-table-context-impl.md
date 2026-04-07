# Task 005: 资源管理 - 临时表上下文管理器实现

## 目标

实现临时表的上下文管理器，确保所有临时表在使用后自动注销。

## BDD 场景

```gherkin
Feature: 临时表内存管理
  作为系统开发者
  我希望临时表在使用后自动注销
  以便避免内存泄漏

  Scenario: CTE 临时表在使用后注销
    Given 系统启动并准备接收查询
    When 用户提交包含 CTE 的查询
    And 查询成功执行
    Then CTE 临时表应该被注销
```

## 依赖

**depends-on**: ["004"]

## 涉及文件

- `quack_cluster/coordinator.py` - 添加 temporary_table 上下文管理器

## 实现步骤

### 1. 创建 temporary_table 上下文管理器

在 [coordinator.py](quack_cluster/coordinator.py) 顶部添加：

```python
from contextlib import contextmanager

@contextmanager
def temporary_table(con: duckdb.DuckDBPyConnection, name: str, table: pa.Table):
    """临时表上下文管理器
    
    自动注册和注销临时表，确保资源清理。
    """
    try:
        con.register(name, table)
        yield
    finally:
        try:
            con.unregister(name)
        except Exception as e:
            logger.warning(f"Failed to unregister table {name}: {e}")
```

### 2. 修改 resolve_and_execute 中的 CTE 处理

在 [coordinator.py:280-293](quack_cluster/coordinator.py#L280-L293) 修改：

```python
if with_clause := query_ast.args.get('with'):
    query_body = query_ast.copy()
    query_body.set('with', None)
    
    for cte in with_clause.expressions:
        logger.info(f"➡️ 正在解析 CTE：'{cte.alias}'")
        cte_result_table = await resolve_and_execute(cte.this, con)
        
        # 使用上下文管理器自动注销
        with temporary_table(con, cte.alias, cte_result_table):
            logger.info(f"✅ CTE '{cte.alias}' 已注册为内存表。")
            # CTE 在此作用域内可用
            pass
    
    return await resolve_and_execute(query_body, con)
```

### 3. 修改子查询处理

在 [coordinator.py:330-351](quack_cluster/coordinator.py#L330-L351) 修改：

```python
for subquery in query_ast.find_all(exp.Subquery):
    subquery_alias = subquery.alias_or_name or f"_subquery_{uuid.uuid4().hex[:8]}"
    logger.info(f"➡️ 正在解析子查询：'{subquery_alias}'")
    
    subquery_result_table = await resolve_and_execute(subquery.this, con)
    
    if isinstance(subquery.parent, (exp.From, exp.Join)):
        # 使用上下文管理器
        with temporary_table(con, subquery_alias, subquery_result_table):
            subquery.replace(exp.to_table(subquery_alias))
    elif isinstance(subquery.parent, exp.In):
        # IN 子句处理保持不变
        if subquery_result_table.num_columns > 0 and subquery_result_table.num_rows > 0:
            values = subquery_result_table.columns[0].to_pylist()
            literal_expressions = [
                exp.Literal.string(v) if isinstance(v, str) else exp.Literal.number(v) 
                for v in values
            ]
            subquery.replace(exp.Tuple(expressions=literal_expressions))
        else:
            subquery.replace(exp.Tuple(expressions=[exp.Null()]))
```

## 验证

运行测试（应该通过）：

```bash
pytest tests/test_resource_management.py::TestTemporaryTableManagement -v
```

## 提交信息

```
fix: use context manager for temporary tables (GREEN)

- Add temporary_table context manager
- Wrap CTE registration with context manager
- Wrap subquery registration with context manager
- Ensures automatic table cleanup even on exceptions

Closes: Task 004 (tests now pass)
```

## 注意事项

1. **异常安全**: finally 块确保即使发生异常也会注销表
2. **日志记录**: 注销失败时记录警告但不中断执行
3. **作用域**: 临时表只在上下文管理器作用域内可用
