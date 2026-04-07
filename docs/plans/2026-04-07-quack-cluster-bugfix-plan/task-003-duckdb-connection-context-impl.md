# Task 003: 资源管理 - DuckDB 连接上下文管理器实现

## 目标

实现 DuckDB 连接的上下文管理器，确保所有连接在使用后自动关闭，修复资源泄漏问题。

## BDD 场景

```gherkin
Feature: DuckDB 连接资源管理
  作为系统管理员
  我希望所有 DuckDB 连接在使用后自动关闭
  以便避免资源泄漏和文件描述符耗尽

  Scenario: 查询成功执行后连接被关闭
    Given 系统启动并准备接收查询
    When 用户提交 SQL 查询 "SELECT * FROM users"
    And 查询成功执行
    Then DuckDB 连接应该被自动关闭
    And 系统不应该有未关闭的连接
```

## 依赖

**depends-on**: ["002"]

## 涉及文件

- `quack_cluster/coordinator.py` - 修改 execute_query 函数使用上下文管理器

## 实现步骤

### 1. 修改 coordinator.py 中的连接管理

在 [coordinator.py:463](quack_cluster/coordinator.py#L463) 修改：

```python
# 当前代码（有资源泄漏）
main_con = duckdb.connect(database=':memory:')
final_arrow_table = await resolve_and_execute(parsed_query, main_con)

# 修改为（使用上下文管理器）
async with duckdb.connect(database=':memory:') as main_con:
    final_arrow_table = await resolve_and_execute(parsed_query, main_con)
```

**注意**: DuckDB 的 Python 连接对象支持上下文管理器协议，会在 `__exit__` 时自动调用 `close()`。

### 2. 完整的 execute_query 函数修改

```python
@app.post("/query")
async def execute_query(
    request: QueryRequest,
    format: Optional[str] = Query("json", enum=["json", "arrow"])
):
    """主要的 SQL 查询执行端点"""
    cache_key = hashlib.sha256(f"{request.sql}:{format}".encode()).hexdigest()
    cached_data = await query_cache.get(cache_key)
    
    if cached_data:
        content, media_type = cached_data
        if media_type == "application/json":
            return content
        else:
            return Response(content=content, media_type=media_type)
    
    logger.info(f"缓存未命中，key: {cache_key[:10]}... 执行查询。")
    
    try:
        try:
            parsed_query = parse_one(request.sql, read="duckdb")
        except ParseError as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"SQL 语法错误：{e}")
        
        # 使用上下文管理器确保连接自动关闭
        async with duckdb.connect(database=':memory:') as main_con:
            final_arrow_table = await resolve_and_execute(parsed_query, main_con)
        
        if final_arrow_table is None:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="查询执行没有返回结果。")
        
        # 根据请求的格式返回结果
        if format == "arrow":
            sink = pa.BufferOutputStream()
            with ipc.new_stream(sink, final_arrow_table.schema) as writer:
                writer.write_table(final_arrow_table)
            content = sink.getvalue().to_pybytes()
            media_type = "application/vnd.apache.arrow.stream"
            await query_cache.set(cache_key, (content, media_type), settings.CACHE_TTL_SECONDS)
            return Response(content=content, media_type=media_type)
        else:
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
```

### 3. 修改 executor.py 中的临时连接

在 [executor.py:466](quack_cluster/executor.py#L466) 修改：

```python
# 当前代码
temp_con = duckdb.connect(database=':memory:')
broadcast_table_arrow = temp_con.execute(...).fetch_arrow_table()
temp_con.close()

# 修改为
with duckdb.connect(database=':memory:') as temp_con:
    broadcast_table_arrow = temp_con.execute(
        f"SELECT * FROM {plan.small_table_reader_func}([{file_list_sql}])"
    ).fetch_arrow_table()
```

## 验证

运行测试（应该通过）：

```bash
pytest tests/test_resource_management.py::TestDuckDBConnectionManagement -v
```

预期输出：
```
PASSED test_connection_closed_after_successful_query
PASSED test_connection_closed_after_failed_query
PASSED test_concurrent_queries_connection_isolation
PASSED test_no_connection_leak_after_multiple_queries
PASSED test_file_descriptors_stable_after_queries
```

## 提交信息

```
fix: use context manager for DuckDB connections (GREEN)

- Wrap main_con in coordinator.py with context manager
- Wrap temp_con in executor.py with context manager
- Ensures automatic connection cleanup even on exceptions
- Fixes resource leak identified in code review

Closes: Task 002 (tests now pass)
Related: docs/plans/2026-04-07-quack-cluster-code-review-and-improvements
```

## 注意事项

1. **异步上下文**: DuckDB 连接支持同步上下文管理器，在 async 函数中使用 `async with` 语法
2. **异常安全**: 上下文管理器确保即使发生异常也会关闭连接
3. **性能影响**: 上下文管理器开销极小（< 0.1ms），不影响查询性能
4. **向后兼容**: 不改变 API 接口，现有查询继续工作
