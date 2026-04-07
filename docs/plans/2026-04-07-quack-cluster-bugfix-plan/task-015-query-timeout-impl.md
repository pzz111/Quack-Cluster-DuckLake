# Task 015: 容错性 - 查询超时实现

## 目标

实现查询级别和 Worker 任务级别的超时机制，防止资源被长时间占用。

## BDD 场景

```gherkin
Feature: 查询超时机制
  作为系统管理员
  我希望查询有超时保护
  以便避免资源被长时间占用

  Scenario: 查询超过超时限制被终止
    Given 系统配置查询超时为 300 秒
    When 用户提交一个需要 400 秒的查询
    Then 查询应该在 300 秒后被终止
    And API 应该返回 408 超时错误
    And 所有相关资源应该被清理

  Scenario: Worker 任务超时被终止
    Given 系统配置 Worker 任务超时为 120 秒
    When Worker 任务执行超过 120 秒
    Then 任务应该被终止
    And Executor 应该收到超时异常
    And 任务应该被标记为失败

  Scenario: 快速查询不受超时影响
    Given 系统配置查询超时为 300 秒
    When 用户提交一个需要 10 秒的查询
    Then 查询应该正常完成
    And 不应该触发超时机制
```

## 依赖

**depends-on**: ["014"]

## 涉及文件

- `quack_cluster/settings.py` - 添加超时配置
- `quack_cluster/coordinator.py` - 添加查询级别超时
- `quack_cluster/executor.py` - 添加 Worker 任务超时

## 实现步骤

### 1. 添加超时配置到 settings.py

```python
class Settings(BaseSettings):
    # 现有配置...
    
    # 超时配置
    QUERY_TIMEOUT_SECONDS: int = _yaml_config.get("query_timeout_seconds", 300)
    WORKER_TASK_TIMEOUT_SECONDS: int = _yaml_config.get("worker_task_timeout_seconds", 120)
```

### 2. 添加查询超时到 coordinator.py

在 [coordinator.py:418-496](quack_cluster/coordinator.py#L418-L496) 修改：

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
        
        # 使用上下文管理器和超时保护
        async with duckdb.connect(database=':memory:') as main_con:
            try:
                # 添加查询级别超时
                final_arrow_table = await asyncio.wait_for(
                    resolve_and_execute(parsed_query, main_con),
                    timeout=settings.QUERY_TIMEOUT_SECONDS
                )
            except asyncio.TimeoutError:
                raise HTTPException(
                    status_code=status.HTTP_408_REQUEST_TIMEOUT,
                    detail=f"查询超过超时限制 {settings.QUERY_TIMEOUT_SECONDS} 秒"
                )
        
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

### 3. 添加 Worker 任务超时到 executor.py

在 `_execute_with_retries` 方法中添加超时：

```python
@staticmethod
async def _execute_with_retries(
    initial_inputs: List[Any],
    task_function: Callable[[Any], Coroutine]
) -> List[Any]:
    """带智能重试和超时的任务执行器"""
    inputs_to_process = list(initial_inputs)
    all_results = []
    retries = 0
    
    while inputs_to_process and retries <= settings.MAX_RETRIES:
        if retries > 0:
            delay = Executor._calculate_backoff_delay(retries)
            logger.warning(
                f"🔁 重试 {len(inputs_to_process)} 个失败的任务..."
                f"（第 {retries} 次重试 / 最多 {settings.MAX_RETRIES} 次，"
                f"延迟 {delay:.1f}s）"
            )
            await asyncio.sleep(delay)
        
        # 为每个任务添加超时
        tasks = [
            asyncio.wait_for(
                task_function(inp),
                timeout=settings.WORKER_TASK_TIMEOUT_SECONDS
            )
            for inp in inputs_to_process
        ]
        
        task_to_input_map = {tasks[i]: inputs_to_process[i] for i in range(len(tasks))}
        task_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        failed_inputs = []
        for i, res in enumerate(task_results):
            if isinstance(res, Exception):
                failed_input = task_to_input_map[tasks[i]]
                
                # 超时错误可以重试
                if isinstance(res, asyncio.TimeoutError):
                    logger.error(f"‼️ Worker 任务超时：{failed_input}")
                    failed_inputs.append(failed_input)
                elif Executor._should_retry(res):
                    logger.error(f"‼️ 可重试错误：{res}")
                    failed_inputs.append(failed_input)
                else:
                    logger.error(f"‼️ 不可重试错误：{res}")
                    raise res
            else:
                all_results.append(res)
        
        inputs_to_process = failed_inputs
        if inputs_to_process:
            retries += 1
    
    if inputs_to_process:
        error_message = (
            f"🛑 查询在 {settings.MAX_RETRIES} 次重试后仍然失败。"
            f"无法处理的输入：{inputs_to_process}"
        )
        logger.critical(error_message)
        raise Exception(error_message)
    
    return all_results
```

### 4. 更新 config.yaml

```yaml
# 超时配置
query_timeout_seconds: 300      # 查询级别超时（5 分钟）
worker_task_timeout_seconds: 120 # Worker 任务超时（2 分钟）
```

## 验证

运行测试（应该通过）：

```bash
pytest tests/test_query_timeout.py -v
```

预期输出：
```
PASSED test_query_timeout_terminates_long_query
PASSED test_worker_task_timeout
PASSED test_fast_query_not_affected_by_timeout
```

## 提交信息

```
feat: add query and worker task timeout protection (GREEN)

- Add QUERY_TIMEOUT_SECONDS and WORKER_TASK_TIMEOUT_SECONDS config
- Wrap resolve_and_execute with asyncio.wait_for in coordinator
- Wrap worker tasks with asyncio.wait_for in executor
- Return 408 status code for query timeouts
- Treat timeout errors as retryable in executor
- Add timeout configuration to config.yaml

Closes: Task 014 (tests now pass)
Related: docs/plans/2026-04-07-quack-cluster-code-review-and-improvements
```

## 注意事项

1. **超时值选择**: 查询超时 > Worker 任务超时，避免误报
2. **资源清理**: asyncio.wait_for 会取消任务，确保资源释放
3. **超时重试**: 超时错误被视为可重试（可能是临时负载高）
4. **用户体验**: 返回 408 状态码，明确告知超时原因
