# Task 013: 容错性 - 指数退避重试实现

## 目标

实现智能重试策略，使用指数退避算法，基于错误类别决定是否重试。

## BDD 场景

```gherkin
Feature: 智能重试策略
  作为系统管理员
  我希望系统使用指数退避重试
  以便更好地处理临时故障

  Scenario: 网络错误使用指数退避重试
    Given Ray 集群正在运行
    And Worker 任务配置为最多重试 3 次
    When Worker 任务因网络错误失败
    Then 系统应该在 1 秒后重试第 1 次
    And 系统应该在 2 秒后重试第 2 次
    And 系统应该在 4 秒后重试第 3 次
    And 延迟不应该超过最大延迟限制

  Scenario: 逻辑错误不应该重试
    Given Ray 集群正在运行
    And Worker 任务配置为最多重试 3 次
    When Worker 任务因 SQL 语法错误失败
    Then 系统不应该重试
    And 系统应该立即抛出异常
    And 异常应该包含原始错误信息
```

## 依赖

**depends-on**: ["001", "012"]

## 涉及文件

- `quack_cluster/executor.py` - 修改 `_execute_with_retries` 方法
- `quack_cluster/settings.py` - 添加重试配置

## 实现步骤

### 1. 添加重试配置到 settings.py

```python
class Settings(BaseSettings):
    # 现有配置...
    
    # 重试配置
    MAX_RETRIES: int = _yaml_config.get("max_retries", 3)
    RETRY_BASE_DELAY: float = _yaml_config.get("retry_base_delay", 1.0)
    RETRY_MAX_DELAY: float = _yaml_config.get("retry_max_delay", 30.0)
    RETRY_EXPONENTIAL_BASE: float = _yaml_config.get("retry_exponential_base", 2.0)
```

### 2. 添加错误分类函数到 executor.py

```python
from .execution_plan import WorkerError, ErrorCategory

@staticmethod
def _should_retry(error: Exception) -> bool:
    """判断错误是否应该重试"""
    if isinstance(error, WorkerError):
        # 只重试网络和资源错误
        return error.category in [ErrorCategory.NETWORK, ErrorCategory.RESOURCE]
    
    if isinstance(error, asyncio.TimeoutError):
        # 超时可以重试
        return True
    
    # 其他错误不重试
    return False

@staticmethod
def _calculate_backoff_delay(retry_count: int) -> float:
    """计算指数退避延迟"""
    delay = settings.RETRY_BASE_DELAY * (
        settings.RETRY_EXPONENTIAL_BASE ** retry_count
    )
    return min(delay, settings.RETRY_MAX_DELAY)
```

### 3. 修改 _execute_with_retries 方法

在 [executor.py:62-132](quack_cluster/executor.py#L62-L132) 修改：

```python
@staticmethod
async def _execute_with_retries(
    initial_inputs: List[Any],
    task_function: Callable[[Any], Coroutine]
) -> List[Any]:
    """带智能重试的任务执行器"""
    inputs_to_process = list(initial_inputs)
    all_results = []
    retries = 0
    
    while inputs_to_process and retries <= settings.MAX_RETRIES:
        if retries > 0:
            # 计算指数退避延迟
            delay = Executor._calculate_backoff_delay(retries)
            logger.warning(
                f"🔁 重试 {len(inputs_to_process)} 个失败的任务..."
                f"（第 {retries} 次重试 / 最多 {settings.MAX_RETRIES} 次，"
                f"延迟 {delay:.1f}s）"
            )
            await asyncio.sleep(delay)
        
        # 创建任务
        tasks = [task_function(inp) for inp in inputs_to_process]
        task_to_input_map = {tasks[i]: inputs_to_process[i] for i in range(len(tasks))}
        
        # 并行执行所有任务
        task_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 分离成功和失败的任务
        failed_inputs = []
        for i, res in enumerate(task_results):
            if isinstance(res, Exception):
                failed_input = task_to_input_map[tasks[i]]
                
                # 检查是否应该重试
                if Executor._should_retry(res):
                    logger.error(f"‼️ 可重试错误：{res}")
                    failed_inputs.append(failed_input)
                else:
                    logger.error(f"‼️ 不可重试错误：{res}")
                    # 立即失败，不再重试
                    raise res
            else:
                # 任务成功
                all_results.append(res)
        
        # 更新待处理任务列表
        inputs_to_process = failed_inputs
        if inputs_to_process:
            retries += 1
    
    # 如果还有失败的任务，说明超过了最大重试次数
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
# 重试配置
max_retries: 3
retry_base_delay: 1.0       # 基础延迟（秒）
retry_max_delay: 30.0       # 最大延迟（秒）
retry_exponential_base: 2.0 # 指数基数
```

## 验证

运行测试（应该通过）：

```bash
pytest tests/test_exponential_backoff.py -v
```

预期输出：
```
PASSED test_network_error_retries_with_backoff
PASSED test_logic_error_does_not_retry
PASSED test_retry_limit_reached
PASSED test_backoff_delay_calculation
```

## 提交信息

```
feat: implement exponential backoff retry strategy (GREEN)

- Add retry configuration (base delay, max delay, exponential base)
- Implement _should_retry() to classify retryable errors
- Implement _calculate_backoff_delay() for exponential backoff
- Update _execute_with_retries() to use intelligent retry logic
- Only retry NETWORK and RESOURCE errors, fail fast on LOGIC errors
- Add retry configuration to config.yaml

Closes: Task 012 (tests now pass)
Related: docs/plans/2026-04-07-quack-cluster-code-review-and-improvements
```

## 注意事项

1. **延迟计算**: 使用 `min(base * (exp_base ^ retry), max_delay)` 防止延迟过长
2. **错误分类**: 依赖 WorkerError 的 category 字段
3. **立即失败**: 不可重试错误立即抛出，不浪费时间
4. **配置灵活性**: 所有重试参数可通过 config.yaml 配置
