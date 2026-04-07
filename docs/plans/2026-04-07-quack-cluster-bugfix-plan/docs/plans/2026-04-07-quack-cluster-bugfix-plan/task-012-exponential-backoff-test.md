# Task 012: 容错性 - 指数退避重试测试

## 目标

编写测试验证指数退避重试策略和智能错误分类。

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

  Scenario: 重试次数达到上限后失败
    Given Ray 集群正在运行
    And Worker 任务配置为最多重试 3 次
    When Worker 任务持续因网络错误失败
    And 已经重试 3 次
    Then 系统应该抛出最终失败异常
    And 异常应该包含所有重试历史
    And 异常应该包含失败的输入
```

## 依赖

**depends-on**: ["001"]

## 涉及文件

- `tests/test_exponential_backoff.py` - 新建测试文件

## 实现步骤

创建 `tests/test_exponential_backoff.py`:

```python
import pytest
import asyncio
import time
from unittest.mock import Mock, AsyncMock
from quack_cluster.executor import Executor
from quack_cluster.execution_plan import WorkerError, ErrorCategory

@pytest.mark.asyncio
class TestExponentialBackoff:
    """测试指数退避重试策略"""
    
    async def test_network_error_retries_with_backoff(self):
        """测试：网络错误使用指数退避重试"""
        call_times = []
        attempt_count = [0]
        
        async def failing_task(inp):
            call_times.append(time.time())
            attempt_count[0] += 1
            if attempt_count[0] <= 3:
                raise WorkerError(
                    "Network error",
                    ErrorCategory.NETWORK,
                    "SELECT 1",
                    ["file.parquet"]
                )
            return "success"
        
        start_time = time.time()
        result = await Executor._execute_with_retries(["input"], failing_task)
        
        # 验证：重试了 3 次
        assert attempt_count[0] == 4  # 初始 + 3 次重试
        
        # 验证：延迟符合指数退避
        if len(call_times) >= 4:
            delay1 = call_times[1] - call_times[0]
            delay2 = call_times[2] - call_times[1]
            delay3 = call_times[3] - call_times[2]
            
            assert 0.9 < delay1 < 1.5  # ~1 秒
            assert 1.8 < delay2 < 2.5  # ~2 秒
            assert 3.5 < delay3 < 5.0  # ~4 秒
    
    async def test_logic_error_does_not_retry(self):
        """测试：逻辑错误不应该重试"""
        attempt_count = [0]
        
        async def failing_task(inp):
            attempt_count[0] += 1
            raise WorkerError(
                "Logic error",
                ErrorCategory.LOGIC,
                "INVALID SQL",
                []
            )
        
        with pytest.raises(WorkerError) as exc_info:
            await Executor._execute_with_retries(["input"], failing_task)
        
        # 验证：只尝试了 1 次，没有重试
        assert attempt_count[0] == 1
        assert exc_info.value.category == ErrorCategory.LOGIC
    
    async def test_retry_limit_reached(self):
        """测试：重试次数达到上限后失败"""
        attempt_count = [0]
        
        async def always_failing_task(inp):
            attempt_count[0] += 1
            raise WorkerError(
                "Network error",
                ErrorCategory.NETWORK,
                "SELECT 1",
                ["file.parquet"]
            )
        
        with pytest.raises(Exception) as exc_info:
            await Executor._execute_with_retries(["input"], always_failing_task)
        
        # 验证：尝试了初始 + MAX_RETRIES 次
        assert attempt_count[0] == 4  # 1 + 3 retries
        assert "重试后仍然失败" in str(exc_info.value)
    
    async def test_backoff_delay_calculation(self):
        """测试：退避延迟计算"""
        # 测试延迟计算函数
        delay1 = Executor._calculate_backoff_delay(1)
        delay2 = Executor._calculate_backoff_delay(2)
        delay3 = Executor._calculate_backoff_delay(3)
        
        # 验证：指数增长
        assert delay1 == 2.0  # 1.0 * 2^1
        assert delay2 == 4.0  # 1.0 * 2^2
        assert delay3 == 8.0  # 1.0 * 2^3
        
        # 验证：不超过最大延迟
        delay_large = Executor._calculate_backoff_delay(10)
        assert delay_large <= 30.0  # MAX_DELAY
```

## 验证

运行测试（预期失败）:

```bash
pytest tests/test_exponential_backoff.py -v
```

## 提交信息

```
test: add exponential backoff retry tests (RED)

- Test network errors retry with exponential backoff
- Test logic errors do not retry
- Test retry limit is enforced
- Test backoff delay calculation

These tests currently FAIL and will pass after implementing
exponential backoff in Task 013.
```
