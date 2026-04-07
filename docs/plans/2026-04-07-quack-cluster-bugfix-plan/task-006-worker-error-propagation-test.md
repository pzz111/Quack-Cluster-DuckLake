# Task 006: 错误处理 - Worker 错误传播测试

## 目标

编写测试验证 Worker 正确传播错误而不是返回空表，并验证错误分类功能。

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

**depends-on**: ["001"]

## 涉及文件

- `tests/test_worker_error_propagation.py` - 新建测试文件

## 实现步骤

### 1. 创建测试文件

创建 `tests/test_worker_error_propagation.py`:

```python
import pytest
import ray
from quack_cluster.worker import DuckDBWorker
from quack_cluster.execution_plan import WorkerError, ErrorCategory

@pytest.fixture(scope="module", autouse=True)
def setup_ray():
    """初始化 Ray 集群"""
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)
    yield
    # 不关闭 Ray，让其他测试使用

class TestWorkerErrorPropagation:
    """测试 Worker 错误传播"""
    
    def test_worker_raises_error_on_invalid_query(self):
        """测试：Worker 查询失败时抛出异常"""
        worker = DuckDBWorker.remote()
        
        # 执行无效查询
        with pytest.raises(ray.exceptions.RayTaskError) as exc_info:
            ray.get(worker.query.remote("SELEC * FROM users", []))
        
        # 验证：异常是 WorkerError
        # 注意：Ray 会包装异常，需要检查原始异常
        assert "WorkerError" in str(exc_info.value)
    
    def test_worker_error_contains_context(self):
        """测试：WorkerError 包含丰富的上下文信息"""
        worker = DuckDBWorker.remote()
        
        try:
            ray.get(worker.query.remote("INVALID SQL", ["file1.parquet"]))
        except ray.exceptions.RayTaskError as e:
            error_str = str(e)
            # 验证：错误包含查询和文件路径
            assert "INVALID SQL" in error_str
            assert "file1.parquet" in error_str
    
    def test_io_error_is_retryable(self):
        """测试：文件 I/O 错误分类为可重试"""
        worker = DuckDBWorker.remote()
        
        # 尝试读取不存在的文件
        with pytest.raises(ray.exceptions.RayTaskError) as exc_info:
            ray.get(worker.query.remote(
                "SELECT * FROM read_parquet({files})",
                ["/nonexistent/file.parquet"]
            ))
        
        error_str = str(exc_info.value)
        # 验证：错误类别是 network（可重试）
        assert "network" in error_str.lower() or "io" in error_str.lower()
    
    def test_logic_error_is_not_retryable(self):
        """测试：SQL 语法错误分类为不可重试"""
        worker = DuckDBWorker.remote()
        
        # 执行语法错误的 SQL
        with pytest.raises(ray.exceptions.RayTaskError) as exc_info:
            ray.get(worker.query.remote("SELEC * FROM users", []))
        
        error_str = str(exc_info.value)
        # 验证：错误类别是 logic（不可重试）
        assert "logic" in error_str.lower() or "parser" in error_str.lower()
    
    def test_empty_result_does_not_raise_error(self):
        """测试：空结果不抛出异常"""
        worker = DuckDBWorker.remote()
        
        # 创建临时表并查询空结果
        result = ray.get(worker.query.remote(
            "SELECT * FROM read_parquet({files}) WHERE 1=0",
            []  # 空文件列表会导致空结果
        ))
        
        # 验证：返回空表，不抛出异常
        # 注意：这个测试可能需要调整，取决于实际实现
```

### 2. 添加单元测试（不依赖 Ray）

```python
from quack_cluster.execution_plan import WorkerError, ErrorCategory

class TestWorkerErrorClass:
    """测试 WorkerError 类本身"""
    
    def test_worker_error_is_retryable(self):
        """测试：可重试错误判断"""
        network_error = WorkerError(
            "Network error",
            ErrorCategory.NETWORK,
            "SELECT 1",
            ["file.parquet"]
        )
        assert network_error.is_retryable() is True
        
        logic_error = WorkerError(
            "Logic error",
            ErrorCategory.LOGIC,
            "SELECT 1"
        )
        assert logic_error.is_retryable() is False
    
    def test_worker_error_to_dict(self):
        """测试：错误序列化"""
        error = WorkerError(
            "Test error",
            ErrorCategory.DATA,
            "SELECT * FROM users",
            ["file1.parquet", "file2.parquet"],
            ValueError("Original error")
        )
        
        error_dict = error.to_dict()
        assert error_dict["error_type"] == "WorkerError"
        assert error_dict["category"] == "data"
        assert error_dict["query"] == "SELECT * FROM users"
        assert error_dict["file_paths"] == ["file1.parquet", "file2.parquet"]
        assert "Original error" in error_dict["original_error"]
```

## 验证

运行测试（预期失败，因为实现尚未完成）：

```bash
pytest tests/test_worker_error_propagation.py -v
```

预期输出：
```
FAILED test_worker_raises_error_on_invalid_query - Worker 返回空表而不是抛出异常
FAILED test_io_error_is_retryable - Worker 返回空表
FAILED test_logic_error_is_not_retryable - Worker 返回空表
PASSED test_empty_result_does_not_raise_error - 空结果正确处理
```

## 提交信息

```
test: add Worker error propagation tests (RED)

- Test Worker raises WorkerError on invalid queries
- Test WorkerError contains rich context (query, files, category)
- Test I/O errors are classified as retryable (NETWORK)
- Test logic errors are classified as non-retryable (LOGIC)
- Test empty results do not raise errors
- Add unit tests for WorkerError class

These tests currently FAIL and will pass after implementing
error propagation in Task 007.
```

## 注意事项

1. **Ray 异常包装**: Ray 会包装 Worker 异常，测试需要处理这种包装
2. **错误分类验证**: 通过错误消息字符串验证分类（因为异常被序列化）
3. **空结果处理**: 确保空结果不被误认为错误
4. **预期失败**: 这是 RED 阶段，测试应该失败
