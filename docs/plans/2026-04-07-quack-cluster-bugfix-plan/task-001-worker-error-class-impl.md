# Task 001: 创建 WorkerError 异常类

## 目标

创建结构化的 `WorkerError` 异常类，支持错误分类和丰富的上下文信息，为后续的智能重试和错误处理奠定基础。

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
```

## 依赖

**depends-on**: []

## 涉及文件

- `quack_cluster/execution_plan.py` - 添加 ErrorCategory 枚举和 WorkerError 类

## 实现步骤

### 1. 创建错误分类枚举

在 `quack_cluster/execution_plan.py` 中添加：

```python
from enum import Enum

class ErrorCategory(str, Enum):
    """错误分类"""
    NETWORK = "network"      # 网络/IO 错误 - 可重试
    RESOURCE = "resource"    # 资源不足 - 可重试
    LOGIC = "logic"          # 逻辑错误 - 不可重试
    DATA = "data"            # 数据错误 - 不可重试
```

### 2. 创建 WorkerError 异常类

```python
from typing import Optional, List

class WorkerError(Exception):
    """Worker 执行错误
    
    包含错误分类和丰富的上下文信息，用于：
    - 智能重试决策（基于 category）
    - 错误诊断（query, file_paths, original_error）
    - 结构化错误响应
    """
    
    def __init__(
        self,
        message: str,
        category: ErrorCategory,
        query: str,
        file_paths: Optional[List[str]] = None,
        original_error: Optional[Exception] = None
    ):
        self.message = message
        self.category = category
        self.query = query
        self.file_paths = file_paths or []
        self.original_error = original_error
        super().__init__(message)
    
    def to_dict(self) -> dict:
        """转换为字典用于序列化"""
        return {
            "error_type": "WorkerError",
            "message": self.message,
            "category": self.category.value,
            "query": self.query,
            "file_paths": self.file_paths,
            "original_error": str(self.original_error) if self.original_error else None
        }
    
    def is_retryable(self) -> bool:
        """判断错误是否可重试"""
        return self.category in [ErrorCategory.NETWORK, ErrorCategory.RESOURCE]
```

## 验证

### 单元测试

创建 `tests/test_worker_error.py`:

```python
import pytest
from quack_cluster.execution_plan import WorkerError, ErrorCategory

def test_worker_error_creation():
    """测试 WorkerError 创建"""
    error = WorkerError(
        "Test error",
        ErrorCategory.NETWORK,
        "SELECT * FROM users",
        ["file1.parquet"],
        ValueError("Original error")
    )
    
    assert error.message == "Test error"
    assert error.category == ErrorCategory.NETWORK
    assert error.query == "SELECT * FROM users"
    assert error.file_paths == ["file1.parquet"]
    assert isinstance(error.original_error, ValueError)

def test_worker_error_is_retryable():
    """测试可重试判断"""
    network_error = WorkerError("Network error", ErrorCategory.NETWORK, "SELECT 1")
    assert network_error.is_retryable() is True
    
    logic_error = WorkerError("Logic error", ErrorCategory.LOGIC, "SELECT 1")
    assert logic_error.is_retryable() is False

def test_worker_error_to_dict():
    """测试序列化"""
    error = WorkerError(
        "Test error",
        ErrorCategory.DATA,
        "SELECT * FROM users",
        ["file1.parquet"]
    )
    
    error_dict = error.to_dict()
    assert error_dict["error_type"] == "WorkerError"
    assert error_dict["category"] == "data"
    assert error_dict["query"] == "SELECT * FROM users"
```

运行测试：
```bash
pytest tests/test_worker_error.py -v
```

## 提交信息

```
feat: add WorkerError exception class with error categorization

- Add ErrorCategory enum (NETWORK, RESOURCE, LOGIC, DATA)
- Add WorkerError class with rich context (query, files, original error)
- Add is_retryable() method for intelligent retry decisions
- Add to_dict() for structured error responses
- Add unit tests for error creation and categorization

This provides the foundation for intelligent error handling and retry logic.
```

## 注意事项

1. **错误分类准确性**: 确保 ErrorCategory 的分类清晰且互斥
2. **序列化安全**: to_dict() 必须处理所有可能的 None 值
3. **向后兼容**: 不影响现有的异常处理代码
