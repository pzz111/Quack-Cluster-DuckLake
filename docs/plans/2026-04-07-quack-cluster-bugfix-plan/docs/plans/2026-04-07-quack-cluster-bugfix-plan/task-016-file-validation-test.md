# Task 016: 文件验证 - 文件存在性检查测试

## 目标

编写测试验证文件操作前的存在性检查。

## BDD 场景

```gherkin
Feature: 文件验证
  作为系统开发者
  我希望文件操作前进行验证
  以便避免文件不存在导致的错误

  Scenario: 计算表大小前验证文件存在
    Given Planner 发现了表 "users" 的文件列表
    When Planner 计算表大小
    And 其中一个文件在计算前被删除
    Then Planner 应该抛出 FileNotFoundError
    And 错误应该列出缺失的文件
    And 错误应该包含清晰的错误消息
```

## 依赖

**depends-on**: []

## 涉及文件

- `tests/test_file_validation.py` - 新建测试文件

## 实现步骤

创建 `tests/test_file_validation.py`:

```python
import pytest
import os
import tempfile
from quack_cluster.planner import Planner

class TestFileValidation:
    """测试文件验证"""
    
    def test_get_table_size_validates_file_existence(self):
        """测试：计算表大小前验证文件存在"""
        # 创建临时文件
        with tempfile.NamedTemporaryFile(delete=False) as f:
            temp_file = f.name
            f.write(b"test data")
        
        try:
            # 删除文件
            os.unlink(temp_file)
            
            # 尝试计算大小
            with pytest.raises(FileNotFoundError) as exc_info:
                Planner._get_table_size_mb([temp_file])
            
            # 验证：错误消息包含文件名
            assert temp_file in str(exc_info.value)
        finally:
            # 清理
            if os.path.exists(temp_file):
                os.unlink(temp_file)
    
    def test_get_table_size_succeeds_when_files_exist(self):
        """测试：所有文件存在时正常计算大小"""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            temp_file = f.name
            f.write(b"test data" * 1000)
        
        try:
            size_mb = Planner._get_table_size_mb([temp_file])
            assert size_mb > 0
        finally:
            os.unlink(temp_file)
```

## 验证

运行测试（预期失败）:

```bash
pytest tests/test_file_validation.py -v
```

## 提交信息

```
test: add file validation tests (RED)

- Test file existence check before size calculation
- Test error message includes missing file names
- Test successful size calculation when files exist

These tests currently FAIL and will pass after implementing
file validation in Task 017.
```
