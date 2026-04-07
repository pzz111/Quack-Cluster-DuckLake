# Task 017: 文件验证 - 文件存在性检查实现

## 目标

实现文件操作前的存在性检查，防止文件不存在导致的错误。

## BDD 场景

```gherkin
Feature: 文件验证
  Scenario: 计算表大小前验证文件存在
    Given Planner 发现了表 "users" 的文件列表
    When Planner 计算表大小
    And 其中一个文件在计算前被删除
    Then Planner 应该抛出 FileNotFoundError
    And 错误应该列出缺失的文件
```

## 依赖

**depends-on**: ["016"]

## 涉及文件

- `quack_cluster/planner.py` - 修改 `_get_table_size_mb` 方法

## 实现步骤

在 [planner.py:98-113](quack_cluster/planner.py#L98-L113) 修改：

```python
@staticmethod
def _get_table_size_mb(files: List[str]) -> float:
    """计算表的总大小（以 MB 为单位）
    
    验证所有文件存在后再计算大小。
    """
    total_bytes = 0
    missing_files = []
    
    for f in files:
        try:
            total_bytes += os.path.getsize(f)
        except FileNotFoundError:
            missing_files.append(f)
    
    if missing_files:
        raise FileNotFoundError(
            f"以下文件在发现后被删除: {missing_files}"
        )
    
    return total_bytes / (1024 * 1024)
```

## 验证

运行测试（应该通过）:

```bash
pytest tests/test_file_validation.py -v
```

## 提交信息

```
fix: add file existence validation before operations (GREEN)

- Check file existence before calculating size
- Raise FileNotFoundError with missing file list
- Prevent cryptic errors from missing files

Closes: Task 016 (tests now pass)
```
