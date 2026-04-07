# Task 014: 容错性 - 查询超时测试

## 目标

编写测试验证查询和 Worker 任务的超时机制。

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
```

## 依赖

**depends-on**: []

## 涉及文件

- `tests/test_query_timeout.py` - 新建测试文件

## 实现步骤

创建 `tests/test_query_timeout.py`:

```python
import pytest
import asyncio
from fastapi.testclient import TestClient
from quack_cluster.coordinator import app

@pytest.fixture
def api_client():
    with TestClient(app) as client:
        yield client

class TestQueryTimeout:
    """测试查询超时机制"""
    
    def test_query_timeout_terminates_long_query(self, api_client, monkeypatch):
        """测试：查询超过超时限制被终止"""
        # 设置短超时用于测试
        monkeypatch.setattr("quack_cluster.settings.settings.QUERY_TIMEOUT_SECONDS", 1)
        
        # 提交一个会超时的查询（模拟长时间运行）
        # 注意：实际测试中需要构造一个真正慢的查询
        response = api_client.post(
            "/query",
            json={"sql": "SELECT * FROM large_table"}  # 假设这是慢查询
        )
        
        # 验证：返回 408 超时错误
        assert response.status_code == 408
        assert "超时" in response.json()["detail"]
    
    def test_fast_query_not_affected_by_timeout(self, api_client):
        """测试：快速查询不受超时影响"""
        response = api_client.post(
            "/query",
            json={"sql": "SELECT 1"}
        )
        
        # 验证：查询正常完成
        assert response.status_code == 200
```

## 验证

运行测试（预期失败）:

```bash
pytest tests/test_query_timeout.py -v
```

## 提交信息

```
test: add query timeout tests (RED)

- Test query timeout terminates long queries
- Test fast queries not affected by timeout
- Test 408 status code returned on timeout

These tests currently FAIL and will pass after implementing
timeout mechanism in Task 015.
```
