# Task 019: 集成测试 - 端到端错误处理验证

## 目标

编写端到端集成测试验证错误处理修复的完整流程。

## BDD 场景

综合验证特性 2（错误处理）的所有场景。

## 依赖

**depends-on**: ["007", "009"]

## 涉及文件

- `tests/test_integration_error_handling.py` - 新建测试文件

## 实现步骤

创建 `tests/test_integration_error_handling.py`:

```python
import pytest
from fastapi.testclient import TestClient
from quack_cluster.coordinator import app

@pytest.fixture
def api_client():
    with TestClient(app) as client:
        yield client

class TestErrorHandlingIntegration:
    """端到端错误处理集成测试"""
    
    def test_end_to_end_error_propagation(self, api_client):
        """测试：完整的错误传播流程"""
        # 执行会导致 Worker 错误的查询
        response = api_client.post(
            "/query",
            json={"sql": "SELECT * FROM nonexistent_table"}
        )
        
        # 验证：返回明确的错误
        assert response.status_code == 404
        assert "nonexistent_table" in response.json()["detail"]
    
    def test_empty_result_vs_error_distinction(self, api_client):
        """测试：空结果与错误的区分"""
        # 空结果查询
        response = api_client.post(
            "/query",
            json={"sql": "SELECT * FROM users WHERE id = 999999"}
        )
        assert response.status_code == 200
        assert response.json()["result"] == []
        
        # 错误查询
        response = api_client.post(
            "/query",
            json={"sql": "SELEC * FROM users"}
        )
        assert response.status_code == 400
```

## 验证

```bash
pytest tests/test_integration_error_handling.py -v
```

## 提交信息

```
test: add end-to-end error handling integration tests

- Test complete error propagation flow
- Test empty result vs error distinction
- Verify structured error responses
```
