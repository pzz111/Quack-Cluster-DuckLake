# Task 018: 集成测试 - 端到端资源管理验证

## 目标

编写端到端集成测试验证资源管理修复的完整流程。

## BDD 场景

综合验证特性 1（资源管理）的所有场景。

## 依赖

**depends-on**: ["003", "005"]

## 涉及文件

- `tests/test_integration_resource_management.py` - 新建测试文件

## 实现步骤

创建 `tests/test_integration_resource_management.py`:

```python
import pytest
from fastapi.testclient import TestClient
from quack_cluster.coordinator import app
from quack_cluster.settings import generate_sample_data

@pytest.fixture(scope="module", autouse=True)
def setup():
    generate_sample_data()

@pytest.fixture
def api_client():
    with TestClient(app) as client:
        yield client

class TestResourceManagementIntegration:
    """端到端资源管理集成测试"""
    
    def test_end_to_end_query_with_resource_cleanup(self, api_client):
        """测试：完整查询流程的资源清理"""
        # 执行包含 CTE 的复杂查询
        sql = """
        WITH user_stats AS (
            SELECT user_id, COUNT(*) as order_count
            FROM orders
            GROUP BY user_id
        )
        SELECT u.name, us.order_count
        FROM users u
        JOIN user_stats us ON u.user_id = us.user_id
        ORDER BY us.order_count DESC
        LIMIT 5
        """
        
        response = api_client.post("/query", json={"sql": sql})
        assert response.status_code == 200
        
        # 验证：资源已清理（通过多次执行验证无泄漏）
        for _ in range(10):
            response = api_client.post("/query", json={"sql": sql})
            assert response.status_code == 200
    
    def test_resource_cleanup_on_query_failure(self, api_client):
        """测试：查询失败时的资源清理"""
        # 执行会失败的查询
        response = api_client.post("/query", json={"sql": "INVALID SQL"})
        assert response.status_code == 400
        
        # 验证：后续查询仍然正常
        response = api_client.post("/query", json={"sql": "SELECT 1"})
        assert response.status_code == 200
```

## 验证

```bash
pytest tests/test_integration_resource_management.py -v
```

## 提交信息

```
test: add end-to-end resource management integration tests

- Test complete query flow with resource cleanup
- Test resource cleanup on query failure
- Verify no leaks after repeated queries
```
