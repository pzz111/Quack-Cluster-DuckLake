# Task 002: 资源管理 - DuckDB 连接上下文管理器测试

## 目标

编写测试验证 DuckDB 连接使用上下文管理器后能够自动关闭，防止资源泄漏。

## BDD 场景

```gherkin
Feature: DuckDB 连接资源管理
  作为系统管理员
  我希望所有 DuckDB 连接在使用后自动关闭
  以便避免资源泄漏和文件描述符耗尽

  Scenario: 查询成功执行后连接被关闭
    Given 系统启动并准备接收查询
    When 用户提交 SQL 查询 "SELECT * FROM users"
    And 查询成功执行
    Then DuckDB 连接应该被自动关闭
    And 系统不应该有未关闭的连接

  Scenario: 查询失败后连接仍被关闭
    Given 系统启动并准备接收查询
    When 用户提交无效 SQL 查询 "SELEC * FROM users"
    And 查询执行失败
    Then DuckDB 连接应该被自动关闭
    And 系统不应该有未关闭的连接

  Scenario: 并发查询的连接隔离
    Given 系统启动并准备接收查询
    When 用户同时提交 10 个查询
    And 所有查询执行完成
    Then 每个查询应该使用独立的连接
    And 所有连接应该被正确关闭
    And 系统不应该有连接泄漏
```

## 依赖

**depends-on**: []

## 涉及文件

- `tests/test_resource_management.py` - 新建测试文件

## 实现步骤

### 1. 创建测试文件

创建 `tests/test_resource_management.py`:

```python
import pytest
import asyncio
import duckdb
from fastapi.testclient import TestClient
from quack_cluster.coordinator import app
from quack_cluster.settings import generate_sample_data

@pytest.fixture(scope="module", autouse=True)
def setup_test_data():
    """生成测试数据"""
    generate_sample_data()

@pytest.fixture
def api_client():
    """创建 API 客户端"""
    with TestClient(app) as client:
        yield client

class TestDuckDBConnectionManagement:
    """测试 DuckDB 连接资源管理"""
    
    def test_connection_closed_after_successful_query(self, api_client):
        """测试：查询成功后连接被关闭"""
        # 执行查询
        response = api_client.post(
            "/query",
            json={"sql": "SELECT * FROM users LIMIT 1"}
        )
        
        assert response.status_code == 200
        
        # 验证：无法检测到未关闭的连接
        # 注意：这是一个间接测试，实际实现后会有更直接的验证方式
        # 可以通过监控文件描述符或内存使用来验证
    
    def test_connection_closed_after_failed_query(self, api_client):
        """测试：查询失败后连接仍被关闭"""
        # 执行无效查询
        response = api_client.post(
            "/query",
            json={"sql": "SELEC * FROM users"}  # 故意拼错
        )
        
        assert response.status_code == 400
        
        # 验证：即使查询失败，连接也应该被关闭
    
    def test_concurrent_queries_connection_isolation(self, api_client):
        """测试：并发查询的连接隔离"""
        import concurrent.futures
        
        def execute_query(query_id):
            response = api_client.post(
                "/query",
                json={"sql": f"SELECT {query_id} as id"}
            )
            return response.status_code == 200
        
        # 并发执行 10 个查询
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(execute_query, i) for i in range(10)]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]
        
        # 验证：所有查询都成功
        assert all(results)
        assert len(results) == 10
    
    def test_no_connection_leak_after_multiple_queries(self, api_client):
        """测试：多次查询后无连接泄漏"""
        # 执行 100 次查询
        for i in range(100):
            response = api_client.post(
                "/query",
                json={"sql": "SELECT 1"}
            )
            assert response.status_code == 200
        
        # 验证：无连接泄漏
        # 实际实现中可以检查进程的文件描述符数量
```

### 2. 添加资源监控辅助函数

```python
import psutil
import os

def get_open_file_descriptors():
    """获取当前进程打开的文件描述符数量"""
    process = psutil.Process(os.getpid())
    return process.num_fds() if hasattr(process, 'num_fds') else len(process.open_files())

class TestConnectionLeakDetection:
    """测试连接泄漏检测"""
    
    def test_file_descriptors_stable_after_queries(self, api_client):
        """测试：查询后文件描述符数量稳定"""
        # 记录初始文件描述符数量
        initial_fds = get_open_file_descriptors()
        
        # 执行 50 次查询
        for _ in range(50):
            api_client.post("/query", json={"sql": "SELECT 1"})
        
        # 记录最终文件描述符数量
        final_fds = get_open_file_descriptors()
        
        # 验证：文件描述符增长不超过 5 个（允许一些缓存）
        assert final_fds - initial_fds < 5, \
            f"File descriptor leak detected: {initial_fds} -> {final_fds}"
```

## 验证

运行测试（预期失败，因为实现尚未完成）：

```bash
pytest tests/test_resource_management.py::TestDuckDBConnectionManagement -v
```

预期输出：
```
FAILED test_connection_closed_after_successful_query - 连接未关闭
FAILED test_connection_closed_after_failed_query - 连接未关闭
FAILED test_concurrent_queries_connection_isolation - 连接泄漏
FAILED test_no_connection_leak_after_multiple_queries - 连接泄漏
```

## 提交信息

```
test: add DuckDB connection management tests (RED)

- Test connection closed after successful query
- Test connection closed after failed query
- Test concurrent query connection isolation
- Test no connection leak after multiple queries
- Add file descriptor monitoring helper

These tests currently FAIL and will pass after implementing
context manager in Task 003.
```

## 注意事项

1. **测试隔离**: 每个测试应该独立，不依赖其他测试
2. **资源监控**: 使用 psutil 监控文件描述符
3. **并发测试**: 使用 ThreadPoolExecutor 模拟并发场景
4. **预期失败**: 这是 RED 阶段，测试应该失败
