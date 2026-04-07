# Task 010: 并发安全 - 缓存原子操作测试

## 目标

编写测试验证缓存操作的原子性，确保无竞态条件。

## BDD 场景

```gherkin
Feature: 缓存并发安全
  作为系统开发者
  我希望缓存操作是原子的
  以便避免竞态条件

  Scenario: 缓存 TTL 检查和数据访问是原子的
    Given 缓存中有一个即将过期的条目
    And 条目的 TTL 还剩 0.1 秒
    When 线程 A 开始读取缓存
    And 线程 B 同时开始读取缓存
    Then 两个线程应该获得一致的结果
    And 不应该出现竞态条件
    And 过期的条目应该被正确删除

  Scenario: 并发缓存写入不会丢失数据
    Given 缓存为空
    When 10 个线程同时写入不同的缓存条目
    Then 所有 10 个条目应该被正确保存
    And 不应该有条目丢失
    And 每个条目的 TTL 应该正确设置

  Scenario: 缓存过期清理是线程安全的
    Given 缓存中有多个已过期的条目
    When 多个线程同时读取缓存
    Then 过期条目应该被正确删除
    And 不应该出现重复删除
    And 不应该抛出 KeyError
```

## 依赖

**depends-on**: []

## 涉及文件

- `tests/test_cache_atomic_ops.py` - 新建测试文件

## 实现步骤

创建 `tests/test_cache_atomic_ops.py`:

```python
import pytest
import asyncio
import time
from quack_cluster.coordinator import QueryCache

@pytest.mark.asyncio
class TestCacheAtomicOperations:
    """测试缓存原子操作"""
    
    async def test_cache_ttl_check_is_atomic(self):
        """测试：缓存 TTL 检查和数据访问是原子的"""
        cache = QueryCache()
        
        # 设置一个即将过期的条目
        await cache.set("key1", "value1", ttl_seconds=1)
        
        # 等待接近过期时间
        await asyncio.sleep(0.9)
        
        # 并发读取
        results = await asyncio.gather(
            cache.get("key1"),
            cache.get("key1"),
            cache.get("key1")
        )
        
        # 验证：所有结果一致（要么都是 value1，要么都是 None）
        assert len(set(results)) == 1, "Race condition detected: inconsistent results"
    
    async def test_concurrent_cache_writes(self):
        """测试：并发缓存写入不会丢失数据"""
        cache = QueryCache()
        
        # 并发写入 10 个不同的条目
        await asyncio.gather(*[
            cache.set(f"key{i}", f"value{i}", ttl_seconds=60)
            for i in range(10)
        ])
        
        # 验证：所有条目都被保存
        results = await asyncio.gather(*[
            cache.get(f"key{i}")
            for i in range(10)
        ])
        
        assert all(results[i] == f"value{i}" for i in range(10))
    
    async def test_cache_expiry_cleanup_is_thread_safe(self):
        """测试：缓存过期清理是线程安全的"""
        cache = QueryCache()
        
        # 设置多个即将过期的条目
        for i in range(5):
            await cache.set(f"key{i}", f"value{i}", ttl_seconds=1)
        
        # 等待过期
        await asyncio.sleep(1.1)
        
        # 并发读取（触发过期清理）
        results = await asyncio.gather(*[
            cache.get(f"key{i}")
            for i in range(5)
        ], return_exceptions=True)
        
        # 验证：所有结果都是 None，没有异常
        assert all(r is None for r in results), "KeyError or race condition detected"
    
    async def test_cache_set_and_get_are_consistent(self):
        """测试：缓存设置和获取的一致性"""
        cache = QueryCache()
        
        # 并发设置和获取同一个键
        async def set_value():
            await cache.set("key", "value", ttl_seconds=60)
        
        async def get_value():
            return await cache.get("key")
        
        # 交替执行设置和获取
        tasks = []
        for _ in range(10):
            tasks.append(set_value())
            tasks.append(get_value())
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 验证：没有异常
        assert all(not isinstance(r, Exception) for r in results)
```

## 验证

运行测试（预期失败）:

```bash
pytest tests/test_cache_atomic_ops.py -v
```

## 提交信息

```
test: add cache atomic operations tests (RED)

- Test TTL check and data access atomicity
- Test concurrent cache writes
- Test thread-safe expiry cleanup
- Test set/get consistency under concurrency

These tests currently FAIL and will pass after implementing
atomic operations in Task 011.
```
