# Task 011: 并发安全 - 缓存原子操作实现

## 目标

修复 QueryCache 的竞态条件，实现原子化的缓存操作。

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
```

## 依赖

**depends-on**: ["010"]

## 涉及文件

- `quack_cluster/coordinator.py` - 修改 QueryCache 类的 get() 和 set() 方法

## 实现步骤

### 1. 修改 QueryCache.get() 方法

在 [coordinator.py:60-79](quack_cluster/coordinator.py#L60-L79) 修改：

```python
async def get(self, key: str) -> Optional[Any]:
    """原子化的缓存获取"""
    async with self._lock:
        # 检查键是否存在
        if key not in self._cache:
            return None
        
        current_time = time.time()
        
        # 原子检查 TTL 和返回数据
        if current_time >= self._ttl[key]:
            # 已过期，删除并返回 None
            del self._cache[key]
            del self._ttl[key]
            logger.info(f"缓存已过期！key: {key[:10]}...")
            return None
        
        # 未过期，返回数据
        logger.info(f"✅ 缓存命中！key: {key[:10]}...")
        return self._cache[key]
```

### 2. 修改 QueryCache.set() 方法

在 [coordinator.py:81-93](quack_cluster/coordinator.py#L81-L93) 修改：

```python
async def set(self, key: str, value: Any, ttl_seconds: int):
    """原子化的缓存设置"""
    async with self._lock:
        expiry_time = time.time() + ttl_seconds
        self._cache[key] = value
        self._ttl[key] = expiry_time
        logger.info(f"已设置缓存，key: {key[:10]}...，TTL: {ttl_seconds}秒")
```

### 3. 完整的 QueryCache 类

```python
class QueryCache:
    """内存缓存类，支持 TTL（生存时间）和线程安全"""
    
    def __init__(self):
        self._cache: Dict[str, Any] = {}
        self._ttl: Dict[str, float] = {}
        self._lock = asyncio.Lock()
    
    async def get(self, key: str) -> Optional[Any]:
        """原子化的缓存获取
        
        在锁内完成所有操作：
        1. 检查键是否存在
        2. 检查 TTL 是否过期
        3. 返回数据或删除过期条目
        
        这确保了 TTL 检查和数据访问之间没有竞态条件。
        """
        async with self._lock:
            if key not in self._cache:
                return None
            
            current_time = time.time()
            
            if current_time >= self._ttl[key]:
                # 已过期，删除并返回 None
                del self._cache[key]
                del self._ttl[key]
                logger.info(f"缓存已过期！key: {key[:10]}...")
                return None
            
            # 未过期，返回数据
            logger.info(f"✅ 缓存命中！key: {key[:10]}...")
            return self._cache[key]
    
    async def set(self, key: str, value: Any, ttl_seconds: int):
        """原子化的缓存设置
        
        在锁内完成所有操作：
        1. 计算过期时间
        2. 设置缓存值
        3. 设置 TTL
        
        这确保了缓存值和 TTL 的一致性。
        """
        async with self._lock:
            expiry_time = time.time() + ttl_seconds
            self._cache[key] = value
            self._ttl[key] = expiry_time
            logger.info(f"已设置缓存，key: {key[:10]}...，TTL: {ttl_seconds}秒")
```

## 验证

运行测试（应该通过）：

```bash
pytest tests/test_cache_atomic_ops.py -v
```

预期输出：
```
PASSED test_cache_ttl_check_is_atomic
PASSED test_concurrent_cache_writes
PASSED test_cache_expiry_cleanup_is_thread_safe
```

## 提交信息

```
fix: make cache operations atomic to prevent race conditions (GREEN)

- Move TTL check inside lock in get() method
- Ensure all cache operations complete atomically
- Prevent race condition between TTL check and data access
- Add detailed comments explaining atomicity guarantees

Closes: Task 010 (tests now pass)
Related: docs/plans/2026-04-07-quack-cluster-code-review-and-improvements
```

## 注意事项

1. **原子性**: 所有检查和修改必须在同一个锁内完成
2. **性能**: 锁持有时间很短（< 1ms），不影响并发性能
3. **一致性**: 缓存值和 TTL 始终保持一致
4. **死锁预防**: 只有一个锁，不会出现死锁
