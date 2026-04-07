# 架构改进方案

本文档详细说明 Quack-Cluster 的架构改进方案，重点关注资源管理、错误处理和系统稳定性。

## 当前架构概览

### 组件关系

```
┌─────────────────────────────────────────────────────────────┐
│                        用户/客户端                            │
└────────────────────────┬────────────────────────────────────┘
                         │ HTTP POST /query
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                    Coordinator (FastAPI)                     │
│  - 接收 SQL 查询                                              │
│  - 解析 AST (SQLGlot)                                        │
│  - 处理 CTE/子查询                                            │
│  - 缓存管理                                                   │
│  - 最终结果聚合                                               │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                      Planner (规划器)                         │
│  - AST 分析                                                   │
│  - 文件发现                                                   │
│  - 策略选择 (Local/Scan/Shuffle/Broadcast)                   │
│  - 生成 ExecutionPlan                                        │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                     Executor (执行器)                         │
│  - Ray 任务编排                                               │
│  - Worker 任务分发                                            │
│  - 重试逻辑                                                   │
│  - 结果收集与聚合                                             │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                  Ray Cluster (分布式运行时)                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   Worker 1   │  │   Worker 2   │  │   Worker N   │      │
│  │  (DuckDB)    │  │  (DuckDB)    │  │  (DuckDB)    │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                    数据存储 (Parquet/CSV/JSON)                │
└─────────────────────────────────────────────────────────────┘
```

## 问题分析

### 1. 资源管理问题

#### 问题 1.1: DuckDB 连接泄漏

**位置**: [coordinator.py:463](quack_cluster/coordinator.py#L463)

**当前实现**:
```python
main_con = duckdb.connect(database=':memory:')
final_arrow_table = await resolve_and_execute(parsed_query, main_con)
# 连接从未关闭
```

**影响**:
- 每个查询创建新连接但不关闭
- 文件描述符耗尽
- 内存持续增长

**改进方案**:
```python
# 方案 A: 使用上下文管理器（推荐）
async with duckdb.connect(database=':memory:') as main_con:
    final_arrow_table = await resolve_and_execute(parsed_query, main_con)

# 方案 B: 连接池（未来优化）
class DuckDBConnectionPool:
    def __init__(self, max_connections: int = 10):
        self._pool = asyncio.Queue(maxsize=max_connections)
        self._created = 0
        self._max = max_connections
    
    async def acquire(self) -> duckdb.DuckDBPyConnection:
        if self._pool.empty() and self._created < self._max:
            conn = duckdb.connect(database=':memory:')
            self._created += 1
            return conn
        return await self._pool.get()
    
    async def release(self, conn: duckdb.DuckDBPyConnection):
        await self._pool.put(conn)
```

#### 问题 1.2: 临时表未注销

**位置**: [coordinator.py:290](quack_cluster/coordinator.py#L290)

**当前实现**:
```python
con.register(cte.alias, cte_result_table)
# 使用后从未注销
```

**改进方案**:
```python
# 使用上下文管理器自动清理
from contextlib import contextmanager

@contextmanager
def temporary_table(con: duckdb.DuckDBPyConnection, name: str, table: pa.Table):
    """临时表上下文管理器"""
    try:
        con.register(name, table)
        yield
    finally:
        try:
            con.unregister(name)
        except Exception as e:
            logger.warning(f"Failed to unregister table {name}: {e}")

# 使用方式
with temporary_table(con, cte.alias, cte_result_table):
    result = await resolve_and_execute(query_body, con)
```

### 2. 错误处理问题

#### 问题 2.1: Worker 静默失败

**位置**: [worker.py:220-224](quack_cluster/worker.py#L220-L224)

**当前实现**:
```python
except Exception as e:
    print("🔥🔥🔥 QUERY WORKER 失败！🔥🔥🔥")
    return pa.Table.from_pydict({})  # 返回空表，掩盖错误
```

**问题**:
- 无法区分"无结果"和"查询失败"
- 调试困难
- 可能导致数据丢失

**改进方案**:

**新增错误模型** ([execution_plan.py](quack_cluster/execution_plan.py)):
```python
from enum import Enum
from typing import Optional, List

class ErrorCategory(str, Enum):
    """错误分类"""
    NETWORK = "network"      # 网络/IO 错误 - 可重试
    RESOURCE = "resource"    # 资源不足 - 可重试
    LOGIC = "logic"          # 逻辑错误 - 不可重试
    DATA = "data"            # 数据错误 - 不可重试

class WorkerError(Exception):
    """Worker 执行错误"""
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
        self.file_paths = file_paths
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
```

**Worker 错误处理** ([worker.py](quack_cluster/worker.py)):
```python
def query(self, sql_query: str, file_paths: List[str]) -> pa.Table:
    query_to_execute = ""
    try:
        formatted_paths = ", ".join([f"'{path}'" for path in file_paths])
        query_to_execute = sql_query.format(files=f"[{formatted_paths}]")
        result = self.con.execute(query_to_execute).fetch_arrow_table()
        
        # 空结果是合法的
        if result.num_rows == 0:
            logger.info(f"Query returned 0 rows (valid empty result)")
        
        return result
        
    except duckdb.IOException as e:
        # 文件 I/O 错误 - 可重试
        raise WorkerError(
            f"Failed to read data files: {e}",
            ErrorCategory.NETWORK,
            query_to_execute,
            file_paths,
            e
        )
    except duckdb.BinderException as e:
        # SQL 绑定错误 - 不可重试
        raise WorkerError(
            f"Invalid query binding: {e}",
            ErrorCategory.LOGIC,
            query_to_execute,
            file_paths,
            e
        )
    except duckdb.ParserException as e:
        # SQL 解析错误 - 不可重试
        raise WorkerError(
            f"Query parsing failed: {e}",
            ErrorCategory.LOGIC,
            query_to_execute,
            file_paths,
            e
        )
    except Exception as e:
        # 未知错误 - 保守起见不重试
        raise WorkerError(
            f"Worker query failed: {e}",
            ErrorCategory.LOGIC,
            query_to_execute,
            file_paths,
            e
        )
```

### 3. 并发安全问题

#### 问题 3.1: 缓存竞态条件

**位置**: [coordinator.py:60-79](quack_cluster/coordinator.py#L60-L79)

**当前实现**:
```python
async def get(self, key: str) -> Optional[Any]:
    async with self._lock:
        if key in self._cache:
            if time.time() < self._ttl[key]:  # 竞态条件
                return self._cache[key]
```

**改进方案**:
```python
async def get(self, key: str) -> Optional[Any]:
    """原子化的缓存获取"""
    async with self._lock:
        if key not in self._cache:
            return None
        
        current_time = time.time()
        
        # 原子检查和操作
        if current_time >= self._ttl[key]:
            # 已过期，删除并返回 None
            del self._cache[key]
            del self._ttl[key]
            logger.info(f"Cache expired: {key[:10]}...")
            return None
        
        # 未过期，返回数据
        logger.info(f"Cache hit: {key[:10]}...")
        return self._cache[key]

async def set(self, key: str, value: Any, ttl_seconds: int):
    """原子化的缓存设置"""
    async with self._lock:
        expiry_time = time.time() + ttl_seconds
        self._cache[key] = value
        self._ttl[key] = expiry_time
        logger.info(f"Cache set: {key[:10]}..., TTL: {ttl_seconds}s")
```

### 4. 重试策略问题

#### 问题 4.1: 固定延迟重试

**位置**: [executor.py:98](quack_cluster/executor.py#L98)

**当前实现**:
```python
await asyncio.sleep(1)  # 固定 1 秒延迟
```

**改进方案**:

**配置** ([settings.py](quack_cluster/settings.py)):
```python
class Settings(BaseSettings):
    # 重试配置
    MAX_RETRIES: int = 3
    RETRY_BASE_DELAY: float = 1.0       # 基础延迟（秒）
    RETRY_MAX_DELAY: float = 30.0       # 最大延迟（秒）
    RETRY_EXPONENTIAL_BASE: float = 2.0 # 指数基数
```

**智能重试** ([executor.py](quack_cluster/executor.py)):
```python
@staticmethod
def _should_retry(error: Exception) -> bool:
    """判断错误是否应该重试"""
    if isinstance(error, WorkerError):
        return error.category in [ErrorCategory.NETWORK, ErrorCategory.RESOURCE]
    
    if isinstance(error, asyncio.TimeoutError):
        return True  # 超时可以重试
    
    return False  # 其他错误不重试

@staticmethod
def _calculate_backoff_delay(retry_count: int) -> float:
    """计算指数退避延迟"""
    delay = settings.RETRY_BASE_DELAY * (
        settings.RETRY_EXPONENTIAL_BASE ** retry_count
    )
    return min(delay, settings.RETRY_MAX_DELAY)

@staticmethod
async def _execute_with_retries(
    initial_inputs: List[Any],
    task_function: Callable[[Any], Coroutine]
) -> List[Any]:
    """带智能重试的任务执行"""
    inputs_to_process = list(initial_inputs)
    all_results = []
    retries = 0
    
    while inputs_to_process and retries <= settings.MAX_RETRIES:
        if retries > 0:
            delay = Executor._calculate_backoff_delay(retries)
            logger.warning(
                f"Retrying {len(inputs_to_process)} failed tasks "
                f"(attempt {retries}/{settings.MAX_RETRIES}, delay {delay:.1f}s)"
            )
            await asyncio.sleep(delay)
        
        tasks = [task_function(inp) for inp in inputs_to_process]
        task_to_input_map = {tasks[i]: inputs_to_process[i] for i in range(len(tasks))}
        task_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        failed_inputs = []
        for i, res in enumerate(task_results):
            if isinstance(res, Exception):
                failed_input = task_to_input_map[tasks[i]]
                
                if Executor._should_retry(res):
                    logger.error(f"Retryable error: {res}")
                    failed_inputs.append(failed_input)
                else:
                    logger.error(f"Non-retryable error: {res}")
                    raise res  # 立即失败
            else:
                all_results.append(res)
        
        inputs_to_process = failed_inputs
        if inputs_to_process:
            retries += 1
    
    if inputs_to_process:
        raise Exception(
            f"Query failed after {settings.MAX_RETRIES} retries. "
            f"Failed inputs: {inputs_to_process}"
        )
    
    return all_results
```

### 5. 超时机制

#### 问题 5.1: 缺少超时保护

**改进方案**:

**配置** ([settings.py](quack_cluster/settings.py)):
```python
class Settings(BaseSettings):
    # 超时配置
    QUERY_TIMEOUT_SECONDS: int = 300      # 查询级别超时（5 分钟）
    WORKER_TASK_TIMEOUT_SECONDS: int = 120 # Worker 任务超时（2 分钟）
```

**查询超时** ([coordinator.py](quack_cluster/coordinator.py)):
```python
@app.post("/query")
async def execute_query(request: QueryRequest, format: Optional[str] = Query("json")):
    try:
        async with duckdb.connect(database=':memory:') as main_con:
            # 添加查询级别超时
            final_arrow_table = await asyncio.wait_for(
                resolve_and_execute(parsed_query, main_con),
                timeout=settings.QUERY_TIMEOUT_SECONDS
            )
    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=status.HTTP_408_REQUEST_TIMEOUT,
            detail=f"Query exceeded timeout of {settings.QUERY_TIMEOUT_SECONDS}s"
        )
```

**Worker 任务超时** ([executor.py](quack_cluster/executor.py)):
```python
@staticmethod
async def _execute_with_retries(...):
    # 为每个任务添加超时
    tasks = [
        asyncio.wait_for(
            task_function(inp),
            timeout=settings.WORKER_TASK_TIMEOUT_SECONDS
        )
        for inp in inputs_to_process
    ]
```

## 改进后的架构

### 资源管理流程

```
┌─────────────────────────────────────────────────────────────┐
│                      查询请求到达                             │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              创建 DuckDB 连接（上下文管理器）                  │
│  async with duckdb.connect(':memory:') as con:              │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                    处理 CTE/子查询                            │
│  with temporary_table(con, name, table):                    │
│      # 使用临时表                                             │
│  # 自动注销                                                   │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                      执行查询                                 │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              连接自动关闭（退出上下文）                        │
└─────────────────────────────────────────────────────────────┘
```

### 错误处理流程

```
┌─────────────────────────────────────────────────────────────┐
│                    Worker 执行任务                            │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
                    ┌─────────┐
                    │ 成功？   │
                    └────┬────┘
                         │
            ┌────────────┴────────────┐
            │                         │
           是                        否
            │                         │
            ▼                         ▼
    ┌──────────────┐         ┌──────────────┐
    │ 返回结果      │         │ 抛出异常      │
    │ (可能为空)    │         │ WorkerError  │
    └──────────────┘         └──────┬───────┘
                                    │
                                    ▼
                            ┌──────────────┐
                            │ 错误分类      │
                            └──────┬───────┘
                                   │
                    ┌──────────────┴──────────────┐
                    │                             │
              可重试错误                      不可重试错误
            (NETWORK/RESOURCE)                (LOGIC/DATA)
                    │                             │
                    ▼                             ▼
            ┌──────────────┐              ┌──────────────┐
            │ 指数退避重试  │              │ 立即失败      │
            └──────────────┘              └──────────────┘
```

### 并发安全流程

```
┌─────────────────────────────────────────────────────────────┐
│                    缓存操作请求                               │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                  获取异步锁                                   │
│  async with self._lock:                                     │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              原子化检查和操作                                  │
│  1. 检查键是否存在                                            │
│  2. 检查 TTL 是否过期                                         │
│  3. 返回数据或删除过期条目                                     │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                  释放锁                                       │
└─────────────────────────────────────────────────────────────┘
```

## 性能影响分析

### 资源管理开销

| 操作 | 当前 | 改进后 | 影响 |
|------|------|--------|------|
| 连接创建/关闭 | 创建但不关闭 | 上下文管理器 | +0.1ms（可忽略） |
| 临时表注册/注销 | 注册但不注销 | 自动注销 | +0.05ms（可忽略） |
| 内存使用 | 持续增长 | 稳定 | 显著改善 |

### 错误处理开销

| 操作 | 当前 | 改进后 | 影响 |
|------|------|--------|------|
| 正常查询 | 无开销 | 无开销 | 无影响 |
| 错误查询 | 返回空表 | 抛出异常 | +1ms（可接受） |
| 错误分类 | 无 | 类型检查 | +0.1ms（可忽略） |

### 重试策略开销

| 场景 | 当前 | 改进后 | 影响 |
|------|------|--------|------|
| 首次成功 | 无开销 | 无开销 | 无影响 |
| 重试 1 次 | 1s 延迟 | 1s 延迟 | 无影响 |
| 重试 2 次 | 2s 延迟 | 3s 延迟 | +1s（可接受） |
| 重试 3 次 | 3s 延迟 | 7s 延迟 | +4s（可接受） |
| 逻辑错误 | 3 次重试 | 立即失败 | 显著改善 |

## 部署策略

### 阶段 1: 资源管理修复（低风险）

**变更**:
- 添加上下文管理器
- 实现临时表自动注销

**风险**: 极低  
**回滚**: 简单（恢复代码）  
**验证**: 资源监控

### 阶段 2: 错误处理改进（中风险）

**变更**:
- 添加 WorkerError 类
- 修改 Worker 错误传播

**风险**: 中等（行为变化）  
**回滚**: 中等（需要测试）  
**验证**: 错误日志分析

### 阶段 3: 重试策略优化（低风险）

**变更**:
- 实现指数退避
- 添加错误分类

**风险**: 低  
**回滚**: 简单  
**验证**: 重试日志分析

### 阶段 4: 超时机制（低风险）

**变更**:
- 添加查询超时
- 添加任务超时

**风险**: 低  
**回滚**: 简单  
**验证**: 超时日志分析

## 监控指标

### 资源指标
- DuckDB 连接数
- 临时表数量
- 内存使用量
- 文件描述符数量

### 错误指标
- Worker 错误率（按类别）
- 重试成功率
- 超时查询数量
- 错误响应时间

### 性能指标
- 查询延迟（P50/P95/P99）
- Worker 任务延迟
- 重试延迟分布
- 缓存命中率

## 总结

本架构改进方案通过以下措施显著提升系统稳定性：

1. **资源管理**: 使用上下文管理器确保资源正确释放
2. **错误处理**: 实现结构化错误传播和分类
3. **并发安全**: 原子化缓存操作避免竞态条件
4. **容错性**: 智能重试策略和超时保护
5. **可观测性**: 完善的日志和监控指标

这些改进为后续的安全加固、性能优化和功能扩展奠定了坚实的基础。
