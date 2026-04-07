# 最佳实践指南

本文档提供 Quack-Cluster 开发的最佳实践，涵盖代码质量、安全性、性能和可维护性。

## 资源管理最佳实践

### 1. 使用上下文管理器

**原则**: 所有需要清理的资源必须使用上下文管理器。

**正确示例**:
```python
# ✅ 推荐：使用上下文管理器
async with duckdb.connect(database=':memory:') as con:
    result = con.execute(query).fetch_arrow_table()
# 连接自动关闭
```

**错误示例**:
```python
# ❌ 避免：手动管理资源
con = duckdb.connect(database=':memory:')
result = con.execute(query).fetch_arrow_table()
con.close()  # 如果发生异常，永远不会执行
```

### 2. 临时表管理

**原则**: 临时表必须在使用后注销。

**推荐模式**:
```python
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

# 使用
with temporary_table(con, "temp_table", data):
    result = con.execute("SELECT * FROM temp_table").fetch_arrow_table()
```

### 3. Ray Actor 生命周期

**原则**: Ray Actor 应该正确初始化和清理。

**推荐模式**:
```python
@ray.remote
class DuckDBWorker:
    def __init__(self):
        self.con = duckdb.connect(database=':memory:', read_only=False)
    
    def __del__(self):
        """清理资源"""
        if hasattr(self, 'con'):
            try:
                self.con.close()
            except Exception:
                pass
```

## 错误处理最佳实践

### 1. 结构化异常

**原则**: 使用结构化异常类，包含足够的上下文信息。

**推荐模式**:
```python
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
```

### 2. 错误分类

**原则**: 根据错误类型决定是否重试。

**分类标准**:
- **可重试** (NETWORK, RESOURCE): 临时故障，重试可能成功
- **不可重试** (LOGIC, DATA): 永久故障，重试无意义

**示例**:
```python
try:
    result = self.con.execute(query).fetch_arrow_table()
except duckdb.IOException as e:
    # 文件 I/O 错误 - 可重试
    raise WorkerError(
        f"Failed to read data files: {e}",
        ErrorCategory.NETWORK,
        query,
        file_paths,
        e
    )
except duckdb.BinderException as e:
    # SQL 绑定错误 - 不可重试
    raise WorkerError(
        f"Invalid query: {e}",
        ErrorCategory.LOGIC,
        query,
        file_paths,
        e
    )
```

### 3. 区分空结果和错误

**原则**: 空结果是合法的，不应该被视为错误。

**推荐模式**:
```python
def query(self, sql_query: str, file_paths: List[str]) -> pa.Table:
    result = self.con.execute(query).fetch_arrow_table()
    
    # 空结果是合法的
    if result.num_rows == 0:
        logger.info("Query returned 0 rows (valid empty result)")
    
    return result  # 返回空表，但带有正确的 schema
```

**错误模式**:
```python
# ❌ 避免：将空结果视为错误
if result.num_rows == 0:
    raise Exception("No results found")  # 错误！
```

### 4. 错误日志记录

**原则**: 错误日志必须包含足够的上下文信息。

**推荐格式**:
```python
logger.error(
    f"Worker query failed: {e}",
    extra={
        "query": query,
        "file_paths": file_paths,
        "error_type": type(e).__name__,
        "error_category": error.category if isinstance(e, WorkerError) else "unknown"
    }
)
```

## 并发安全最佳实践

### 1. 原子操作

**原则**: 检查和修改必须在同一个锁内完成。

**正确示例**:
```python
# ✅ 推荐：原子操作
async def get(self, key: str) -> Optional[Any]:
    async with self._lock:
        if key not in self._cache:
            return None
        
        if time.time() >= self._ttl[key]:
            del self._cache[key]
            del self._ttl[key]
            return None
        
        return self._cache[key]
```

**错误示例**:
```python
# ❌ 避免：非原子操作
async def get(self, key: str) -> Optional[Any]:
    async with self._lock:
        if key in self._cache:
            pass
    
    # 竞态条件：锁已释放
    if time.time() < self._ttl[key]:
        return self._cache[key]  # 可能已被删除
```

### 2. 避免死锁

**原则**: 锁的获取顺序必须一致。

**推荐模式**:
```python
# 总是按相同顺序获取锁
async with self._lock_a:
    async with self._lock_b:
        # 操作
        pass
```

### 3. 最小化锁持有时间

**原则**: 只在必要时持有锁，尽快释放。

**推荐模式**:
```python
# 在锁外准备数据
data = prepare_data()

# 只在修改共享状态时持有锁
async with self._lock:
    self._cache[key] = data
```

## 性能最佳实践

### 1. 避免不必要的数据复制

**原则**: 使用引用而不是复制大型数据结构。

**推荐模式**:
```python
# ✅ 推荐：使用引用
def process_table(table: pa.Table) -> pa.Table:
    # 直接操作，不复制
    return table.select(['col1', 'col2'])
```

**错误示例**:
```python
# ❌ 避免：不必要的复制
def process_table(table: pa.Table) -> pa.Table:
    table_copy = table.to_pandas().copy()  # 昂贵的复制
    return pa.Table.from_pandas(table_copy)
```

### 2. 批量操作

**原则**: 批量处理而不是逐个处理。

**推荐模式**:
```python
# ✅ 推荐：批量处理
results = await asyncio.gather(*[
    worker.query.remote(sql, files)
    for files in file_batches
])
```

**错误示例**:
```python
# ❌ 避免：逐个处理
results = []
for files in file_batches:
    result = await worker.query.remote(sql, files)
    results.append(result)
```

### 3. 延迟计算

**原则**: 只在需要时才计算。

**推荐模式**:
```python
# ✅ 推荐：延迟计算
def get_table_size(files: List[str]) -> float:
    # 只在调用时计算
    return sum(os.path.getsize(f) for f in files) / (1024 * 1024)
```

## 安全最佳实践

### 1. 输入验证

**原则**: 验证所有外部输入。

**推荐模式**:
```python
def validate_table_name(name: str) -> str:
    """验证表名，防止路径遍历"""
    # 只允许字母、数字、下划线
    if not re.match(r'^[a-zA-Z0-9_]+$', name):
        raise ValueError(f"Invalid table name: {name}")
    return name

# 使用
table_name = validate_table_name(user_input)
```

### 2. SQL 注入防护

**原则**: 使用参数化查询或验证输入。

**推荐模式**:
```python
# ✅ 推荐：使用 SQLGlot 解析和验证
try:
    parsed = parse_one(user_sql, read="duckdb")
except ParseError as e:
    raise HTTPException(400, f"Invalid SQL: {e}")
```

**错误示例**:
```python
# ❌ 避免：直接拼接 SQL
query = f"SELECT * FROM {user_table}"  # SQL 注入风险
```

### 3. 路径遍历防护

**原则**: 验证文件路径在允许的目录内。

**推荐模式**:
```python
def validate_file_path(path: str, base_dir: str) -> str:
    """验证文件路径在基础目录内"""
    abs_path = os.path.abspath(path)
    abs_base = os.path.abspath(base_dir)
    
    if not abs_path.startswith(abs_base):
        raise ValueError(f"Path outside base directory: {path}")
    
    return abs_path
```

### 4. 敏感信息保护

**原则**: 不在日志中记录敏感信息。

**推荐模式**:
```python
# ✅ 推荐：脱敏日志
logger.info(f"Query executed: {query[:100]}...")  # 截断

# ❌ 避免：记录完整查询（可能包含敏感数据）
logger.info(f"Query: {query}")
```

## 测试最佳实践

### 1. 单元测试结构

**原则**: 使用 AAA 模式（Arrange-Act-Assert）。

**推荐模式**:
```python
def test_worker_error_propagation():
    # Arrange
    worker = DuckDBWorker()
    invalid_query = "SELEC * FROM users"
    
    # Act & Assert
    with pytest.raises(WorkerError) as exc_info:
        worker.query(invalid_query, [])
    
    assert exc_info.value.category == ErrorCategory.LOGIC
```

### 2. 测试隔离

**原则**: 每个测试必须独立，不依赖其他测试。

**推荐模式**:
```python
@pytest.fixture
def clean_cache():
    """每个测试前清空缓存"""
    cache = QueryCache()
    yield cache
    # 清理

def test_cache_get(clean_cache):
    # 使用独立的缓存实例
    clean_cache.set("key", "value", 60)
    assert clean_cache.get("key") == "value"
```

### 3. 模拟外部依赖

**原则**: 使用 mock 隔离外部依赖。

**推荐模式**:
```python
@pytest.fixture
def mock_ray_worker(mocker):
    """模拟 Ray Worker"""
    mock = mocker.Mock()
    mock.query.remote.return_value = pa.Table.from_pydict({"id": [1, 2, 3]})
    return mock

def test_executor_with_mock(mock_ray_worker):
    # 使用 mock 测试 Executor 逻辑
    pass
```

### 4. 边界条件测试

**原则**: 测试边界条件和异常情况。

**测试清单**:
- ✅ 空输入
- ✅ 单个元素
- ✅ 大量元素
- ✅ 无效输入
- ✅ 并发访问
- ✅ 资源耗尽

## 日志记录最佳实践

### 1. 日志级别

**使用指南**:
- **DEBUG**: 详细的调试信息
- **INFO**: 正常操作信息
- **WARNING**: 警告但不影响功能
- **ERROR**: 错误但系统可继续
- **CRITICAL**: 严重错误，系统无法继续

**示例**:
```python
logger.debug(f"Processing file: {file_path}")
logger.info(f"Query completed in {duration:.2f}s")
logger.warning(f"Cache miss for key: {key}")
logger.error(f"Worker task failed: {error}")
logger.critical(f"Ray cluster unreachable")
```

### 2. 结构化日志

**原则**: 使用结构化日志便于分析。

**推荐模式**:
```python
logger.info(
    "Query executed",
    extra={
        "query_id": query_id,
        "duration_ms": duration,
        "rows_returned": result.num_rows,
        "cache_hit": cache_hit
    }
)
```

### 3. 日志采样

**原则**: 高频日志应该采样。

**推荐模式**:
```python
# 只记录 1% 的查询
if random.random() < 0.01:
    logger.debug(f"Query details: {query}")
```

## 配置管理最佳实践

### 1. 配置验证

**原则**: 启动时验证所有配置。

**推荐模式**:
```python
class Settings(BaseSettings):
    DATA_DIR: str
    
    @validator('DATA_DIR')
    def validate_data_dir(cls, v):
        if not os.path.exists(v):
            raise ValueError(f"DATA_DIR does not exist: {v}")
        if not os.access(v, os.R_OK):
            raise ValueError(f"DATA_DIR is not readable: {v}")
        return v
```

### 2. 环境特定配置

**原则**: 使用环境变量覆盖默认配置。

**推荐模式**:
```python
# config.yaml (默认)
data_dir: "/app/sample_data"

# 环境变量覆盖
export DATA_DIR="/custom/path"
```

### 3. 敏感配置

**原则**: 敏感配置不应该硬编码。

**推荐模式**:
```python
# ✅ 推荐：从环境变量读取
AWS_ACCESS_KEY: str = os.getenv("AWS_ACCESS_KEY")

# ❌ 避免：硬编码
AWS_ACCESS_KEY: str = "AKIAIOSFODNN7EXAMPLE"
```

## 代码审查清单

### 提交前检查

- [ ] 所有资源使用上下文管理器
- [ ] 错误被正确分类和传播
- [ ] 并发操作是线程安全的
- [ ] 添加了适当的日志记录
- [ ] 输入已验证
- [ ] 添加了单元测试
- [ ] 文档已更新
- [ ] 通过了 linter 检查
- [ ] 没有硬编码的敏感信息

### 代码审查关注点

- [ ] 资源泄漏风险
- [ ] 错误处理是否完整
- [ ] 并发安全问题
- [ ] 性能瓶颈
- [ ] 安全漏洞
- [ ] 测试覆盖率
- [ ] 代码可读性
- [ ] 文档完整性

## 持续改进

### 监控指标

定期检查以下指标：
- 资源使用趋势（内存、连接数）
- 错误率和错误类型分布
- 查询性能（P50/P95/P99）
- 缓存命中率
- 重试成功率

### 定期审查

每月进行以下审查：
- 代码质量指标
- 测试覆盖率
- 技术债务清单
- 性能基准测试
- 安全漏洞扫描

## 总结

遵循这些最佳实践将帮助您：
- ✅ 编写更安全、更可靠的代码
- ✅ 减少资源泄漏和内存问题
- ✅ 提高系统的可维护性
- ✅ 简化问题诊断和调试
- ✅ 提升整体代码质量

记住：**好的代码不仅能工作，还要易于理解、维护和扩展。**
