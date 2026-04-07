# Task 022: 文档更新 - 更新架构和开发指南

## 目标

更新项目文档以反映所有 Bug 修复和架构改进。

## BDD 场景

本任务不直接对应 BDD 场景，但确保所有修复都有文档记录。

## 依赖

**depends-on**: ["003", "005", "007", "009", "011", "013", "015", "017"]

## 涉及文件

- `documentation/ARCHITECTURE.md` - 更新架构文档
- `documentation/DEVELOPER_GUIDE.md` - 更新开发指南
- `CLAUDE.md` - 更新 Claude 指南
- `README.md` - 更新 README

## 实现步骤

### 1. 更新 ARCHITECTURE.md

添加资源管理和错误处理章节：

```markdown
## 资源管理

### DuckDB 连接管理

所有 DuckDB 连接使用上下文管理器自动关闭：

\`\`\`python
# coordinator.py
async with duckdb.connect(database=':memory:') as main_con:
    result = await resolve_and_execute(query, main_con)
# 连接自动关闭
\`\`\`

### 临时表管理

临时表使用上下文管理器自动注销：

\`\`\`python
with temporary_table(con, "temp_table", data):
    result = con.execute("SELECT * FROM temp_table").fetch_arrow_table()
# 临时表自动注销
\`\`\`

## 错误处理

### WorkerError 异常类

所有 Worker 错误使用 `WorkerError` 异常类传播：

\`\`\`python
class WorkerError(Exception):
    def __init__(
        self,
        message: str,
        category: ErrorCategory,  # NETWORK, RESOURCE, LOGIC, DATA
        query: str,
        file_paths: Optional[List[str]] = None,
        original_error: Optional[Exception] = None
    ):
        ...
\`\`\`

### 错误分类

- **NETWORK**: 网络/IO 错误 - 可重试
- **RESOURCE**: 资源不足 - 可重试
- **LOGIC**: 逻辑错误 - 不可重试
- **DATA**: 数据错误 - 不可重试

### 空结果 vs 错误

- 空结果（0 行）是合法的，返回空表但带正确 schema
- 查询失败抛出 WorkerError 异常

## 容错性

### 指数退避重试

系统使用指数退避策略重试可重试错误：

- 第 1 次重试：1 秒延迟
- 第 2 次重试：2 秒延迟
- 第 3 次重试：4 秒延迟
- 最大延迟：30 秒

逻辑错误立即失败，不重试。

### 超时保护

- 查询级别超时：300 秒（可配置）
- Worker 任务超时：120 秒（可配置）

超时查询返回 408 状态码。

## 并发安全

### 缓存原子操作

QueryCache 的所有操作都是原子的：

\`\`\`python
async def get(self, key: str) -> Optional[Any]:
    async with self._lock:
        # TTL 检查和数据访问在同一个锁内
        if key not in self._cache:
            return None
        if time.time() >= self._ttl[key]:
            del self._cache[key]
            del self._ttl[key]
            return None
        return self._cache[key]
\`\`\`
```

### 2. 更新 DEVELOPER_GUIDE.md

添加最佳实践章节：

```markdown
## 资源管理最佳实践

### 使用上下文管理器

所有需要清理的资源必须使用上下文管理器：

\`\`\`python
# ✅ 推荐
async with duckdb.connect(':memory:') as con:
    result = con.execute(query).fetch_arrow_table()

# ❌ 避免
con = duckdb.connect(':memory:')
result = con.execute(query).fetch_arrow_table()
con.close()  # 如果发生异常，永远不会执行
\`\`\`

## 错误处理最佳实践

### 使用结构化异常

\`\`\`python
# ✅ 推荐
raise WorkerError(
    "Failed to read file",
    ErrorCategory.NETWORK,
    query,
    file_paths,
    original_error
)

# ❌ 避免
return pa.Table.from_pydict({})  # 静默失败
\`\`\`

### 区分空结果和错误

\`\`\`python
# ✅ 推荐
result = con.execute(query).fetch_arrow_table()
if result.num_rows == 0:
    logger.info("Query returned 0 rows (valid empty result)")
return result

# ❌ 避免
if result.num_rows == 0:
    raise Exception("No results")  # 错误！
\`\`\`

## 配置管理

### 重试配置

\`\`\`yaml
# config.yaml
max_retries: 3
retry_base_delay: 1.0
retry_max_delay: 30.0
retry_exponential_base: 2.0
\`\`\`

### 超时配置

\`\`\`yaml
# config.yaml
query_timeout_seconds: 300
worker_task_timeout_seconds: 120
\`\`\`
```

### 3. 更新 CLAUDE.md

添加关键修复说明：

```markdown
## 最近的重要修复

### 资源管理（2026-04-07）

- 所有 DuckDB 连接使用上下文管理器
- 临时表自动注销
- 无资源泄漏

### 错误处理（2026-04-07）

- Worker 使用 WorkerError 传播错误
- 错误分类支持智能重试
- 区分空结果和查询失败

### 容错性（2026-04-07）

- 指数退避重试策略
- 查询和任务超时保护
- 缓存原子操作

## 开发注意事项

1. **资源管理**: 始终使用上下文管理器
2. **错误处理**: 使用 WorkerError，不要返回空表
3. **并发安全**: 缓存操作已原子化
4. **超时**: 长时间运行的查询会被终止
```

### 4. 更新 README.md

添加稳定性改进说明：

```markdown
## 最近更新

### 稳定性改进（2026-04-07）

我们完成了一次全面的代码审查和 Bug 修复，显著提升了系统稳定性：

- ✅ **零资源泄漏**: 所有连接和临时表自动清理
- ✅ **清晰的错误诊断**: 结构化错误传播，丰富的上下文信息
- ✅ **智能重试**: 指数退避策略，只重试可重试错误
- ✅ **超时保护**: 防止资源被长时间占用
- ✅ **并发安全**: 缓存操作原子化，无竞态条件

详见 [架构文档](documentation/ARCHITECTURE.md) 和 [开发指南](documentation/DEVELOPER_GUIDE.md)。
```

## 验证

### 1. 检查文档完整性

```bash
# 检查所有文档文件存在
ls documentation/ARCHITECTURE.md
ls documentation/DEVELOPER_GUIDE.md
ls CLAUDE.md
ls README.md
```

### 2. 验证链接

确保所有内部链接有效。

### 3. 代码示例验证

确保文档中的代码示例与实际实现一致。

## 提交信息

```
docs: update architecture and developer guide with bug fixes

- Add resource management section to ARCHITECTURE.md
- Add error handling section with WorkerError details
- Add fault tolerance section (retry, timeout)
- Add concurrency safety section (atomic cache)
- Update DEVELOPER_GUIDE.md with best practices
- Update CLAUDE.md with recent fixes
- Update README.md with stability improvements

All documentation now reflects the bug fixes implemented in Tasks 001-021.
```

## 注意事项

1. **代码示例准确性**: 确保文档中的代码与实际实现一致
2. **链接有效性**: 验证所有内部链接
3. **版本信息**: 标注修复日期（2026-04-07）
4. **用户友好**: 使用清晰的语言和示例
