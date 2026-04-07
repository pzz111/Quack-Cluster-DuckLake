# BDD 规范：Quack-Cluster 关键 Bug 修复

本文档定义了修复 Quack-Cluster 关键 Bug 的行为驱动开发（BDD）规范。

## 特性 1: 资源管理

### 场景 1.1: DuckDB 连接自动关闭
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

### 场景 1.2: 临时表自动注销
```gherkin
Feature: 临时表内存管理
  作为系统开发者
  我希望临时表在使用后自动注销
  以便避免内存泄漏

  Scenario: CTE 临时表在使用后注销
    Given 系统启动并准备接收查询
    When 用户提交包含 CTE 的查询
      """
      WITH temp AS (SELECT * FROM users)
      SELECT * FROM temp
      """
    And 查询成功执行
    Then CTE 临时表 "temp" 应该被注销
    And 内存中不应该保留该临时表

  Scenario: 子查询临时表在使用后注销
    Given 系统启动并准备接收查询
    When 用户提交包含子查询的查询
      """
      SELECT * FROM (SELECT * FROM users) AS subq
      """
    And 查询成功执行
    Then 子查询临时表应该被注销
    And 内存中不应该保留该临时表

  Scenario: 多个 CTE 的临时表管理
    Given 系统启动并准备接收查询
    When 用户提交包含多个 CTE 的查询
      """
      WITH cte1 AS (SELECT * FROM users),
           cte2 AS (SELECT * FROM orders)
      SELECT * FROM cte1 JOIN cte2 ON cte1.id = cte2.user_id
      """
    And 查询成功执行
    Then 所有 CTE 临时表应该被注销
    And 内存中不应该保留任何临时表
```

## 特性 2: 错误处理

### 场景 2.1: Worker 错误传播
```gherkin
Feature: Worker 错误传播
  作为系统开发者
  我希望 Worker 错误被正确传播
  以便能够诊断和修复问题

  Scenario: Worker 查询失败时抛出异常
    Given Ray 集群正在运行
    And Worker 已初始化
    When Worker 执行无效查询
    Then Worker 应该抛出 WorkerError 异常
    And 异常应该包含错误类别
    And 异常应该包含原始查询
    And 异常应该包含文件路径
    And 异常不应该返回空表

  Scenario: 区分空结果和查询失败
    Given Ray 集群正在运行
    And Worker 已初始化
    When Worker 执行查询 "SELECT * FROM users WHERE id = 999"
    And 查询成功但无结果
    Then Worker 应该返回空表（带正确 schema）
    And Worker 不应该抛出异常
    And 日志应该记录 "Query returned 0 rows (valid empty result)"

  Scenario: 文件 I/O 错误分类为可重试
    Given Ray 集群正在运行
    And Worker 已初始化
    When Worker 尝试读取不存在的文件
    Then Worker 应该抛出 WorkerError
    And 错误类别应该是 "network"
    And 错误应该被标记为可重试

  Scenario: SQL 语法错误分类为不可重试
    Given Ray 集群正在运行
    And Worker 已初始化
    When Worker 执行语法错误的 SQL
    Then Worker 应该抛出 WorkerError
    And 错误类别应该是 "logic"
    And 错误应该被标记为不可重试
```

### 场景 2.2: 结构化错误响应
```gherkin
Feature: 结构化错误响应
  作为 API 用户
  我希望收到结构化的错误响应
  以便能够程序化处理错误

  Scenario: API 返回结构化错误
    Given 系统启动并准备接收查询
    When 用户提交无效查询
    Then API 应该返回 400 状态码
    And 响应应该包含错误类型
    And 响应应该包含错误消息
    And 响应应该包含错误详情

  Scenario: 超时错误返回 408 状态码
    Given 系统启动并准备接收查询
    When 用户提交长时间运行的查询
    And 查询超过超时限制
    Then API 应该返回 408 状态码
    And 响应应该包含超时时长
    And 响应应该包含建议的解决方案
```

## 特性 3: 并发安全

### 场景 3.1: 缓存原子操作
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

## 特性 4: 容错性

### 场景 4.1: 指数退避重试
```gherkin
Feature: 智能重试策略
  作为系统管理员
  我希望系统使用指数退避重试
  以便更好地处理临时故障

  Scenario: 网络错误使用指数退避重试
    Given Ray 集群正在运行
    And Worker 任务配置为最多重试 3 次
    When Worker 任务因网络错误失败
    Then 系统应该在 1 秒后重试第 1 次
    And 系统应该在 2 秒后重试第 2 次
    And 系统应该在 4 秒后重试第 3 次
    And 延迟不应该超过最大延迟限制

  Scenario: 逻辑错误不应该重试
    Given Ray 集群正在运行
    And Worker 任务配置为最多重试 3 次
    When Worker 任务因 SQL 语法错误失败
    Then 系统不应该重试
    And 系统应该立即抛出异常
    And 异常应该包含原始错误信息

  Scenario: 重试次数达到上限后失败
    Given Ray 集群正在运行
    And Worker 任务配置为最多重试 3 次
    When Worker 任务持续因网络错误失败
    And 已经重试 3 次
    Then 系统应该抛出最终失败异常
    And 异常应该包含所有重试历史
    And 异常应该包含失败的输入
```

### 场景 4.2: 查询超时保护
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

  Scenario: Worker 任务超时被终止
    Given 系统配置 Worker 任务超时为 120 秒
    When Worker 任务执行超过 120 秒
    Then 任务应该被终止
    And Executor 应该收到超时异常
    And 任务应该被标记为失败

  Scenario: 快速查询不受超时影响
    Given 系统配置查询超时为 300 秒
    When 用户提交一个需要 10 秒的查询
    Then 查询应该正常完成
    And 不应该触发超时机制
```

## 特性 5: 文件验证

### 场景 5.1: 文件存在性检查
```gherkin
Feature: 文件验证
  作为系统开发者
  我希望文件操作前进行验证
  以便避免文件不存在导致的错误

  Scenario: 计算表大小前验证文件存在
    Given Planner 发现了表 "users" 的文件列表
    When Planner 计算表大小
    And 其中一个文件在计算前被删除
    Then Planner 应该抛出 FileNotFoundError
    And 错误应该列出缺失的文件
    And 错误应该包含清晰的错误消息

  Scenario: 所有文件存在时正常计算大小
    Given Planner 发现了表 "users" 的文件列表
    When Planner 计算表大小
    And 所有文件都存在
    Then Planner 应该返回正确的总大小
    And 不应该抛出异常
```

## 测试策略

### 单元测试
- **覆盖率目标**: 核心逻辑 80%+
- **关键模块**:
  - `worker.py`: 错误处理和资源清理
  - `executor.py`: 重试逻辑和超时机制
  - `coordinator.py`: 缓存并发安全
  - `planner.py`: 文件验证

### 集成测试
- **端到端场景**: 完整查询流程
- **并发测试**: 多个并发查询
- **故障注入**: 模拟网络故障、文件缺失
- **资源泄漏检测**: 长时间运行测试

### 性能测试
- **基准测试**: 修复前后性能对比
- **负载测试**: 高并发场景
- **资源监控**: 内存、文件描述符使用

## 验收标准

### 必须通过的测试
1. ✅ 所有 BDD 场景通过
2. ✅ 单元测试覆盖率 ≥ 80%
3. ✅ 集成测试全部通过
4. ✅ 无资源泄漏（24 小时运行测试）
5. ✅ 性能无显著回退（< 5%）

### 代码质量标准
1. ✅ 所有错误都有明确的错误消息
2. ✅ 所有资源使用上下文管理器
3. ✅ 所有异常都被正确分类
4. ✅ 所有并发操作都是线程安全的

## 测试数据

### 测试表
- `users`: 1000 行，3 列
- `orders`: 10000 行，5 列
- `large_table`: 1000000 行，10 列（用于超时测试）

### 测试场景
- 正常查询
- 空结果查询
- 语法错误查询
- 超时查询
- 并发查询（10-100 并发）
- 资源泄漏测试（1000+ 查询）

## 持续集成

### CI 流程
1. 代码提交触发 CI
2. 运行 linter 和格式检查
3. 运行单元测试
4. 运行集成测试
5. 运行性能基准测试
6. 生成覆盖率报告
7. 检查资源泄漏

### 质量门禁
- 测试通过率 100%
- 代码覆盖率 ≥ 80%
- 性能回退 < 5%
- 无资源泄漏
- 无安全漏洞
