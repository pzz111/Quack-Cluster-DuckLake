# Quack-Cluster 关键 Bug 修复实施计划

## 目标

修复 Quack-Cluster 中影响系统稳定性的关键 Bug，包括资源泄漏、错误处理、并发安全、容错性和文件验证问题。

## 背景

基于 [2026-04-07 代码审查设计](../2026-04-07-quack-cluster-code-review-and-improvements/_index.md)，我们识别了 18 个关键问题。本计划专注于修复这些问题，优先处理 P0 级别的稳定性问题。

### 当前状态 vs 目标状态

| 维度 | 当前状态 | 目标状态 |
|------|---------|---------|
| **资源管理** | DuckDB 连接泄漏，临时表未注销 | 所有资源使用上下文管理器自动清理 |
| **错误处理** | Worker 静默失败，返回空表 | 结构化异常传播，明确区分空结果和错误 |
| **并发安全** | 缓存存在竞态条件 | 原子化缓存操作，无竞态条件 |
| **容错性** | 固定延迟重试，所有错误都重试 | 指数退避，智能错误分类，超时保护 |
| **文件验证** | 无文件存在性检查 | 操作前验证文件存在 |

## 约束条件

1. **向后兼容**: API 接口保持不变
2. **性能**: 修复不应导致 >5% 的性能回退
3. **测试优先**: 所有修复必须有对应的测试
4. **渐进式部署**: 支持分阶段部署和回滚

## 架构决策

### ADR-001: 使用上下文管理器管理资源

**决策**: 所有 DuckDB 连接和临时表使用 Python 上下文管理器

**理由**:
- 自动资源清理，即使发生异常
- 符合 Python 最佳实践
- 代码更简洁易读

**替代方案**: try-finally 块（更冗长，容易遗漏）

### ADR-002: 结构化异常类

**决策**: 创建 `WorkerError` 异常类，包含错误分类和上下文

**理由**:
- 支持智能重试决策
- 提供丰富的调试信息
- 区分可重试和不可重试错误

**替代方案**: 使用标准异常（缺少上下文信息）

### ADR-003: 指数退避重试

**决策**: 实现指数退避重试策略，基于错误类别决定是否重试

**理由**:
- 减少对临时故障系统的压力
- 避免无意义的重试（逻辑错误）
- 行业标准做法

**替代方案**: 固定延迟（不适应网络抖动）

## 执行计划

```yaml
tasks:
  - id: "001"
    subject: "创建 WorkerError 异常类"
    slug: "worker-error-class"
    type: "impl"
    depends-on: []
  
  - id: "002"
    subject: "资源管理 - DuckDB 连接上下文管理器测试"
    slug: "duckdb-connection-context-test"
    type: "test"
    depends-on: []
  
  - id: "003"
    subject: "资源管理 - DuckDB 连接上下文管理器实现"
    slug: "duckdb-connection-context-impl"
    type: "impl"
    depends-on: ["002"]
  
  - id: "004"
    subject: "资源管理 - 临时表上下文管理器测试"
    slug: "temp-table-context-test"
    type: "test"
    depends-on: []
  
  - id: "005"
    subject: "资源管理 - 临时表上下文管理器实现"
    slug: "temp-table-context-impl"
    type: "impl"
    depends-on: ["004"]
  
  - id: "006"
    subject: "错误处理 - Worker 错误传播测试"
    slug: "worker-error-propagation-test"
    type: "test"
    depends-on: ["001"]
  
  - id: "007"
    subject: "错误处理 - Worker 错误传播实现"
    slug: "worker-error-propagation-impl"
    type: "impl"
    depends-on: ["001", "006"]
  
  - id: "008"
    subject: "错误处理 - 空结果区分测试"
    slug: "empty-result-distinction-test"
    type: "test"
    depends-on: ["001"]
  
  - id: "009"
    subject: "错误处理 - 空结果区分实现"
    slug: "empty-result-distinction-impl"
    type: "impl"
    depends-on: ["001", "008"]
  
  - id: "010"
    subject: "并发安全 - 缓存原子操作测试"
    slug: "cache-atomic-ops-test"
    type: "test"
    depends-on: []
  
  - id: "011"
    subject: "并发安全 - 缓存原子操作实现"
    slug: "cache-atomic-ops-impl"
    type: "impl"
    depends-on: ["010"]
  
  - id: "012"
    subject: "容错性 - 指数退避重试测试"
    slug: "exponential-backoff-test"
    type: "test"
    depends-on: ["001"]
  
  - id: "013"
    subject: "容错性 - 指数退避重试实现"
    slug: "exponential-backoff-impl"
    type: "impl"
    depends-on: ["001", "012"]
  
  - id: "014"
    subject: "容错性 - 查询超时测试"
    slug: "query-timeout-test"
    type: "test"
    depends-on: []
  
  - id: "015"
    subject: "容错性 - 查询超时实现"
    slug: "query-timeout-impl"
    type: "impl"
    depends-on: ["014"]
  
  - id: "016"
    subject: "文件验证 - 文件存在性检查测试"
    slug: "file-validation-test"
    type: "test"
    depends-on: []
  
  - id: "017"
    subject: "文件验证 - 文件存在性检查实现"
    slug: "file-validation-impl"
    type: "impl"
    depends-on: ["016"]
  
  - id: "018"
    subject: "集成测试 - 端到端资源管理验证"
    slug: "integration-resource-management"
    type: "test"
    depends-on: ["003", "005"]
  
  - id: "019"
    subject: "集成测试 - 端到端错误处理验证"
    slug: "integration-error-handling"
    type: "test"
    depends-on: ["007", "009"]
  
  - id: "020"
    subject: "集成测试 - 端到端容错性验证"
    slug: "integration-fault-tolerance"
    type: "test"
    depends-on: ["013", "015"]
  
  - id: "021"
    subject: "配置更新 - 添加重试和超时配置"
    slug: "config-retry-timeout"
    type: "config"
    depends-on: ["013", "015"]
  
  - id: "022"
    subject: "文档更新 - 更新架构和开发指南"
    slug: "docs-update"
    type: "docs"
    depends-on: ["003", "005", "007", "009", "011", "013", "015", "017"]
```

## 任务文件引用

- [Task 001: 创建 WorkerError 异常类](./task-001-worker-error-class-impl.md)
- [Task 002: 资源管理 - DuckDB 连接上下文管理器测试](./task-002-duckdb-connection-context-test.md)
- [Task 003: 资源管理 - DuckDB 连接上下文管理器实现](./task-003-duckdb-connection-context-impl.md)
- [Task 004: 资源管理 - 临时表上下文管理器测试](./task-004-temp-table-context-test.md)
- [Task 005: 资源管理 - 临时表上下文管理器实现](./task-005-temp-table-context-impl.md)
- [Task 006: 错误处理 - Worker 错误传播测试](./task-006-worker-error-propagation-test.md)
- [Task 007: 错误处理 - Worker 错误传播实现](./task-007-worker-error-propagation-impl.md)
- [Task 008: 错误处理 - 空结果区分测试](./task-008-empty-result-distinction-test.md)
- [Task 009: 错误处理 - 空结果区分实现](./task-009-empty-result-distinction-impl.md)
- [Task 010: 并发安全 - 缓存原子操作测试](./task-010-cache-atomic-ops-test.md)
- [Task 011: 并发安全 - 缓存原子操作实现](./task-011-cache-atomic-ops-impl.md)
- [Task 012: 容错性 - 指数退避重试测试](./task-012-exponential-backoff-test.md)
- [Task 013: 容错性 - 指数退避重试实现](./task-013-exponential-backoff-impl.md)
- [Task 014: 容错性 - 查询超时测试](./task-014-query-timeout-test.md)
- [Task 015: 容错性 - 查询超时实现](./task-015-query-timeout-impl.md)
- [Task 016: 文件验证 - 文件存在性检查测试](./task-016-file-validation-test.md)
- [Task 017: 文件验证 - 文件存在性检查实现](./task-017-file-validation-impl.md)
- [Task 018: 集成测试 - 端到端资源管理验证](./task-018-integration-resource-management.md)
- [Task 019: 集成测试 - 端到端错误处理验证](./task-019-integration-error-handling.md)
- [Task 020: 集成测试 - 端到端容错性验证](./task-020-integration-fault-tolerance.md)
- [Task 021: 配置更新 - 添加重试和超时配置](./task-021-config-retry-timeout.md)
- [Task 022: 文档更新 - 更新架构和开发指南](./task-022-docs-update.md)

## BDD 覆盖

所有 BDD 场景已映射到对应任务：

| BDD 场景 | 任务 ID |
|---------|---------|
| 特性 1.1: DuckDB 连接自动关闭 | 002, 003 |
| 特性 1.2: 临时表自动注销 | 004, 005 |
| 特性 2.1: Worker 错误传播 | 006, 007 |
| 特性 2.1: 区分空结果和查询失败 | 008, 009 |
| 特性 2.2: 结构化错误响应 | 007 |
| 特性 3.1: 缓存原子操作 | 010, 011 |
| 特性 4.1: 指数退避重试 | 012, 013 |
| 特性 4.2: 查询超时保护 | 014, 015 |
| 特性 5.1: 文件存在性检查 | 016, 017 |

## 依赖链

```
001 (WorkerError 类)
├── 006 (Worker 错误传播测试)
│   └── 007 (Worker 错误传播实现)
│       └── 019 (集成测试 - 错误处理)
├── 008 (空结果区分测试)
│   └── 009 (空结果区分实现)
│       └── 019 (集成测试 - 错误处理)
├── 012 (指数退避测试)
│   └── 013 (指数退避实现)
│       ├── 020 (集成测试 - 容错性)
│       └── 021 (配置更新)
└── (被多个任务依赖)

002 (DuckDB 连接测试)
└── 003 (DuckDB 连接实现)
    ├── 018 (集成测试 - 资源管理)
    └── 022 (文档更新)

004 (临时表测试)
└── 005 (临时表实现)
    ├── 018 (集成测试 - 资源管理)
    └── 022 (文档更新)

010 (缓存原子操作测试)
└── 011 (缓存原子操作实现)
    └── 022 (文档更新)

014 (查询超时测试)
└── 015 (查询超时实现)
    ├── 020 (集成测试 - 容错性)
    ├── 021 (配置更新)
    └── 022 (文档更新)

016 (文件验证测试)
└── 017 (文件验证实现)
    └── 022 (文档更新)
```

## 提交边界

### Commit 1: 错误处理基础设施
- Task 001: WorkerError 异常类

### Commit 2: 资源管理 - DuckDB 连接
- Task 002: 测试
- Task 003: 实现

### Commit 3: 资源管理 - 临时表
- Task 004: 测试
- Task 005: 实现

### Commit 4: 错误处理 - Worker 错误传播
- Task 006: 测试
- Task 007: 实现

### Commit 5: 错误处理 - 空结果区分
- Task 008: 测试
- Task 009: 实现

### Commit 6: 并发安全 - 缓存原子操作
- Task 010: 测试
- Task 011: 实现

### Commit 7: 容错性 - 指数退避重试
- Task 012: 测试
- Task 013: 实现

### Commit 8: 容错性 - 查询超时
- Task 014: 测试
- Task 015: 实现

### Commit 9: 文件验证
- Task 016: 测试
- Task 017: 实现

### Commit 10: 集成测试
- Task 018: 资源管理集成测试
- Task 019: 错误处理集成测试
- Task 020: 容错性集成测试

### Commit 11: 配置和文档
- Task 021: 配置更新
- Task 022: 文档更新

## 验证标准

### 单元测试
- 所有新代码覆盖率 ≥ 80%
- 所有 BDD 场景有对应测试

### 集成测试
- 端到端资源管理验证
- 端到端错误处理验证
- 端到端容错性验证

### 性能测试
- 查询延迟回退 < 5%
- 资源清理开销 < 1ms

### 资源泄漏测试
- 1000 次查询后无连接泄漏
- 1000 次查询后无临时表泄漏

## 风险与缓解

| 风险 | 影响 | 可能性 | 缓解措施 |
|------|------|--------|---------|
| 向后兼容性破坏 | 高 | 低 | 保持 API 接口不变，添加功能测试 |
| 性能回退 | 中 | 低 | 性能基准测试，轻量级资源清理 |
| 引入新 Bug | 高 | 中 | 全面测试覆盖，代码审查 |
| 部署复杂度 | 低 | 低 | 配置向后兼容，提供迁移指南 |

## 估算

- **总任务数**: 22 个任务
- **预计工作量**: 7-8 天
- **关键路径**: 001 → 006 → 007 → 019 → 022

## 下一步

1. 获取用户批准
2. 开始执行 Task 001
3. 按照依赖链顺序执行后续任务
