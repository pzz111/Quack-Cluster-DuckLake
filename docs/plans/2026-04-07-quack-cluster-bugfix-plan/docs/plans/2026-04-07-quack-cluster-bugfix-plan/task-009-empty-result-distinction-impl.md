# Task 009: 错误处理 - 空结果区分实现

## 目标

确保 Worker 正确区分空结果和查询失败，空结果返回带 schema 的空表。

## BDD 场景

```gherkin
Feature: Worker 错误传播
  Scenario: 区分空结果和查询失败
    Given Ray 集群正在运行
    And Worker 已初始化
    When Worker 执行查询 "SELECT * FROM users WHERE id = 999"
    And 查询成功但无结果
    Then Worker 应该返回空表（带正确 schema）
    And Worker 不应该抛出异常
    And 日志应该记录 "Query returned 0 rows (valid empty result)"
```

## 依赖

**depends-on**: ["001", "008"]

## 涉及文件

- `quack_cluster/worker.py` - 已在 Task 007 中修改

## 实现步骤

### 验证 Task 007 的实现包含空结果处理

在 Task 007 中，我们已经添加了空结果日志记录：

```python
def query(self, sql_query: str, file_paths: List[str]) -> pa.Table:
    try:
        formatted_paths = ", ".join([f"'{path}'" for path in file_paths])
        query_to_execute = sql_query.format(files=f"[{formatted_paths}]")
        result = self.con.execute(query_to_execute).fetch_arrow_table()
        
        # 空结果是合法的
        if result.num_rows == 0:
            logger.info(f"Query returned 0 rows (valid empty result)")
        
        return result
    except ...:
        # 错误处理
```

### 确保所有 Worker 方法正确处理空结果

验证其他方法也正确处理空结果：

1. **run_join_task**: 空 JOIN 结果是合法的
2. **partition_by_key**: 空分区是合法的
3. **join_partitions**: 空表 JOIN 返回空结果

## 验证

运行测试（应该通过）:

```bash
pytest tests/test_empty_result_distinction.py -v
```

## 提交信息

```
fix: ensure empty results are handled correctly (GREEN)

- Empty results (0 rows) return table with schema
- Empty results do not raise exceptions
- Log message for empty results
- All Worker methods handle empty results correctly

Closes: Task 008 (tests now pass)
```

## 注意事项

1. **Schema 保留**: 空表必须保留列定义
2. **日志级别**: 使用 INFO 级别记录空结果
3. **不是错误**: 空结果是查询的合法输出
