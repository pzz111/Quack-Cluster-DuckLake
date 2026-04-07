# Quack Cluster API 教程

欢迎使用 Quack Cluster API！本文档为您提供如何发送查询以及理解支持的 SQL 操作范围的指南。

---

## 1. 如何查询 API

查询服务非常简单。您需要发送一个 HTTP **POST** 请求，将 SQL 查询放在 JSON 正文中发送到 `/query` 端点。

### 端点

`POST /query`

### 请求体

请求体必须是一个 JSON 对象，包含一个键 `sql`，其值是您要执行的 SQL 查询。

```json
{
  "sql": "SELECT * FROM your_table WHERE condition = 'value';"
}
```

### 响应格式（可选）

您可以使用 `format` 查询参数指定响应格式。

- `?format=json`（默认）：以标准 JSON 数组格式返回结果。
- `?format=arrow`：以二进制 Apache Arrow 流格式返回结果，非常适合高性能客户端和数据科学应用。

### 使用 cURL 的完整示例

以下是如何从 `users` 表查询所有数据并接收 JSON 响应的完整示例。

```bash
curl -X POST "http://127.0.0.1:8000/query?format=json" \
-H "Content-Type: application/json" \
-d '{
    "sql": "SELECT name, city FROM \"users\" ORDER BY name;"
}'
```

---

## 2. 查询范围与 SQL 功能

该 API 利用了 **DuckDB SQL 方言**。您可以通过将数据文件集合（如 Parquet 或 CSV）当作表来查询。系统支持 glob 模式来同时查询多个文件（例如 `"data_part_*.parquet"`）。

以下是经过测试确认的支持的 SQL 功能摘要。

### ✅ 基本操作

- **`SELECT`**：选择全部（`*`）或特定列。
- **`FROM`**：指定表，包括文件 glob 模式（例如 `"data_part_*"`）。
- **`WHERE`**：根据条件过滤行。
- **`GROUP BY`**：为聚合函数对行进行分组。
- **`ORDER BY`**：对结果集进行排序。
- **`LIMIT` / `OFFSET`**：对结果进行分页。

### ✅ 聚合与分组

- **聚合函数**：`COUNT()`、`SUM()`、`AVG()`、`MIN()`、`MAX()`。
- **`HAVING`**：在聚合后过滤分组。

### ✅ 连接与集合操作

- **`INNER JOIN`**：从两个表中选择匹配的记录。
- **`LEFT JOIN`**：从左表选择所有记录，并从右表选择匹配的记录。
- **`FULL OUTER JOIN`**：当左表或右表中有匹配时，选择所有记录。
- **`UNION ALL`**：合并两个或多个 `SELECT` 语句的结果集（包括重复）。

### ✅ 高级 SQL 功能

- **子查询**：将 `SELECT` 语句嵌套在另一个语句中（例如在 `FROM` 或 `WHERE IN (...)` 中）。
- **公用表表达式（CTE）**：使用 `WITH` 子句定义临时的命名结果集。
- **窗口函数**：在一组表行上执行计算（例如 `SUM(...) OVER (PARTITION BY ...)`）。
- **条件逻辑**：使用 `CASE...WHEN...THEN...ELSE...END` 语句。
- **高级 `SELECT` 语法**：
  - `SELECT DISTINCT`：仅返回唯一值。
  - `SELECT DISTINCT ON (...)`：返回每个唯一组的第一个行。
  - `SELECT * EXCLUDE (...)`：选择除指定列外的所有列。
  - `SELECT * REPLACE (...)`：用新表达式替换列的值。
  - `SELECT COLUMNS('<regex>')`：选择与正则表达式匹配的列。
- **日期/时间函数**：如 `DATE_TRUNC` 等操作日期/时间值的函数。

---

## 3. 错误处理

API 将返回标准 HTTP 状态码来指示请求的结果。

- **`400 Bad Request`**：您的 SQL 查询有语法错误。响应正文将包含详细错误信息。
- **`404 Not Found`**：您的查询中指定的表或文件不存在。
- **`200 OK`**：查询成功。空结果集仍然是成功。
