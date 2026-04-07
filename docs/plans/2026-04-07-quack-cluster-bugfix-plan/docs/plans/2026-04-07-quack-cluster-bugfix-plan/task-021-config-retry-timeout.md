# Task 021: 配置更新 - 添加重试和超时配置

## 目标

更新配置文件，添加重试和超时相关的配置项。

## BDD 场景

本任务不直接对应 BDD 场景，但为 Task 013 和 015 提供配置支持。

## 依赖

**depends-on**: ["013", "015"]

## 涉及文件

- `config.yaml` - 添加新配置项
- `quack_cluster/settings.py` - 已在 Task 013 和 015 中修改

## 实现步骤

### 1. 更新 config.yaml

添加以下配置：

```yaml
# 重试配置
max_retries: 3
retry_base_delay: 1.0       # 基础延迟（秒）
retry_max_delay: 30.0       # 最大延迟（秒）
retry_exponential_base: 2.0 # 指数基数

# 超时配置
query_timeout_seconds: 300      # 查询级别超时（5 分钟）
worker_task_timeout_seconds: 120 # Worker 任务超时（2 分钟）
```

### 2. 验证配置加载

创建测试验证配置正确加载：

```python
from quack_cluster.settings import settings

def test_retry_config_loaded():
    assert settings.MAX_RETRIES == 3
    assert settings.RETRY_BASE_DELAY == 1.0
    assert settings.RETRY_MAX_DELAY == 30.0
    assert settings.RETRY_EXPONENTIAL_BASE == 2.0

def test_timeout_config_loaded():
    assert settings.QUERY_TIMEOUT_SECONDS == 300
    assert settings.WORKER_TASK_TIMEOUT_SECONDS == 120
```

## 验证

```bash
python -c "from quack_cluster.settings import settings; print(settings.MAX_RETRIES)"
```

## 提交信息

```
config: add retry and timeout configuration

- Add max_retries, retry delays, exponential base
- Add query and worker task timeout settings
- Update config.yaml with documented defaults

Supports Task 013 (retry) and Task 015 (timeout)
```
