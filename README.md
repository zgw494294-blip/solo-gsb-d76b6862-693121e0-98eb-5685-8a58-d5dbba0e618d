# 多副本离线写入核对 API

接收 2～8 个副本离线产生的 1～120 条 put/delete 事件（可乱序），校验
向量时钟因果关系，并按向量时钟偏序解析出每个 key 的有效版本：

- 唯一极大事件是 put → 返回其值
- 唯一极大事件是 delete → 标记为已删除
- 存在多个极大事件（互不可比，即使值相同）→ 报告冲突

服务完全无状态，**请求数据不落库**。

## 启动

需要 Docker 与 Docker Compose v2（`docker compose` 子命令）。

```bash
docker compose up --build -d
```

停止：

```bash
docker compose down
```

## 访问地址与端口

| 用途 | 地址 |
| --- | --- |
| 健康检查 | <http://localhost:8000/health> |
| 解析接口 | `POST http://localhost:8000/api/resolve` |
| Swagger UI | <http://localhost:8000/docs> |
| OpenAPI 文档 | <http://localhost:8000/openapi.json> |

容器内监听 `0.0.0.0:8000`，映射到宿主机 **8000** 端口。

## 请求格式

```json
{
  "replica_ids": ["r1", "r2"],
  "events": [
    {
      "replica_id": "r1",
      "seq": 1,
      "key": "name",
      "put": "alice",
      "clock": {"r1": 1, "r2": 0}
    },
    {
      "replica_id": "r2",
      "seq": 1,
      "key": "name",
      "delete": "delete",
      "clock": {"r1": 0, "r2": 1}
    }
  ]
}
```

约束：

- `replica_ids`：2～8 个非空且唯一的副本 ID。
- 每个事件属于其中某个副本；`seq` 从 1 开始，同一副本内不重复且必须连续
  （事件在请求中可乱序）。
- `put`（字符串，允许空串）与 `delete`（值必须是字符串 `"delete"`）二选一。
- `clock` 必须覆盖**全部**副本且无多余分量，分量为非负整数；
  本副本分量必须等于本事件 `seq`。
- 时钟任何分量都不得指向不存在的事件（0 表示尚不知道该副本的事件）。
- 本副本上一事件（`seq - 1`）以及其他副本分量所指的最新事件，其时钟必须
  逐分量 `<=` 当前时钟且至少一个分量严格更小。
- 任何违例整批返回 **HTTP 422**，错误详情中带 `event_index`（请求 `events`
  数组下标）指出具体事件。

## 可执行 HTTP 示例

### 1. 冲突：两个副本并发改同一个 key（值相同也冲突）

```bash
curl -sS -X POST http://localhost:8000/api/resolve \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 0, "r2": 1}},
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}}
    ]
  }'
```

响应（结果按 key 排序；冲突事件按 `replica_id`、`seq` 排序）：

```json
{
  "results": [
    {
      "key": "name",
      "state": "conflict",
      "events": [
        {"replica_id": "r1", "seq": 1, "op": "put", "value": "alice",
         "clock": {"r1": 1, "r2": 0}},
        {"replica_id": "r2", "seq": 1, "op": "put", "value": "alice",
         "clock": {"r1": 0, "r2": 1}}
      ]
    }
  ]
}
```

### 2. 同步后再写入：put 生效、旧 delete 被遮蔽

```bash
curl -sS -X POST http://localhost:8000/api/resolve \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 0}},
      {"replica_id": "r1", "seq": 2, "key": "session", "delete": "delete",
       "clock": {"r1": 2, "r2": 0}},
      {"replica_id": "r2", "seq": 1, "key": "name", "put": "alice",
       "clock": {"r1": 1, "r2": 1}},
      {"replica_id": "r1", "seq": 3, "key": "name", "put": "alice2",
       "clock": {"r1": 3, "r2": 1}}
    ]
  }'
```

响应：

```json
{
  "results": [
    {
      "key": "name",
      "state": "put",
      "value": "alice2",
      "event": {"replica_id": "r1", "seq": 3, "op": "put", "value": "alice2",
                "clock": {"r1": 3, "r2": 1}}
    },
    {
      "key": "session",
      "state": "deleted",
      "event": {"replica_id": "r1", "seq": 2, "op": "delete", "value": null,
                "clock": {"r1": 2, "r2": 0}}
    }
  ]
}
```

### 3. 非法批次：时钟本副本分量不等于 seq → 422

```bash
curl -sS -i -X POST http://localhost:8000/api/resolve \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r1", "seq": 1, "key": "k", "put": "v",
       "clock": {"r1": 2, "r2": 0}}
    ]
  }'
```

返回 `422 Unprocessable Entity`，body 例如：

```json
{
  "detail": [
    {
      "code": "clock_self_component",
      "message": "clock['r1'] = 2 must equal seq = 1",
      "event_index": 0
    }
  ]
}
```

## 本地开发（不使用 Docker）

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```
