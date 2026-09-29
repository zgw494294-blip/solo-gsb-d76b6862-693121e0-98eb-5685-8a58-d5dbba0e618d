# 多副本离线写入版本核对服务

用 Python / FastAPI 实现的 HTTP API：接收 2～8 个副本离线产生的、可乱序提交的
`put` / `delete` 事件（每条带向量时钟），先做整批一致性校验，再按向量时钟偏序
归并出每个 key 的有效版本（存活值 / 已删除 / 冲突）。**请求数据只做内存计算，不落库。**

## 校验规则（任一违反，整批返回 `422`，`detail` 中指出违规事件）

1. `replica_ids`：2～8 个非空且唯一的副本 ID；`events`：1～120 条。
2. 每个副本的 `seq` 从 1 开始，**不得重复且必须连续**。
3. 向量时钟长度等于副本数，各分量为非负整数；事件的**本副本分量必须等于 `seq`**。
4. 时钟**任何分量都不得指向缺失事件**（没有事件的副本，其分量只能为 0）。
5. 对事件 `e`（副本 r，seq=s），以下前驱事件的时钟都必须逐分量 `<=` 当前时钟、
   且至少一个分量严格更小：
   - 同副本上一事件 `(r, s-1)`（`s > 1` 时）；
   - 对每个其他副本 q，其时钟分量 `c[q] > 0` 时所指的最新事件 `(q, c[q])`。

## 归并规则

- 同一 `key` 的事件按向量时钟偏序比较（`a < b`：逐分量 `<=` 且至少一项 `<`），
  取所有**极大事件**。
- 恰有 1 个极大事件：`put` → `status="present"` 并给出 `value`；
  `delete` → `status="deleted"`。
- 有多个极大事件：`status="conflict"`，**即使各事件 value 完全相同也报冲突**。
- 结果按 `key` 字典序排序；冲突事件按 `replica_id`、`seq` 排序。

## 启动命令（单命令）

```bash
docker compose up --build
```

启动后监听：

- 服务地址 / 端口：`http://localhost:8000`（容器内 8000，见 `docker-compose.yml`）
- 健康检查：`GET http://localhost:8000/health`
- 接口文档（Swagger UI）：`http://localhost:8000/docs`
- OpenAPI：`http://localhost:8000/openapi.json`

> 不使用 Docker 时也可本地运行（Python 3.11+）：
> `pip install -r requirements.txt && uvicorn app.main:app --host 0.0.0.0 --port 8000`

## 可执行 HTTP 示例

### 1. 正常请求（事件故意乱序提交）

```bash
curl -s -X POST http://localhost:8000/resolve \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r2", "seq": 2, "op": "put",    "key": "color", "value": "blue", "clock": [1, 2]},
      {"replica_id": "r1", "seq": 1, "op": "put",    "key": "color", "value": "red",  "clock": [1, 0]},
      {"replica_id": "r2", "seq": 1, "op": "put",    "key": "color", "value": "red",  "clock": [0, 1]},
      {"replica_id": "r1", "seq": 2, "op": "delete", "key": "temp",                  "clock": [2, 1]},
      {"replica_id": "r1", "seq": 3, "op": "put",    "key": "name",  "value": "final","clock": [3, 2]}
    ]
  }'
```

响应 `200`：

```json
{
  "results": [
    {
      "key": "color",
      "status": "present",
      "value": "blue",
      "events": [{"replica_id": "r2", "seq": 2, "clock": [1, 2], "value": "blue"}]
    },
    {
      "key": "name",
      "status": "present",
      "value": "final",
      "events": [{"replica_id": "r1", "seq": 3, "clock": [3, 2], "value": "final"}]
    },
    {
      "key": "temp",
      "status": "deleted",
      "value": null,
      "events": [{"replica_id": "r1", "seq": 2, "clock": [2, 1], "value": null}]
    }
  ]
}
```

### 2. 同值并发仍然冲突

```bash
curl -s -X POST http://localhost:8000/resolve \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r1", "seq": 1, "op": "put", "key": "color", "value": "red", "clock": [1, 0]},
      {"replica_id": "r2", "seq": 1, "op": "put", "key": "color", "value": "red", "clock": [0, 1]}
    ]
  }'
```

响应 `200`：

```json
{
  "results": [
    {
      "key": "color",
      "status": "conflict",
      "value": null,
      "events": [
        {"replica_id": "r1", "seq": 1, "clock": [1, 0], "value": "red"},
        {"replica_id": "r2", "seq": 1, "clock": [0, 1], "value": "red"}
      ]
    }
  ]
}
```

### 3. 校验失败：seq 不连续（缺失 seq=2），整批 `422`

```bash
curl -s -i -X POST http://localhost:8000/resolve \
  -H 'Content-Type: application/json' \
  -d '{
    "replica_ids": ["r1", "r2"],
    "events": [
      {"replica_id": "r1", "seq": 1, "op": "put", "key": "k", "value": "a", "clock": [1, 0]},
      {"replica_id": "r1", "seq": 3, "op": "put", "key": "k", "value": "b", "clock": [3, 0]}
    ]
  }'
```

响应 `422`：

```json
{
  "detail": [
    {
      "replica_id": "r1",
      "seq": 3,
      "key": "k",
      "events_index": 1,
      "rule": "seq_must_be_contiguous",
      "message": "副本 'r1' 的 seq 不连续：期望 2，实际为 3"
    }
  ]
}
```

其他 `rule` 取值：`duplicate_seq`、`seq_must_start_at_1`、
`own_clock_component_must_equal_seq`、`clock_points_to_missing_event`、
`predecessor_clock_not_le`、`predecessor_clock_must_advance`。

### 4. 健康检查

```bash
curl -s http://localhost:8000/health
# {"status":"ok"}
```

## 请求 / 响应字段

| 字段 | 说明 |
| --- | --- |
| `replica_ids[]` | 2～8 个非空、唯一的副本 ID（字符串） |
| `events[].replica_id` | 必须出现在 `replica_ids` 中 |
| `events[].seq` | 该副本内从 1 起、不重复且连续的整数 |
| `events[].op` | `"put"`（必须带字符串 `value`，允许空串）或 `"delete"`（不得带 `value`） |
| `events[].key` | 非空 key |
| `events[].clock[]` | 覆盖全部副本的非负整数向量时钟，长度等于副本数，分量顺序与 `replica_ids` 一致 |

## 项目结构

```
app/
  main.py      # FastAPI 应用、/health、/resolve、422 异常处理
  models.py    # pydantic v2 请求/响应模型
  solver.py    # 整批校验 + 向量时钟偏序归并
tests/         # pytest 单元与 HTTP 冒烟测试（16 个）
Dockerfile
docker-compose.yml
requirements.txt
```

运行测试：

```bash
pip install -r requirements.txt pytest httpx
pytest -q
```
