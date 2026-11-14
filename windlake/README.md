# 风电数据湖(windlake)

几百台风机 SCADA 秒级/十分钟数据的小型数据湖:CSV 增量接入,Parquet 存储,
DuckDB 查询,自带 Web 控制台。核心是一条硬保证:**任何时间点回查,结果都和当时写入的一致**。

## 快速开始

```bash
pip install duckdb pyarrow pandas numpy fastapi uvicorn jinja2

# 跑测试(8 个需求点全覆盖)
python -m pytest tests/ -q

# 生成测试数据(默认 10 台 × 3 天;几百台一个月:--turbines 300 --days 30)
python -I scripts/generate_data.py --turbines 10 --days 3 --out data/csv

# 启动控制台
WINDLAKE_HOME=data uvicorn web.app:app --app-dir . --port 8000
# 打开 http://127.0.0.1:8000
```

控制台五个页面:**数据接入 / 表管理 / 快照历史 / 数据质量 / 查询**。

## 需求对照

| # | 需求 | 实现 |
|---|------|------|
| 1 | CSV 增量写入,按风机日期分区 | `Table.ingest_csv/df`,目录 `data/turbine_id=X/date=Y/` |
| 2 | 每次写入生成快照,按时间点回查 | 不可变快照 `_snapshots/NNNNNN.json`;`snapshot(seq=)` / `snapshot(as_of=)` |
| 3 | 加列/改名/类型放宽不重写老数据 | schema 版本化 + 演进日志,读取时按版本对齐(补 NULL / 改名映射 / CAST) |
| 4 | 迟到数据合并进对应分区 | `ingest_df(merge=True)`:按 `(turbine_id, ts)` 去重,只重写受影响分区 |
| 5 | 小文件定时合并,不影响查询 | `compact()`:写新文件+新快照,老文件等过期回收,在跑查询持有旧快照不受影响 |
| 6 | 秒级聚合十分钟,与 SCADA 对比 | `aggregate.aggregate_10min` + `compare_with_scada`,偏差报告 |
| 7 | 空值/跳变/卡死值规则可配 | `quality.py`,规则 JSON 可编辑,报告存档 |
| 8 | 过期快照清理,保留期可设 | `expire_snapshots(retain_hours, keep_last, grace_seconds)` |

## 工作原理

```
<table>/
  _schema/v1.json v2.json ... ops.json   # schema 版本 + 演进日志
  _snapshots/000001.json ...             # 每次变更一个快照:schema版本 + 文件清单
  _quality/rules.json report-*.json      # 质检规则与历史报告
  data/turbine_id=WT001/date=2026-10-01/part-*.parquet
```

- **快照即真相**:快照记录「当时生效的 schema 版本 + 当时的文件清单」。
  回查 = 重放该清单,用该版本的 schema 读那批文件,所以结果必然和当时一致。
- **文件只增不改**:合并、压缩都是写新文件、提交新快照;老文件只有在
  所有保留快照都不再引用它、且过了宽限期(`grace_seconds`)后才物理删除,
  正在跑的查询持有的文件清单永远有效。
- **schema 演进零重写**:加列 → 老文件读时补 NULL;改名 → 读时按演进日志
  把老列名映射到新名;类型放宽(如 BIGINT→DOUBLE)→ 读时 CAST。收窄直接拒绝。
- **回查历史快照时用当时的 schema**,所以改名前能看到 `status`,改名后看到
  `run_status`,各是各的样子。

## 代码结构

```
lake/core.py       存储引擎:快照、写入合并、压缩、过期、schema 演进
lake/quality.py    质检规则引擎(null_rate / range / spike / stuck)
lake/aggregate.py  秒级→十分钟聚合、与 SCADA 十分钟对比偏差
web/app.py         FastAPI 控制台(五个页面)
scripts/generate_data.py  测试数据生成(带异常注入)
tests/test_lake.py 8 个需求点的端到端测试
```

## 常用操作

```python
from lake import Lake
lake = Lake("data/lake")
t = lake.table("scada_seconds")

t.ingest_csv("scada_seconds_2026-10-01.csv")   # 增量写入,迟到数据自动合并
t.add_column("gearbox_temp", "DOUBLE")          # 加列,老数据不动
t.rename_column("status", "run_status")         # 改名
t.compact(min_files=3)                          # 小文件合并
t.expire_snapshots(retain_hours=168, keep_last=5)  # 快照保留 7 天

snap = t.snapshot(as_of=some_timestamp)         # 时间点回查
df = t.df(snapshot=snap)
```

## 说明

- 单写者模型(进程内锁),适合场站侧单机部署;多进程并发写需要再加文件锁。
- 测试数据规模:`--turbines 300 --days 30` 约 7.8 亿秒级行,生成和写入都按天
  分块,建议分批 ingest 后再统一 compact。
