"""覆盖需求 1-8 的端到端测试。"""
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lake import Lake, LakeError
from lake import aggregate as agg
from lake import quality as dq

SCHEMA = {"turbine_id": "VARCHAR", "ts": "TIMESTAMP", "wind_speed": "DOUBLE",
          "power": "DOUBLE", "alarm_code": "BIGINT", "status": "VARCHAR"}


def make_df(tids, day, hours=2, power_base=1000.0):
    """生成若干风机某天前 hours 小时的秒级数据。"""
    frames = []
    for tid in tids:
        n = hours * 3600
        ts = pd.date_range(day, periods=n, freq="s")
        frames.append(pd.DataFrame({
            "turbine_id": tid, "ts": ts,
            "wind_speed": np.full(n, 8.0),
            "power": np.full(n, power_base),
            "status": "RUN",
        }))
    return pd.concat(frames, ignore_index=True)


@pytest.fixture
def lake(tmp_path):
    lk = Lake(tmp_path / "lake")
    lk.create_table("sec", SCHEMA)
    return lk


def fingerprint(table, snapshot):
    """一张快照的内容指纹:行数 + power 总和 + 首末行。"""
    df = table.df(snapshot)
    return (len(df), round(float(df["power"].sum()), 3),
            df["ts"].min(), df["ts"].max())


# 1. CSV 增量写入,按风机+日期分区 ----------------------------------------

def test_ingest_partitioned(lake, tmp_path):
    t = lake.table("sec")
    df = make_df(["WT01", "WT02"], "2026-10-01")
    csv = tmp_path / "day1.csv"
    df.to_csv(csv, index=False)
    snap = t.ingest_csv(csv)
    assert snap.total_rows == len(df)
    # 分区目录存在
    assert (t.dir / "data/turbine_id=WT01/date=2026-10-01").is_dir()
    assert (t.dir / "data/turbine_id=WT02/date=2026-10-01").is_dir()
    out = t.df()
    assert len(out) == len(df)
    assert abs(out["power"].sum() - df["power"].sum()) < 1e-6


# 2. 每次写入生成快照,任何时间点回查都和当时一致 --------------------------

def test_time_travel_consistency(lake):
    t = lake.table("sec")
    history = []  # (快照提交后的时间戳, seq, 指纹)

    for day, base in [("2026-10-01", 1000.0), ("2026-10-02", 1100.0),
                      ("2026-10-03", 1200.0)]:
        t.ingest_df(make_df(["WT01", "WT02"], day, power_base=base))
        snap = t.snapshot()
        history.append((snap.committed_at, snap.seq, fingerprint(t, snap)))

    # 后续继续折腾:迟到数据、schema 演进、压缩
    t.ingest_df(make_df(["WT01"], "2026-10-01", hours=1, power_base=999.0))
    t.add_column("gearbox_temp", "DOUBLE")
    t.rename_column("status", "run_status")
    t.widen_column("alarm_code", "DOUBLE")
    t.compact(min_files=1)

    # 按 seq 回查
    for _, seq, fp in history:
        assert fingerprint(t, t.snapshot(seq=seq)) == fp, f"快照 #{seq} 回查不一致"
    # 按时间点回查
    for ts, seq, fp in history:
        snap = t.snapshot(as_of=ts + 0.001)
        assert snap.seq == seq
        assert fingerprint(t, snap) == fp, f"as_of 回查快照 #{seq} 不一致"


# 3. 加列/改名/类型放宽,老数据文件不重写 ---------------------------------

def test_schema_evolution_no_rewrite(lake):
    t = lake.table("sec")
    t.ingest_df(make_df(["WT01"], "2026-10-01", hours=1))
    old_files = [f["path"] for f in t.snapshot().files]
    old_mtime = {p: (t.dir / p).stat().st_mtime_ns for p in old_files}

    t.add_column("gearbox_temp", "DOUBLE")
    t.rename_column("status", "run_status")
    t.widen_column("alarm_code", "DOUBLE")

    # 老文件原封不动
    for p in old_files:
        assert (t.dir / p).stat().st_mtime_ns == old_mtime[p]

    df = t.df()
    assert "gearbox_temp" in df.columns and df["gearbox_temp"].isna().all()
    assert "run_status" in df.columns and (df["run_status"] == "RUN").all()
    assert "status" not in df.columns

    # 演进后写入带新列的数据,新老混合读
    new = make_df(["WT01"], "2026-10-02", hours=1)
    new["run_status"] = new.pop("status")
    new["gearbox_temp"] = 55.5
    t.ingest_df(new)
    out = t.df()
    assert out["gearbox_temp"].notna().sum() == len(new)
    assert out["gearbox_temp"].isna().sum() == 3600

    # 不允许收窄
    with pytest.raises(LakeError):
        t.widen_column("power", "BIGINT")


# 4. 迟到数据合并进对应分区 ----------------------------------------------

def test_late_data_merge(lake):
    t = lake.table("sec")
    t.ingest_df(make_df(["WT01"], "2026-10-01", hours=1, power_base=1000.0))
    # 迟到数据:同分区,前 100 行是修正值(power 改成 500),其余是新增
    late = make_df(["WT01"], "2026-10-01", hours=1, power_base=1000.0)
    late.loc[:99, "power"] = 500.0
    late = late.iloc[50:]  # 前 50 行重复修正,后面有 50 行与已有重叠 + 新增? 不,行数相同
    t.ingest_df(late, merge=True)

    df = t.df()
    # 主键 (turbine_id, ts) 唯一
    assert not df.duplicated(["turbine_id", "ts"]).any()
    assert len(df) == 3600
    # 修正值生效(keep=last)
    head = df.sort_values("ts").head(100)
    assert (head.iloc[50:]["power"] == 500.0).all()
    # 仍然只有一个分区目录,老分区文件被新合并文件替换
    assert t.snapshot().files[0]["date"] == "2026-10-01"


# 5. 小文件合并不影响在跑的查询 ------------------------------------------

def test_compact_safe_for_running_queries(lake):
    t = lake.table("sec")
    # 同一分区写 4 次,制造小文件
    for h in range(4):
        df = make_df(["WT01"], "2026-10-01", hours=1)
        df["ts"] += pd.Timedelta(hours=h)
        t.ingest_df(df, merge=False)
    stats = t.partition_stats()
    assert stats["files"].max() == 4

    # 模拟一个正在跑的查询:先拿到旧快照的视图 SQL
    old_snap = t.snapshot()
    old_sql = t.view_sql(old_snap)
    before = fingerprint(t, old_snap)

    t.compact(min_files=3)
    assert t.partition_stats()["files"].max() == 1
    # 压缩后当前视图内容不变
    assert fingerprint(t, t.snapshot()) == before
    # 旧快照(只要还保留着)依然可查,结果不变
    assert fingerprint(t, t.snapshot(seq=old_snap.seq)) == before


# 6. 秒级聚合十分钟,与 SCADA 对比偏差 ------------------------------------

def test_aggregate_and_compare(lake):
    t = lake.table("sec")
    t.ingest_df(make_df(["WT01"], "2026-10-01", hours=2, power_base=1000.0))
    # SCADA 十分钟表:真值 1000,人为加 1% 偏差
    lake.create_table("scada_10min", {"turbine_id": "VARCHAR", "ts": "TIMESTAMP",
                                      "power": "DOUBLE", "wind_speed": "DOUBLE"})
    scada = lake.table("scada_10min")
    ts = pd.date_range("2026-10-01", periods=12, freq="10min")
    scada.ingest_df(pd.DataFrame({"turbine_id": "WT01", "ts": ts,
                                  "power": 1010.0, "wind_speed": 8.0}))

    calc = agg.ensure_calc_table(lake)
    n = agg.aggregate_10min(t, calc)
    assert n == 12
    cmp_df = agg.compare_with_scada(calc, scada)
    assert len(cmp_df) == 12
    assert (cmp_df["power_calc"].round(1) == 1000.0).all()
    assert (cmp_df["deviation"].round(1) == -10.0).all()
    s = agg.deviation_summary(cmp_df, threshold_pct=0.5)
    assert s["over_threshold"] == 12  # 1% 偏差超过 0.5% 阈值


# 7. 空值/跳变/卡死值规则可配 ---------------------------------------------

def test_quality_rules(lake):
    t = lake.table("sec")
    df = make_df(["WT01"], "2026-10-01", hours=1, power_base=1000.0)
    df.loc[0:9, "power"] = np.nan                 # 10 个空值
    df.loc[100, "power"] = 99999.0                # 跳变 + 超范围
    df.loc[200:499, "wind_speed"] = 8.0           # 300 秒卡死
    t.ingest_df(df)

    rules = [
        {"name": "p_null", "column": "power", "type": "null_rate", "max_pct": 0.1},
        {"name": "p_range", "column": "power", "type": "range", "min": 0, "max": 4000},
        {"name": "p_spike", "column": "power", "type": "spike", "max_jump": 500},
        {"name": "w_stuck", "column": "wind_speed", "type": "stuck", "max_repeat": 200},
    ]
    dq.save_rules(t, rules)
    report = dq.run_quality(t)
    r = {x["rule"]["name"]: x for x in report["results"]}
    assert r["p_null"]["violations"] == 10 and not r["p_null"]["ok"]
    assert r["p_range"]["violations"] == 1
    assert r["p_spike"]["violations"] >= 1
    assert r["w_stuck"]["violations"] >= 200
    # 规则确实可配:放宽阈值后通过
    rules[0]["max_pct"] = 50
    dq.save_rules(t, rules)
    assert dq.run_quality(t)["results"][0]["ok"]


# 8. 过期快照清理,保留期可设 ---------------------------------------------

def test_expire_snapshots(lake):
    t = lake.table("sec")
    t.ingest_df(make_df(["WT01"], "2026-10-01", hours=1))          # 快照1: 文件A
    t.ingest_df(make_df(["WT01"], "2026-10-01", hours=1))          # 快照2: 合并后文件B 替换 A
    t.ingest_df(make_df(["WT01"], "2026-10-02", hours=1))          # 快照3: B + C
    t0 = time.time()
    assert len(list(t.data_dir.rglob("*.parquet"))) == 3  # A、B、C

    # 把快照1、2 的提交时间改到 10 天前,模拟过期
    for seq in (1, 2):
        p = t._snapshot_path(seq)
        s = t._read_json(p)
        s["committed_at"] = t0 - 10 * 86400
        t._write_json(p, s)

    r = t.expire_snapshots(retain_hours=24, keep_last=1, now=t0)
    assert r["expired"] == 2
    # 只有文件A 不再被任何保留快照引用(B、C 仍被快照3 引用)
    assert r["files_deleted"] == 1
    assert len(t.snapshots()) == 1
    # 保留的最新快照数据完整可查(两天的合并结果)
    df = t.df()
    assert len(df) == 7200 and not df.duplicated(["turbine_id", "ts"]).any()
    # 保留期放宽:不过期
    r = t.expire_snapshots(retain_hours=24 * 365, keep_last=1)
    assert r["expired"] == 0
