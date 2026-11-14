"""数据质量规则引擎:空值率 / 范围 / 跳变 / 卡死值,规则可配,结果存档。"""
from __future__ import annotations

import json
import time

from .core import Table

DEFAULT_RULES = [
    {"name": "power_null", "column": "power", "type": "null_rate", "max_pct": 1.0},
    {"name": "power_range", "column": "power", "type": "range", "min": -200, "max": 4000},
    {"name": "power_spike", "column": "power", "type": "spike", "max_jump": 800},
    {"name": "wind_stuck", "column": "wind_speed", "type": "stuck", "max_repeat": 300},
]


def _rule_sql(rule: dict, table: str) -> tuple[str, str]:
    """返回 (违规行数 SQL, 违规样本 SQL)。"""
    col = f'"{rule["column"]}"'
    t = f'"{table}"'
    rt = rule["type"]
    if rt == "null_rate":
        return (f"SELECT count(*) FILTER (WHERE {col} IS NULL) FROM {t}",
                f"SELECT turbine_id, ts FROM {t} WHERE {col} IS NULL LIMIT 20")
    if rt == "range":
        cond = f"{col} < {rule['min']} OR {col} > {rule['max']}"
        return (f"SELECT count(*) FROM {t} WHERE {cond}",
                f"SELECT turbine_id, ts, {col} FROM {t} WHERE {cond} LIMIT 20")
    if rt == "spike":
        w = f"lag({col}) OVER (PARTITION BY turbine_id ORDER BY ts)"
        base = f"(SELECT turbine_id, ts, {col} AS v, {w} AS prev FROM {t})"
        cond = f"prev IS NOT NULL AND abs(v - prev) > {rule['max_jump']}"
        return (f"SELECT count(*) FROM {base} WHERE {cond}",
                f"SELECT turbine_id, ts, v, prev FROM {base} WHERE {cond} LIMIT 20")
    if rt == "stuck":
        # 同一台风机连续取同一个值的游程,长度 >= max_repeat 判为卡死
        base = f"""
        WITH s AS (
          SELECT turbine_id, ts, {col} AS v,
                 row_number() OVER (PARTITION BY turbine_id ORDER BY ts)
                 - row_number() OVER (PARTITION BY turbine_id, {col} ORDER BY ts) AS grp
          FROM {t} WHERE {col} IS NOT NULL
        ), runs AS (
          SELECT turbine_id, v, grp, count(*) AS n, min(ts) AS start_ts, max(ts) AS end_ts
          FROM s GROUP BY 1, 2, 3
        )"""
        return (f"{base} SELECT coalesce(sum(n), 0) FROM runs WHERE n >= {rule['max_repeat']}",
                f"{base} SELECT turbine_id, v, n, start_ts, end_ts FROM runs "
                f"WHERE n >= {rule['max_repeat']} ORDER BY n DESC LIMIT 20")
    raise ValueError(f"未知规则类型 {rt}")


def get_rules(table: Table) -> list:
    p = table.quality_dir / "rules.json"
    if p.exists():
        return json.loads(p.read_text())
    return list(DEFAULT_RULES)


def save_rules(table: Table, rules: list):
    table._write_json(table.quality_dir / "rules.json", rules)


def run_quality(table: Table, rules: list | None = None) -> dict:
    """对最新快照跑全部规则,报告存到 _quality/report-<时间>.json。"""
    rules = rules if rules is not None else get_rules(table)
    con = table.connect()
    results = []
    try:
        total = con.execute(f'SELECT count(*) FROM "{table.name}"').fetchone()[0]
        for rule in rules:
            count_sql, sample_sql = _rule_sql(rule, table.name)
            n = con.execute(count_sql).fetchone()[0] or 0
            samples = con.execute(sample_sql).df().astype(str).to_dict("records")
            pct = round(100.0 * n / total, 4) if total else 0.0
            ok = True
            if rule["type"] == "null_rate":
                ok = pct <= rule.get("max_pct", 0)
            else:
                ok = n == 0
            results.append({"rule": rule, "violations": int(n), "pct": pct,
                            "ok": ok, "samples": samples})
    finally:
        con.close()
    report = {"table": table.name, "run_at": time.time(),
              "run_at_iso": time.strftime("%Y-%m-%d %H:%M:%S"),
              "total_rows": total, "results": results}
    table._write_json(table.quality_dir / f"report-{int(time.time())}.json", report)
    return report


def list_reports(table: Table) -> list[dict]:
    if not table.quality_dir.exists():
        return []
    out = []
    for p in sorted(table.quality_dir.glob("report-*.json"), reverse=True):
        r = json.loads(p.read_text())
        out.append({"run_at_iso": r["run_at_iso"], "total_rows": r["total_rows"],
                    "failed": sum(1 for x in r["results"] if not x["ok"]),
                    "file": p.name})
    return out
