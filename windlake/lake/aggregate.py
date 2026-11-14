"""秒级数据聚合成十分钟,并与 SCADA 自带的十分钟数据对比偏差。"""
from __future__ import annotations

import pandas as pd

from .core import Lake, Table

AGG_COLUMNS = {
    "power": "DOUBLE",
    "wind_speed": "DOUBLE",
    "rotor_speed": "DOUBLE",
    "temperature": "DOUBLE",
}


def ensure_calc_table(lake: Lake, name: str = "scada_10min_calc") -> Table:
    if name in lake.tables():
        return lake.table(name)
    cols = {"turbine_id": "VARCHAR", "ts": "TIMESTAMP"}
    cols.update({f"{c}_avg": t for c, t in AGG_COLUMNS.items()})
    cols["sample_count"] = "BIGINT"
    return lake.create_table(name, cols)


def aggregate_10min(seconds_table: Table, calc_table: Table,
                    date_from: str | None = None, date_to: str | None = None) -> int:
    """把秒级表按 10 分钟桶聚合写入计算表(按分区合并去重,可重复跑)。
    只聚合秒级表里实际存在的列。"""
    have = {c["name"] for c in seconds_table.schema()["columns"]}
    cols = [c for c in AGG_COLUMNS if c in have]
    where = ""
    if date_from and date_to:
        where = (f"WHERE CAST(ts AS DATE) BETWEEN CAST('{date_from}' AS DATE) "
                 f"AND CAST('{date_to}' AS DATE)")
    aggs = ", ".join(f"avg({c}) AS {c}_avg" for c in cols)
    con = seconds_table.connect()
    try:
        df = con.execute(f"""
            SELECT turbine_id,
                   time_bucket(INTERVAL '10 minutes', ts) AS ts,
                   {aggs}, count(*) AS sample_count
            FROM "{seconds_table.name}" {where}
            GROUP BY 1, 2
        """).df()
    finally:
        con.close()
    if len(df):
        calc_table.ingest_df(df, merge=True)
    return len(df)


def compare_with_scada(calc_table: Table, scada_table: Table,
                       power_col: str = "power") -> pd.DataFrame:
    """计算聚合值与 SCADA 十分钟值的偏差。返回逐点对比明细。"""
    con = calc_table.connect()
    con.execute(f'CREATE VIEW scada AS {scada_table.view_sql()}')
    try:
        return con.execute(f"""
            SELECT c.turbine_id, c.ts,
                   c.power_avg AS power_calc,
                   s."{power_col}" AS power_scada,
                   c.power_avg - s."{power_col}" AS deviation,
                   CASE WHEN abs(s."{power_col}") > 1e-6
                        THEN round(100.0 * (c.power_avg - s."{power_col}") / s."{power_col}", 3)
                        ELSE NULL END AS deviation_pct,
                   c.sample_count
            FROM "{calc_table.name}" c
            JOIN scada s ON s.turbine_id = c.turbine_id AND s.ts = c.ts
            ORDER BY c.turbine_id, c.ts
        """).df()
    finally:
        con.close()


def deviation_summary(cmp_df: pd.DataFrame, threshold_pct: float = 5.0) -> dict:
    if not len(cmp_df):
        return {"points": 0}
    over = cmp_df[cmp_df["deviation_pct"].abs() > threshold_pct]
    return {
        "points": int(len(cmp_df)),
        "mean_abs_deviation": round(float(cmp_df["deviation"].abs().mean()), 3),
        "max_abs_deviation_pct": round(float(cmp_df["deviation_pct"].abs().max()), 3),
        "over_threshold": int(len(over)),
        "threshold_pct": threshold_pct,
        "worst": over.reindex(over["deviation_pct"].abs().sort_values(ascending=False).index)
        .head(20).astype(str).to_dict("records"),
    }
