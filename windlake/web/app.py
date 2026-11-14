"""风电数据湖 Web 控制台:数据接入 / 表管理 / 快照历史 / 数据质量 / 查询。

启动: uvicorn web.app:app --app-dir windlake --port 8000
数据目录用环境变量 WINDLAKE_HOME 指定,默认 ./data
"""
from __future__ import annotations

import io
import json
import os
import time
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from lake import Lake, LakeError
from lake import aggregate as agg
from lake import quality as dq

HOME = Path(os.environ.get("WINDLAKE_HOME", "data"))
LAKE = Lake(HOME / "lake")
CSV_DIR = HOME / "csv"
CSV_DIR.mkdir(parents=True, exist_ok=True)

SECONDS_SCHEMA = {"turbine_id": "VARCHAR", "ts": "TIMESTAMP", "wind_speed": "DOUBLE",
                  "power": "DOUBLE", "rotor_speed": "DOUBLE", "temperature": "DOUBLE",
                  "status": "VARCHAR"}
TENMIN_SCHEMA = {"turbine_id": "VARCHAR", "ts": "TIMESTAMP",
                 "power": "DOUBLE", "wind_speed": "DOUBLE"}

app = FastAPI(title="风电数据湖控制台")
templates = Jinja2Templates(directory=Path(__file__).parent / "templates")


def render(request: Request, page: str, **ctx):
    return templates.TemplateResponse(
        f"{page}.html", {"request": request, "page": page,
                         "tables": LAKE.tables(), **ctx})


def msg(url: str, text: str) -> RedirectResponse:
    sep = "&" if "?" in url else "?"
    return RedirectResponse(f"{url}{sep}msg={text}", status_code=303)


# ---------------------------------------------------------------- 数据接入

@app.get("/", response_class=HTMLResponse)
@app.get("/ingest", response_class=HTMLResponse)
def ingest_page(request: Request, msg: str = ""):
    overview = []
    for t in LAKE.tables():
        tb = LAKE.table(t)
        try:
            snap = tb.snapshot()
            overview.append({"name": t, "snapshots": len(tb.snapshots()),
                             "rows": snap.total_rows, "files": len(snap.files),
                             "schema_version": snap.schema_version})
        except LakeError:
            overview.append({"name": t, "snapshots": 0, "rows": 0, "files": 0,
                             "schema_version": "-"})
    csvs = sorted(p.name for p in CSV_DIR.glob("*.csv"))
    return render(request, "ingest", overview=overview, csvs=csvs, msg=msg)


@app.post("/ingest/upload")
async def ingest_upload(table: str = Form(...), file: UploadFile = None):
    df = pd.read_csv(io.BytesIO(await file.read()))
    snap = LAKE.table(table).ingest_df(df, merge=True)
    return msg("/ingest", f"已写入 {table},快照 #{snap.seq},{snap.detail}")


@app.post("/ingest/file")
def ingest_file(table: str = Form(...), filename: str = Form(...)):
    snap = LAKE.table(table).ingest_csv(CSV_DIR / filename, merge=True)
    return msg("/ingest", f"{filename} -> {table},快照 #{snap.seq},{snap.detail}")


@app.post("/ingest/generate")
def ingest_generate(turbines: int = Form(10), days: int = Form(3)):
    import subprocess, sys
    script = Path(__file__).parent.parent / "scripts" / "generate_data.py"
    subprocess.run([sys.executable, "-I", str(script), "--turbines", str(turbines),
                    "--days", str(days), "--out", str(CSV_DIR)], check=True,
                   capture_output=True)
    return msg("/ingest", f"已生成 {turbines} 台 × {days} 天的 CSV 到 {CSV_DIR}")


# ---------------------------------------------------------------- 表管理

@app.get("/tables", response_class=HTMLResponse)
def tables_page(request: Request, table: str = "", msg: str = ""):
    detail = None
    if table:
        tb = LAKE.table(table)
        detail = {"name": table, "schema": tb.schema(),
                  "ops": tb._ops(),
                  "partitions": tb.partition_stats().to_dict("records")
                  if tb._snapshot_seqs() else []}
    return render(request, "tables", detail=detail, selected=table, msg=msg)


@app.post("/tables/create")
def table_create(name: str = Form(...), preset: str = Form("seconds")):
    schema = SECONDS_SCHEMA if preset == "seconds" else TENMIN_SCHEMA
    try:
        LAKE.create_table(name, schema)
    except LakeError as e:
        return msg("/tables", f"建表失败:{e}")
    return msg("/tables", f"表 {name} 已创建({preset} 模板)")


@app.post("/tables/{table}/evolve")
def table_evolve(table: str, op: str = Form(...), column: str = Form(""),
                 new_name: str = Form(""), dtype: str = Form("DOUBLE")):
    tb = LAKE.table(table)
    try:
        if op == "add":
            tb.add_column(column, dtype)
        elif op == "rename":
            tb.rename_column(column, new_name)
        elif op == "widen":
            tb.widen_column(column, dtype)
    except LakeError as e:
        return msg(f"/tables?table={table}", f"演进失败:{e}")
    return msg(f"/tables?table={table}", f"schema 已演进:{op} {column}")


@app.post("/tables/{table}/compact")
def table_compact(table: str, min_files: int = Form(3)):
    snap = LAKE.table(table).compact(min_files=min_files)
    if snap is None:
        return msg(f"/tables?table={table}", "没有需要压缩的分区")
    return msg(f"/tables?table={table}",
               f"压缩完成,快照 #{snap.seq},{snap.detail}")


# ---------------------------------------------------------------- 快照历史

@app.get("/snapshots", response_class=HTMLResponse)
def snapshots_page(request: Request, table: str = "", preview: int = 0,
                   msg: str = ""):
    snaps, preview_df, preview_sql = [], None, None
    if table:
        tb = LAKE.table(table)
        snaps = [{"seq": s.seq, "committed_at": s.committed_at_iso,
                  "operation": s.operation, "schema_version": s.schema_version,
                  "files": len(s.files), "rows": s.total_rows, "detail": s.detail}
                 for s in reversed(tb.snapshots())]
        if preview:
            snap = tb.snapshot(seq=preview)
            con = tb.connect(snap)
            preview_sql = f'-- 快照 #{preview} 时刻的数据\nSELECT * FROM "{table}" LIMIT 50'
            preview_df = con.execute(f'SELECT * FROM "{table}" LIMIT 50').df()
            con.close()
    return render(request, "snapshots", selected=table, snaps=snaps,
                  preview=preview, preview_df=preview_df, preview_sql=preview_sql,
                  msg=msg)


@app.post("/snapshots/expire")
def snapshots_expire(table: str = Form(...), retain_hours: float = Form(168),
                     keep_last: int = Form(5)):
    r = LAKE.table(table).expire_snapshots(retain_hours=retain_hours,
                                           keep_last=keep_last)
    return msg(f"/snapshots?table={table}",
               f"清理完成:过期快照 {r['expired']} 个,回收文件 {r['files_deleted']} 个")


# ---------------------------------------------------------------- 数据质量

@app.get("/quality", response_class=HTMLResponse)
def quality_page(request: Request, table: str = "", msg: str = ""):
    rules_json, reports, last_report = "", [], None
    if table:
        tb = LAKE.table(table)
        rules_json = json.dumps(dq.get_rules(tb), ensure_ascii=False, indent=2)
        reports = dq.list_reports(tb)
        if reports:
            p = tb.quality_dir / reports[0]["file"]
            last_report = json.loads(p.read_text())
    return render(request, "quality", selected=table, rules_json=rules_json,
                  reports=reports, last_report=last_report, msg=msg)


@app.post("/quality/rules")
def quality_rules(table: str = Form(...), rules: str = Form(...)):
    try:
        dq.save_rules(LAKE.table(table), json.loads(rules))
    except (json.JSONDecodeError, LakeError) as e:
        return msg(f"/quality?table={table}", f"规则保存失败:{e}")
    return msg(f"/quality?table={table}", "规则已保存")


@app.post("/quality/run")
def quality_run(table: str = Form(...)):
    r = dq.run_quality(LAKE.table(table))
    bad = sum(1 for x in r["results"] if not x["ok"])
    return msg(f"/quality?table={table}",
               f"质检完成:{len(r['results'])} 条规则,{bad} 条未通过")


@app.post("/quality/aggregate")
def quality_aggregate(seconds_table: str = Form("scada_seconds"),
                      scada_table: str = Form("scada_10min")):
    calc = agg.ensure_calc_table(LAKE)
    n = agg.aggregate_10min(LAKE.table(seconds_table), calc)
    cmp_df = agg.compare_with_scada(calc, LAKE.table(scada_table))
    s = agg.deviation_summary(cmp_df)
    (HOME / "last_compare.json").write_text(json.dumps(s, ensure_ascii=False, indent=1))
    return msg(f"/quality?table={seconds_table}",
               f"聚合 {n} 个十分钟点;对比 {s.get('points', 0)} 点,"
               f"超阈值 {s.get('over_threshold', 0)} 点,结果见查询页帮助")


@app.get("/quality/compare", response_class=HTMLResponse)
def quality_compare(request: Request):
    p = HOME / "last_compare.json"
    summary = json.loads(p.read_text()) if p.exists() else None
    return render(request, "compare", summary=summary)


# ---------------------------------------------------------------- 查询

@app.get("/query", response_class=HTMLResponse)
def query_page(request: Request, msg: str = ""):
    snap_options = {}
    for t in LAKE.tables():
        snap_options[t] = [{"seq": s.seq, "label": f"#{s.seq} {s.committed_at_iso} "
                           f"({s.operation}, {s.total_rows} 行)"}
                           for s in reversed(LAKE.table(t).snapshots())]
    return render(request, "query", snap_options=snap_options, result=None,
                  sql="", msg=msg)


@app.post("/query", response_class=HTMLResponse)
async def query_run(request: Request):
    form = await request.form()
    sql = form.get("sql", "").strip()
    import duckdb
    con = duckdb.connect()
    error, result, elapsed = None, None, 0.0
    try:
        for t in LAKE.tables():
            seq = form.get(f"snap_{t}")
            snap = LAKE.table(t).snapshot(seq=int(seq)) if seq else None
            con.execute(f'CREATE VIEW "{t}" AS {LAKE.table(t).view_sql(snap)}')
        t0 = time.time()
        result = con.execute(sql).df()
        elapsed = time.time() - t0
        if len(result) > 500:
            result = result.head(500)
    except Exception as e:  # 查询错误直接展示给使用者
        error = str(e)
    finally:
        con.close()
    snap_options = {}
    for t in LAKE.tables():
        snap_options[t] = [{"seq": s.seq, "label": f"#{s.seq} {s.committed_at_iso} "
                           f"({s.operation}, {s.total_rows} 行)"}
                           for s in reversed(LAKE.table(t).snapshots())]
    return render(request, "query", snap_options=snap_options, result=result,
                  sql=sql, error=error, elapsed=round(elapsed, 3), msg="")
