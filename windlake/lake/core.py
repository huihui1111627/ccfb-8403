"""核心存储引擎:快照式 Parquet 数据湖。

设计要点:
- 每次写入/合并/压缩/演进产生一个不可变快照(_snapshots/NNNNNN.json),
  快照记录当时生效的 schema 版本和文件清单,任何时间点回查都重放该清单。
- 数据文件只增不改:迟到数据合并、小文件压缩都是写新文件 + 提交新快照,
  老文件只有在没有任何保留快照引用时才被物理删除,正在跑的查询不受影响。
- schema 演进(加列/改名/类型放宽)只登记新版本,读取时按版本对齐,
  老数据文件一个字节都不重写。
"""
from __future__ import annotations

import json
import os
import shutil
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------- 类型映射

DTYPES = {
    "BIGINT": pa.int64(),
    "DOUBLE": pa.float64(),
    "VARCHAR": pa.string(),
    "BOOLEAN": pa.bool_(),
    "TIMESTAMP": pa.timestamp("us"),
    "DATE": pa.date32(),
}

# 允许的类型放宽方向(只能往宽里放,老数据读取时无损失上转)
WIDENING = {
    "BIGINT": {"DOUBLE", "VARCHAR"},
    "DOUBLE": {"VARCHAR"},
    "BOOLEAN": {"VARCHAR"},
    "DATE": {"TIMESTAMP", "VARCHAR"},
    "TIMESTAMP": {"VARCHAR"},
    "VARCHAR": set(),
}

KEY_COLS = ["turbine_id", "ts"]  # 去重主键


class LakeError(Exception):
    pass


# ---------------------------------------------------------------- 快照

@dataclass
class Snapshot:
    table: "Table"
    seq: int
    committed_at: float
    operation: str
    schema_version: int
    files: list  # [{path, schema_version, rows, turbine_id, date}]
    detail: dict

    @property
    def committed_at_iso(self) -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.committed_at))

    @property
    def total_rows(self) -> int:
        return sum(f["rows"] for f in self.files)


# ---------------------------------------------------------------- 表

class Table:
    def __init__(self, lake: "Lake", name: str):
        self.lake = lake
        self.name = name
        self.dir = lake.root / name
        self.snap_dir = self.dir / "_snapshots"
        self.schema_dir = self.dir / "_schema"
        self.data_dir = self.dir / "data"
        self.quality_dir = self.dir / "_quality"
        self._lock = threading.Lock()

    # ---------------- 内部:目录/文件 ----------------

    def _write_json(self, path: Path, obj):
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=1))
        os.replace(tmp, path)  # 原子提交

    def _read_json(self, path: Path):
        return json.loads(path.read_text())

    def _snapshot_path(self, seq: int) -> Path:
        return self.snap_dir / f"{seq:06d}.json"

    def _snapshot_seqs(self) -> list[int]:
        if not self.snap_dir.exists():
            return []
        return sorted(int(p.stem) for p in self.snap_dir.glob("*.json"))

    # ---------------- schema ----------------

    def schema(self, version: int | None = None) -> dict:
        """返回 {version, columns: [{name, dtype}]},默认最新版本。"""
        if version is None:
            version = self.current_schema_version()
        return self._read_json(self.schema_dir / f"v{version}.json")

    def current_schema_version(self) -> int:
        versions = sorted(int(p.stem[1:]) for p in self.schema_dir.glob("v*.json"))
        if not versions:
            raise LakeError(f"表 {self.name} 没有 schema")
        return versions[-1]

    def _ops(self) -> list:
        p = self.schema_dir / "ops.json"
        return self._read_json(p) if p.exists() else []

    def _commit_schema(self, columns: list, op: dict) -> int:
        new_version = self.current_schema_version() + 1
        self._write_json(self.schema_dir / f"v{new_version}.json",
                         {"version": new_version, "columns": columns})
        ops = self._ops()
        op = dict(op, version=new_version, at=time.time())
        ops.append(op)
        self._write_json(self.schema_dir / "ops.json", ops)
        return new_version

    def _current_files(self) -> list:
        seqs = self._snapshot_seqs()
        return self.snapshot().files if seqs else []

    def add_column(self, name: str, dtype: str):
        """加列:只登记新 schema,老文件读取时该列补 NULL。"""
        dtype = dtype.upper()
        if dtype not in DTYPES:
            raise LakeError(f"不支持的类型 {dtype}")
        cols = self.schema()["columns"]
        if any(c["name"] == name for c in cols):
            raise LakeError(f"列 {name} 已存在")
        cols = cols + [{"name": name, "dtype": dtype}]
        with self._lock:
            v = self._commit_schema(cols, {"op": "add_column", "column": name, "dtype": dtype})
            self._commit_snapshot("schema_add_column", v, self._current_files(),
                                  {"column": name, "dtype": dtype})

    def rename_column(self, old: str, new: str):
        """改列名:读取时把老文件里的 old 映射成 new,不重写数据。"""
        cols = self.schema()["columns"]
        names = [c["name"] for c in cols]
        if old not in names:
            raise LakeError(f"列 {old} 不存在")
        if new in names:
            raise LakeError(f"列 {new} 已存在")
        for c in cols:
            if c["name"] == old:
                c["name"] = new
        with self._lock:
            v = self._commit_schema(cols, {"op": "rename_column", "from": old, "to": new})
            self._commit_snapshot("schema_rename_column", v, self._current_files(),
                                  {"from": old, "to": new})

    def widen_column(self, name: str, new_dtype: str):
        """类型放宽(如 BIGINT->DOUBLE):读取时 CAST,老文件不重写。"""
        new_dtype = new_dtype.upper()
        cols = self.schema()["columns"]
        target = next((c for c in cols if c["name"] == name), None)
        if target is None:
            raise LakeError(f"列 {name} 不存在")
        old_dtype = target["dtype"]
        if new_dtype not in WIDENING.get(old_dtype, set()):
            raise LakeError(f"不允许 {old_dtype} -> {new_dtype}(只允许放宽: {WIDENING.get(old_dtype)})")
        target["dtype"] = new_dtype
        with self._lock:
            v = self._commit_schema(cols, {"op": "widen_column", "column": name,
                                           "from": old_dtype, "to": new_dtype})
            self._commit_snapshot("schema_widen_column", v, self._current_files(),
                                  {"column": name, "from": old_dtype, "to": new_dtype})

    # ---------------- 快照 ----------------

    def _commit_snapshot(self, operation: str, schema_version: int,
                         files: list, detail: dict | None = None) -> Snapshot:
        seqs = self._snapshot_seqs()
        seq = (seqs[-1] + 1) if seqs else 1
        snap = {"seq": seq, "committed_at": time.time(), "operation": operation,
                "schema_version": schema_version, "files": files, "detail": detail or {}}
        self._write_json(self._snapshot_path(seq), snap)
        return Snapshot(self, seq, snap["committed_at"], operation,
                        schema_version, files, snap["detail"])

    def snapshot(self, seq: int | None = None, as_of: float | None = None) -> Snapshot:
        """取快照:指定 seq,或取 as_of 时间戳之前最近的一个,默认最新。"""
        seqs = self._snapshot_seqs()
        if not seqs:
            raise LakeError(f"表 {self.name} 还没有快照")
        if seq is None:
            if as_of is not None:
                cand = [s for s in seqs
                        if self._read_json(self._snapshot_path(s))["committed_at"] <= as_of]
                if not cand:
                    raise LakeError(f"{time.strftime('%F %T', time.localtime(as_of))} 之前没有快照")
                seq = cand[-1]
            else:
                seq = seqs[-1]
        s = self._read_json(self._snapshot_path(seq))
        return Snapshot(self, s["seq"], s["committed_at"], s["operation"],
                        s["schema_version"], s["files"], s.get("detail", {}))

    def snapshots(self) -> list[Snapshot]:
        return [self.snapshot(seq=s) for s in self._snapshot_seqs()]

    # ---------------- 读取:schema 对齐 ----------------

    def _column_name_at(self, column: str, from_version: int, to_version: int) -> str | None:
        """把当前列名回溯到老 schema 版本下的物理列名;当时不存在则返回 None。"""
        name = column
        for op in reversed([o for o in self._ops() if from_version < o["version"] <= to_version]):
            if op["op"] == "rename_column" and op["to"] == name:
                name = op["from"]
            elif op["op"] == "add_column" and op["column"] == name:
                return None
        return name

    def _select_sql(self, files: list[str], file_schema_version: int,
                    current: dict) -> str:
        cols = current["columns"]
        exprs = []
        for c in cols:
            src = self._column_name_at(c["name"], file_schema_version, current["version"])
            if src is None:
                exprs.append(f"NULL::{c['dtype']} AS \"{c['name']}\"")
            else:
                exprs.append(f"CAST(\"{src}\" AS {c['dtype']}) AS \"{c['name']}\"")
        paths = ", ".join(f"'{p}'" for p in files)
        return (f"SELECT {', '.join(exprs)} "
                f"FROM read_parquet([{paths}], union_by_name=true)")

    def view_sql(self, snapshot: Snapshot | None = None) -> str:
        """生成对齐到当前 schema 的读取 SQL(按文件 schema 版本分组再 UNION)。"""
        snap = snapshot or self.snapshot()
        # 回查历史快照时,使用该快照生效的 schema,而不是最新 schema
        current = self.schema(snap.schema_version)
        groups: dict[int, list[str]] = {}
        for f in snap.files:
            groups.setdefault(f["schema_version"], []).append(str(self.dir / f["path"]))
        if not groups:
            cols = ", ".join(f"NULL::{c['dtype']} AS \"{c['name']}\"" for c in current["columns"])
            return f"SELECT {cols} WHERE false"
        parts = [self._select_sql(files, v, current) for v, files in sorted(groups.items())]
        return parts[0] if len(parts) == 1 else " UNION ALL ".join(f"({p})" for p in parts)

    def connect(self, snapshot: Snapshot | None = None,
                as_of: float | None = None) -> duckdb.DuckDBPyConnection:
        """返回一个注册好本表视图(视图名=表名)的 DuckDB 连接。"""
        snap = snapshot or self.snapshot(as_of=as_of)
        con = duckdb.connect()
        con.execute(f'CREATE VIEW "{self.name}" AS {self.view_sql(snap)}')
        return con

    def df(self, snapshot: Snapshot | None = None, as_of: float | None = None) -> pd.DataFrame:
        con = self.connect(snapshot, as_of)
        try:
            return con.execute(f'SELECT * FROM "{self.name}"').df()
        finally:
            con.close()

    # ---------------- 写入 ----------------

    def _validate_and_cast(self, df: pd.DataFrame) -> pd.DataFrame:
        schema = self.schema()
        cols = {c["name"]: c["dtype"] for c in schema["columns"]}
        missing_keys = [k for k in KEY_COLS if k not in df.columns]
        if missing_keys:
            raise LakeError(f"缺少主键列 {missing_keys}")
        extra = [c for c in df.columns if c not in cols]
        if extra:
            raise LakeError(f"数据包含 schema 外的列 {extra},请先在表管理里加列")
        df = df.copy()
        df["ts"] = pd.to_datetime(df["ts"])
        df["turbine_id"] = df["turbine_id"].astype(str)
        for name, dtype in cols.items():
            if name not in df.columns:
                df[name] = None
        return df[[c["name"] for c in schema["columns"]]]

    def _write_partition(self, pdf: pd.DataFrame, turbine_id: str, date_str: str) -> dict:
        schema = self.schema()
        arrow_schema = pa.schema([(c["name"], DTYPES[c["dtype"]]) for c in schema["columns"]])
        tbl = pa.Table.from_pandas(pdf, schema=arrow_schema, preserve_index=False)
        rel = f"data/turbine_id={turbine_id}/date={date_str}/part-{uuid.uuid4().hex[:12]}.parquet"
        out = self.dir / rel
        out.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(tbl, out, compression="zstd")
        return {"path": rel, "schema_version": schema["version"], "rows": len(pdf),
                "turbine_id": turbine_id, "date": date_str}

    def ingest_df(self, df: pd.DataFrame, merge: bool = True) -> Snapshot:
        """写入一批数据。merge=True 时按 (turbine_id, ts) 与对应分区现有数据去重合并
        (迟到数据覆盖旧值),只重写受影响的分区。"""
        df = self._validate_and_cast(df)
        df["date"] = df["ts"].dt.strftime("%Y-%m-%d")
        with self._lock:
            old_files = self.snapshot().files if self._snapshot_seqs() else []
            affected = set(zip(df["turbine_id"], df["date"]))
            kept, merged_new = [], []
            if merge:
                # 受影响分区:读出现有数据,与新数据合并去重后整体重写
                for tid, d in sorted(affected):
                    part_files = [f for f in old_files
                                  if f["turbine_id"] == tid and f["date"] == d]
                    new_part = df[(df["turbine_id"] == tid) & (df["date"] == d)]
                    if part_files:
                        old_df = self.connect().execute(
                            f'SELECT * FROM "{self.name}" WHERE turbine_id = ? '
                            f"AND CAST(ts AS DATE) = CAST(? AS DATE)",
                            [tid, d]).df()
                        combined = pd.concat([old_df, new_part[old_df.columns]],
                                             ignore_index=True)
                        combined = combined.drop_duplicates(KEY_COLS, keep="last")
                    else:
                        combined = new_part
                    combined = combined.sort_values("ts")
                    merged_new.append(self._write_partition(
                        combined.drop(columns=["date"], errors="ignore"), tid, d))
                kept = [f for f in old_files
                        if (f["turbine_id"], f["date"]) not in affected]
            else:
                for (tid, d), part in df.groupby(["turbine_id", "date"]):
                    merged_new.append(self._write_partition(
                        part.drop(columns=["date"], errors="ignore"), tid, d))
                kept = old_files
            return self._commit_snapshot(
                "ingest_merge" if merge else "ingest_append",
                self.current_schema_version(), kept + merged_new,
                {"rows_in": len(df), "partitions": len(affected)})

    def ingest_csv(self, path: str | Path, merge: bool = True) -> Snapshot:
        return self.ingest_df(pd.read_csv(path), merge=merge)

    # ---------------- 压缩 / 清理 ----------------

    def compact(self, min_files: int = 3) -> Snapshot | None:
        """把文件数 >= min_files 的分区各合并成一个文件。写新文件+新快照,
        老文件等 expire 时回收,正在跑的查询不受影响。"""
        with self._lock:
            snap = self.snapshot()
            by_part: dict[tuple, list] = {}
            for f in snap.files:
                by_part.setdefault((f["turbine_id"], f["date"]), []).append(f)
            targets = {k: v for k, v in by_part.items() if len(v) >= min_files}
            if not targets:
                return None
            con = self.connect(snap)
            new_files = []
            try:
                for (tid, d) in sorted(targets):
                    pdf = con.execute(
                        f'SELECT * FROM "{self.name}" WHERE turbine_id = ? '
                        f"AND CAST(ts AS DATE) = CAST(? AS DATE) ORDER BY ts",
                        [tid, d]).df()
                    new_files.append(self._write_partition(pdf, tid, d))
            finally:
                con.close()
            kept = [f for f in snap.files
                    if (f["turbine_id"], f["date"]) not in targets]
            return self._commit_snapshot("compact", self.current_schema_version(),
                                         kept + new_files,
                                         {"partitions_compacted": len(targets)})

    def expire_snapshots(self, retain_hours: float = 24 * 7, keep_last: int = 5,
                         grace_seconds: float = 0, now: float | None = None) -> dict:
        """清理过期快照并回收不再被任何保留快照引用的数据文件。

        retain_hours: 快照保留时长;keep_last: 无论多老都保留最近 N 个;
        grace_seconds: 文件解除引用后再宽限这么久才删(保护刚好打开旧快照的查询)。
        """
        now = now or time.time()
        seqs = self._snapshot_seqs()
        if not seqs:
            return {"expired": 0, "files_deleted": 0}
        cutoff = now - retain_hours * 3600
        keep_seqs = set(seqs[-keep_last:])
        kept, expired = [], []
        for s in seqs:
            snap = self._read_json(self._snapshot_path(s))
            if s in keep_seqs or snap["committed_at"] >= cutoff:
                kept.append(s)
            else:
                expired.append(s)
        referenced = set()
        for s in kept:
            for f in self._read_json(self._snapshot_path(s))["files"]:
                referenced.add(f["path"])
        deleted = 0
        for s in expired:
            for f in self._read_json(self._snapshot_path(s))["files"]:
                p = self.dir / f["path"]
                if f["path"] not in referenced and p.exists() \
                        and now - p.stat().st_mtime >= grace_seconds:
                    p.unlink()
                    deleted += 1
            self._snapshot_path(s).unlink()
        # 清掉空分区目录
        for d in sorted(self.data_dir.rglob("*"), reverse=True):
            if d.is_dir() and not any(d.iterdir()):
                d.rmdir()
        return {"expired": len(expired), "files_deleted": deleted}

    # ---------------- 统计 ----------------

    def partition_stats(self) -> pd.DataFrame:
        snap = self.snapshot()
        rows = {}
        for f in snap.files:
            k = (f["turbine_id"], f["date"])
            r = rows.setdefault(k, {"turbine_id": k[0], "date": k[1], "files": 0, "rows": 0})
            r["files"] += 1
            r["rows"] += f["rows"]
        return pd.DataFrame(rows.values())


# ---------------------------------------------------------------- 湖

class Lake:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def create_table(self, name: str, columns: dict[str, str]) -> Table:
        """columns: {列名: 类型},必须包含 turbine_id 和 ts。"""
        cols = {k: v.upper() for k, v in columns.items()}
        for k in KEY_COLS:
            if k not in cols:
                raise LakeError(f"schema 必须包含主键列 {k}")
        for v in cols.values():
            if v not in DTYPES:
                raise LakeError(f"不支持的类型 {v}")
        t = Table(self, name)
        if t.dir.exists():
            raise LakeError(f"表 {name} 已存在")
        t._write_json(t.schema_dir / "v1.json",
                      {"version": 1, "columns": [{"name": k, "dtype": v}
                                                 for k, v in cols.items()]})
        t._write_json(t.schema_dir / "ops.json", [])
        return t

    def table(self, name: str) -> Table:
        t = Table(self, name)
        if not t.dir.exists():
            raise LakeError(f"表 {name} 不存在")
        return t

    def tables(self) -> list[str]:
        return sorted(p.name for p in self.root.iterdir()
                      if p.is_dir() and (p / "_schema").exists())

    def drop_table(self, name: str):
        shutil.rmtree(self.root / name)
