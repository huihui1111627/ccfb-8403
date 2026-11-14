"""生成风电场 SCADA 测试数据(秒级 CSV + SCADA 十分钟 CSV)。

默认规模可以快速跑通;要压到几百台机一个月:
    python scripts/generate_data.py --turbines 300 --days 30 --out data/csv

会按天输出 CSV,模拟 SCADA 每天导出一批;同时注入空值/跳变/卡死等异常,
十分钟文件在秒级真值聚合的基础上上加可控偏差,用于对比功能。
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

SECONDS_PER_DAY = 86400


def gen_turbine_day(rng: np.random.Generator, tid: str, day: str,
                    rated: float = 2000.0) -> pd.DataFrame:
    n = SECONDS_PER_DAY
    ts = pd.date_range(day, periods=n, freq="s")
    # 风速:日变化 + 慢扰动 + 湍流噪声
    t = np.arange(n)
    wind = (8.0 + 3.0 * np.sin(2 * np.pi * t / n + rng.uniform(0, 6))
            + np.convolve(rng.normal(0, 0.6, n), np.ones(60) / 60, mode="same")
            + rng.normal(0, 0.3, n)).clip(0, 30)
    # 功率:简化功率曲线(切入 3,额定 12,切出 25)
    power = np.where(wind < 3, 0.0,
            np.where(wind < 12, rated * ((wind - 3) / 9) ** 3,
            np.where(wind < 25, rated, 0.0)))
    power = (power + rng.normal(0, 8, n)).clip(-50, rated * 1.05)
    df = pd.DataFrame({
        "turbine_id": tid,
        "ts": ts,
        "wind_speed": wind.round(2),
        "power": power.round(1),
        "rotor_speed": (power / rated * 15 + rng.normal(0, 0.1, n)).clip(0, 20).round(2),
        "temperature": (15 + 8 * np.sin(2 * np.pi * t / n) + rng.normal(0, 0.5, n)).round(1),
        "status": np.where(power > 0, "RUN", "IDLE"),
    })
    # ---- 注入异常(用于数据质量页面演示)----
    if rng.random() < 0.3:  # 空值
        idx = rng.choice(n, size=rng.integers(5, 30), replace=False)
        df.loc[idx, "power"] = np.nan
    if rng.random() < 0.2:  # 跳变
        idx = rng.choice(n, size=rng.integers(1, 5), replace=False)
        df.loc[idx, "power"] = df.loc[idx, "power"] + rng.choice([-1, 1]) * 1500
    if rng.random() < 0.15:  # 卡死值(连续 10 分钟同一个风速)
        start = rng.integers(0, n - 700)
        df.loc[start:start + 600, "wind_speed"] = df.loc[start, "wind_speed"]
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--turbines", type=int, default=10)
    ap.add_argument("--days", type=int, default=3)
    ap.add_argument("--start", default="2026-10-01")
    ap.add_argument("--out", default="data/csv")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--scada-bias-pct", type=float, default=0.5,
                    help="SCADA 十分钟值相对真值聚合的偏差百分比")
    args = ap.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(args.seed)
    days = pd.date_range(args.start, periods=args.days, freq="D")
    tids = [f"WT{i:03d}" for i in range(1, args.turbines + 1)]

    for day in days:
        day_str = day.strftime("%Y-%m-%d")
        frames = [gen_turbine_day(rng, tid, day_str) for tid in tids]
        sec = pd.concat(frames, ignore_index=True)
        sec["ts"] = sec["ts"].dt.strftime("%Y-%m-%d %H:%M:%S")
        sec.to_csv(out / f"scada_seconds_{day_str}.csv", index=False)

        # SCADA 自带十分钟:真值聚合 + 偏差
        sec["ts"] = pd.to_datetime(sec["ts"])
        ten = (sec.set_index("ts").groupby("turbine_id")
               .resample("10min")[["power", "wind_speed"]].mean().reset_index())
        bias = 1 + rng.normal(0, args.scada_bias_pct / 100, len(ten))
        ten["power"] = (ten["power"] * bias).round(1)
        ten["wind_speed"] = ten["wind_speed"].round(2)
        ten["ts"] = ten["ts"].dt.strftime("%Y-%m-%d %H:%M:%S")
        ten.to_csv(out / f"scada_10min_{day_str}.csv", index=False)
        print(f"{day_str}: {len(sec):,} 秒级行 / {len(ten):,} 十分钟行")

    print(f"完成 -> {out.resolve()}")


if __name__ == "__main__":
    main()
