
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# use this to get rank data

import os
import time
import math
import random
import argparse  # Ensure this is imported
from typing import List, Tuple, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import pandas as pd
from datetime import datetime

# ================== Tunables ==================
N_DAYS = 90

# Phase 1 (parallel) – keep conservative to reduce 风控
MAX_WORKERS = 8
SUBMIT_GAP = 0.02   # seconds between submitting tasks

# Phase 2 (sequential slow) – for missing boards
SLOW_RETRY_ROUNDS = 2            # how many slow rounds
SLOW_SLEEP_BETWEEN_CALLS = 0.9   # seconds base, will add jitter

# request-level retry/backoff
RETRY_TIMES = 4
BACKOFF_BASE = 0.6    # seconds

# headers / endpoints
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://quote.eastmoney.com/",
    "Accept": "application/json, text/plain, */*",
    "Connection": "keep-alive",
}

LIST_URL = "https://push2.eastmoney.com/api/qt/clist/get"
KLINE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"

BOARD_FS = "m:90+t:2"    # industry; concept: "m:90+t:3"; region: "m:90+t:1"

OUTPUT_CSV = "board_rank_data.csv"


# ================== HTTP utils ==================
def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    # Retry on common transient errors and 429
    retry = Retry(
        total=RETRY_TIMES,
        read=RETRY_TIMES,
        connect=RETRY_TIMES,
        backoff_factor=0.25,     # urllib3自动退避（和我们自定义的退避叠加也没问题）
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


# ================== Fetchers ==================
def fetch_board_list(session: requests.Session, fs: str) -> List[Tuple[str, str]]:
    params = {
        "pn": 1, "pz": 500, "po": 1, "np": 1,
        "fltt": 2, "invt": 2, "fid": "f3",
        "fs": fs,
        "fields": "f12,f14",  # code, name
    }
    r = session.get(LIST_URL, params=params, timeout=10)
    r.raise_for_status()
    diff = r.json().get("data", {}).get("diff", []) or []
    boards = [(it.get("f12"), it.get("f14")) for it in diff if it.get("f12") and it.get("f14")]
    return boards


def _parse_klines(kl) -> List[Tuple[str, float]]:
    recs = []
    for row in kl or []:
        parts = row.split(",")
        if len(parts) >= 9:
            date = parts[0]
            try:
                pct = float(parts[8]) if parts[8] != "" else math.nan
            except ValueError:
                pct = math.nan
            recs.append((date, pct))
    return recs


def _fetch_lmt(session: requests.Session, bk_code: str, n: int) -> List[Tuple[str, float]]:
    params = {
        "fields1": "f1,f2,f3,f4,f5",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": 101, "fqt": 0,
        "secid": f"90.{bk_code}",
        "end": "20500101",
        "lmt": n,
    }
    r = session.get(KLINE_URL, params=params, timeout=10)
    r.raise_for_status()
    kl = (r.json().get("data") or {}).get("klines")
    return _parse_klines(kl)


def _fetch_beg_end(session: requests.Session, bk_code: str, n: int) -> List[Tuple[str, float]]:
    params = {
        "fields1": "f1,f2,f3,f4,f5",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": 101, "fqt": 0,
        "secid": f"90.{bk_code}",
        "beg": "19900101",
        "end": "20500101",
    }
    r = session.get(KLINE_URL, params=params, timeout=10)
    r.raise_for_status()
    kl = (r.json().get("data") or {}).get("klines")
    recs = _parse_klines(kl)
    if recs:
        recs = recs[-n:]
    return recs


def fetch_one_board(session: requests.Session, bk_code: str, bk_name: str, n_days: int,
                    slow_mode: bool = False) -> pd.DataFrame:
    """
    Robust fetch for a single board.
    Returns DataFrame with columns: [trade_date, bk_code, bk_name, pct_chg]
    """
    last_err = None
    tries = RETRY_TIMES if not slow_mode else (RETRY_TIMES + 2)

    for attempt in range(tries):
        try:
            recs = _fetch_lmt(session, bk_code, n_days)
            if not recs or all(math.isnan(p[1]) for p in recs):
                # fallback
                recs = _fetch_beg_end(session, bk_code, n_days)

            if recs:
                df = pd.DataFrame(recs, columns=["trade_date", "pct_chg"])
                # 安全：立即 .copy() 并加上 name，避免 SettingWithCopyWarning
                df = df.copy()
                df["bk_code"] = bk_code
                df["bk_name"] = bk_name
                return df[["trade_date", "bk_code", "bk_name", "pct_chg"]]

            raise RuntimeError("empty klines")
        except Exception as e:
            last_err = e
            # backoff + jitter；慢速模式下再加基础睡眠
            base = BACKOFF_BASE * (2 ** attempt)
            if slow_mode:
                base += SLOW_SLEEP_BETWEEN_CALLS
            time.sleep(base + random.uniform(0, 0.25))

    # final fail -> empty DF (上层统计并做二次慢速抓取)
    return pd.DataFrame(columns=["trade_date", "bk_code", "bk_name", "pct_chg"])


# ================== Pipeline ==================
def build_csv(fs: str, n_days: int, out_csv: str, max_workers: int):
    session = build_session()
    boards = fetch_board_list(session, fs)
    total = len(boards)
    print(f"[info] total boards (list): {total}")

    # Check if the CSV already contains the latest date, skip fetching past dates
    if os.path.exists(out_csv):
        existing_df = pd.read_csv(out_csv)
        existing_dates = pd.to_datetime(existing_df.columns[1:], errors="coerce")
        latest_existing_date = existing_dates.max().date()  # Convert to date

        if latest_existing_date >= datetime.now().date():
            print("[info] Latest data is already up-to-date. Skipping past data fetch.")
        else:
            print("[info] Updating with today's data.")

    # ===== Phase 1: mild parallel =====
    try:
        from tqdm import tqdm
        pbar = tqdm(total=total, desc="Phase1 parallel")
    except Exception:
        pbar = None

    results: Dict[str, pd.DataFrame] = {}
    fails: List[Tuple[str, str]] = []

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        fut_map = {}
        for bk_code, bk_name in boards:
            fut = ex.submit(fetch_one_board, session, bk_code, bk_name, n_days, False)
            fut_map[fut] = (bk_code, bk_name)
            time.sleep(SUBMIT_GAP)  # 轻微节流

        for fut in as_completed(fut_map):
            bk_code, bk_name = fut_map[fut]
            df = fut.result()
            if not df.empty:
                results[bk_code] = df
            else:
                fails.append((bk_code, bk_name))
            if pbar:
                pbar.update(1)

    if pbar:
        pbar.close()

    print(f"[info] phase1 ok={len(results)}, fail={len(fails)}")

    # ===== Phase 2: sequential slow retries for fails =====
    if fails:
        try:
            from tqdm import tqdm
            pbar2 = tqdm(total=len(fails)*SLOW_RETRY_ROUNDS, desc="Phase2 slow")
        except Exception:
            pbar2 = None

        for _round in range(SLOW_RETRY_ROUNDS):
            next_fails = []
            for bk_code, bk_name in fails:
                df = fetch_one_board(session, bk_code, bk_name, n_days, slow_mode=True)
                # 每次调用之间拉长时间，加入随机抖动
                time.sleep(SLOW_SLEEP_BETWEEN_CALLS + random.uniform(0, 0.4))
                if not df.empty:
                    results[bk_code] = df
                else:
                    next_fails.append((bk_code, bk_name))
                if pbar2:
                    pbar2.update(1)
            fails = next_fails
            print(f"[info] slow round done, remaining fails: {len(fails)}")
            if not fails:
                break
        if pbar2:
            pbar2.close()

    if fails:
        # 仍有缺失，打印样本
        print(f"[warn] still missing {len(fails)} boards after slow retries.")
        print("       sample:", ", ".join([f"{c}|{n}" for c, n in fails[:12]]))

    # ===== Assemble =====
    if not results:
        raise RuntimeError("No data fetched for any board.")

    all_df = pd.concat(results.values(), ignore_index=True)
    all_df["trade_date"] = pd.to_datetime(all_df["trade_date"])
    all_df = all_df.sort_values(["trade_date", "bk_code"]).reset_index(drop=True)

    # rank per date (descending)
    all_df["rank"] = all_df.groupby("trade_date")["pct_chg"].rank(ascending=False, method="first").astype("Int64")

    # format "rank|pct_chg"
    def fmt_cell(row):
        r = row["rank"]
        p = row["pct_chg"]
        rs = "" if pd.isna(r) else str(int(r))
        ps = "" if pd.isna(p) else f"{p:.2f}"
        if not rs and not ps:
            return ""
        return f"{rs}|{ps}"

    all_df["cell"] = all_df.apply(fmt_cell, axis=1)
    all_df["row_key"] = all_df["bk_code"].astype(str) + "|" + all_df["bk_name"].astype(str)

    # Pivot
    wide = all_df.pivot_table(index="row_key", columns="trade_date", values="cell", aggfunc="first")
    wide = wide.sort_index(axis=1)

    # ===== Ensure all boards appear =====
    # 行索引按完整清单补齐（缺失的留空）
    full_index = pd.Index([f"{c}|{n}" for c, n in boards], name="row_key")
    wide = wide.reindex(full_index)

    # 输出 CSV
    wide.to_csv(out_csv, encoding="utf-8-sig", index_label="trade_date")
    print(f"[done] wrote: {out_csv}")

    # 简短预览
    with pd.option_context("display.max_columns", None, "display.width", 180):
        print(wide.head().iloc[:, :min(6, wide.shape[1])])


# ================== CLI ==================
def parse_args():
    ap = argparse.ArgumentParser(description="Export ALL boards rank|pct_chg pivot CSV (last N trading days).")
    ap.add_argument("--fs", default=BOARD_FS, help='industry="m:90+t:2", concept="m:90+t:3", region="m:90+t:1"')
    ap.add_argument("--days", type=int, default=N_DAYS, help="recent trading days")
    ap.add_argument("--out", default=OUTPUT_CSV, help="output CSV path")
    ap.add_argument("--workers", type=int, default=MAX_WORKERS, help="parallel workers for phase1")
    ap.add_argument("--slow-rounds", type=int, default=SLOW_RETRY_ROUNDS, help="sequential slow retry rounds")
    return ap.parse_args()


if __name__ == "__main__":
    args = parse_args()
    # 覆盖默认值
    build_csv(fs=args.fs, n_days=args.days, out_csv=args.out, max_workers=args.workers)