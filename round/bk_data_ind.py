#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
A股板块抓取（Eastmoney，批量抓“今天” + 稳健二轮 + 基线建一次）
- CSV 不存在（基线）：抓最近 N 天（push2his），建立全量历史（一次性）
- CSV 存在（增量）：今天列改为【clist/get 批量抓】（一次请求覆盖全部板块）
- 单元格：rank|pct_chg|value（value≈最新价，收盘后即收盘价；盘中为最新成交价）
- 失败保护：今天列若已有旧值，本次失败/空值将保留旧值
- 二轮补抓：仅用于“基线”或你切换到 his 模式时；默认今天用批量抓无需二轮
- 日志：每个阶段打印单行日志；支持 --verbose-http 打印 HTTP 细节
- Ctrl+C：中断时已获取的数据也会写回今天列
"""

import os, sys, time, math, random, argparse, signal
from typing import List, Tuple, Optional, Dict
from datetime import datetime
from pandas.tseries.offsets import BDay

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import ProxyError, ConnectionError as ReqConnErr, ReadTimeout

import pandas as pd
import akshare as ak

# ================== Tunables ==================
N_DAYS = 90
RETRY_TIMES = 4
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://quote.eastmoney.com/",
    "Accept": "application/json, text/plain, */*",
    "Connection": "keep-alive",
}

LIST_URL   = "https://push2.eastmoney.com/api/qt/clist/get"           # 批量列表（今天用它）
KLINE_URL  = "https://push2his.eastmoney.com/api/qt/stock/kline/get"  # 历史K线（仅基线）
BOARD_FS   = "m:90+t:2"     # 行业
# BOARD_FS   = "m:90+t:3"     # 概念
OUTPUT_CSV = "data_industry.csv"

INTERRUPTED = False
def _sigint_handler(signum, frame):
    global INTERRUPTED
    INTERRUPTED = True
signal.signal(signal.SIGINT, _sigint_handler)


# =============== HTTP & throttle helpers ===============
def build_session(timeout_s: float) -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    retry = Retry(
        total=RETRY_TIMES,
        backoff_factor=0.25,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=16, pool_maxsize=16)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.request_timeout = timeout_s  # 自定义属性
    return s


def polite_sleep(min_interval_s: float, base_sleep: float, jitter: float, last_ts_holder: list):
    """满足 rpm 的最小间隔，再叠加自定义等待与抖动。"""
    now = time.time()
    last_ts = last_ts_holder[0]
    need = 0.0
    if last_ts is not None:
        elapsed = now - last_ts
        if elapsed < min_interval_s:
            need = (min_interval_s - elapsed)
    total = max(0.0, need) + max(0.0, base_sleep) + (random.random() * max(0.0, jitter))
    if total > 0:
        time.sleep(total)
    last_ts_holder[0] = time.time()


# =============== Eastmoney fetchers ===============
def http_get(session: requests.Session, url: str, params: dict, verbose_http: bool) -> requests.Response:
    t0 = time.time()
    resp = session.get(url, params=params, timeout=session.request_timeout)
    dt = (time.time() - t0) * 1000.0
    if verbose_http:
        path = url.split("//", 1)[-1].split("/", 1)[-1]
        # 只打印关键字段，避免超长
        log_params = {k: params.get(k) for k in ("fs","secid","beg","end","klt","lmt")}
        print(f"[http] GET /{path} {log_params} -> {resp.status_code} ({dt:.0f}ms)")
    return resp


def fetch_board_list_basic(
    session: requests.Session,
    fs: str,
    verbose_http: bool,
    page_size: int = 100,
    max_pages: int = 50,
) -> List[Tuple[str, str]]:
    """
    自动分页获取所有板块 (bk_code, bk_name)
    clist/get 实际每页最多 100 条，pz>100 无效
    """

    all_rows: List[Tuple[str, str]] = []
    seen = set()

    for pn in range(1, max_pages + 1):
        params = {
            "pn": pn,
            "pz": page_size,   # 实际最大 100
            "po": 1,
            "np": 1,
            "fltt": 2,
            "invt": 2,
            "fid": "f3",
            "fs": fs,
            "fields": "f12,f14",
        }

        r = http_get(session, LIST_URL, params, verbose_http)
        r.raise_for_status()

        data = r.json().get("data") or {}
        diff = data.get("diff") or []

        if not diff:
            if verbose_http:
                print(f"[info] page {pn}: empty diff, stop paging")
            break

        new_cnt = 0
        for it in diff:
            code = it.get("f12")
            name = it.get("f14")
            if not code or not name:
                continue
            key = (code, name)
            if key in seen:
                continue
            seen.add(key)
            all_rows.append(key)
            new_cnt += 1

        if verbose_http:
            total = data.get("total")
            print(f"[info] page {pn}: fetched={len(diff)}, new={new_cnt}, total={total}")

        # 已拿完
        total = data.get("total")
        if total and len(all_rows) >= total:
            break

    return all_rows


def fetch_board_list_today(
    session: requests.Session,
    fs: str,
    verbose_http: bool,
    page_size: int = 100,
    max_pages: int = 50,
) -> pd.DataFrame:
    """
    分页批量获取所有板块的“最新价 & 涨跌幅(%)”
    - clist/get 单页最大 100 条
    - 自动翻页直到拿全
    - 打印抓取数量日志
    """

    rows = []
    seen = set()

    for pn in range(1, max_pages + 1):
        params = {
            "pn": pn,
            "pz": page_size,   # 实际最大 100
            "po": 1,
            "np": 1,
            "fltt": 2,
            "invt": 2,
            "fid": "f3",
            "fs": fs,
            # "fields": "f12,f14,f2,f3",
            "fields": "f12,f14,f2,f3,f8,f104,f105,f128",
        }

        r = http_get(session, LIST_URL, params, verbose_http)
        r.raise_for_status()

        data = r.json().get("data") or {}
        diff = data.get("diff") or []

        if not diff:
            print(f"[info] today clist page {pn}: empty diff, stop paging")
            break

        page_new = 0
        for it in diff:
            code = it.get("f12")
            name = it.get("f14")
            if not code or not name:
                continue

            key = (code, name)
            if key in seen:
                continue
            seen.add(key)

            try:
                close = float(it.get("f2")) if it.get("f2") not in (None, "", "--") else math.nan
            except Exception:
                close = math.nan

            try:
                pct = float(it.get("f3")) if it.get("f3") not in (None, "", "--") else math.nan
            except Exception:
                pct = math.nan
            
            try:
                turnover = float(it.get("f8")) if it.get("f8") not in (None, "", "--") else math.nan
            except Exception:
                turnover = math.nan

            up_count = it.get("f104") or ""
            down_count = it.get("f105") or ""
            leader = it.get("f128") or ""


            # rows.append({
            #     "bk_code": code,
            #     "bk_name": name,
            #     "close": close,
            #     "pct_chg": pct,
            # })

            rows.append({
                "bk_code": code,
                "bk_name": name,
                "close": close,
                "pct_chg": pct,
                "turnover": turnover,
                "up_count": up_count,
                "down_count": down_count,
                "leader": leader,
            })


            page_new += 1

        total = data.get("total")
        print(
            f"[info] today clist page {pn}: "
            f"fetched={len(diff)}, new={page_new}, "
            f"accumulated={len(rows)}, total={total}"
        )

        # 已抓全
        if total and len(rows) >= total:
            break

    df = pd.DataFrame(rows)
    print(f"[summary] today clist fetched rows={len(df)}")

    return df




def _parse_klines(kl):
    """
    fields2：
    0 f51=日期, 1 f52=开盘, 2 f53=收盘, 3 f54=最高, 4 f55=最低,
    5 f56=成交量, 6 f57=成交额, 7 f58=振幅, 8 f59=涨跌幅(%), 9 f60=涨跌额, 10 f61=换手率
    """
    recs = []
    for row in kl or []:
        parts = row.split(",")
        if len(parts) >= 11:
            date = parts[0]
            try:
                close = float(parts[2]) if parts[2] != "" else math.nan
            except ValueError:
                close = math.nan
            try:
                pct = float(parts[8]) if parts[8] != "" else math.nan
            except ValueError:
                pct = math.nan
            recs.append((date, pct, close))
    return recs


def _fetch_range(session: requests.Session, bk_code: str, beg: str, end: str, verbose_http: bool):
    params = {
        "fields1": "f1,f2,f3,f4,f5",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": 101, "fqt": 0,
        "secid": f"90.{bk_code}",
        "beg": beg, "end": end,
    }
    r = http_get(session, KLINE_URL, params, verbose_http)
    r.raise_for_status()
    kl = (r.json().get("data") or {}).get("klines")
    return _parse_klines(kl)


# =============== cell helpers ===============
# def fmt_cell(rank: Optional[int], pct: Optional[float], close: Optional[float]) -> str:
#     r = "" if rank is None else str(int(rank))
#     p = "" if pct is None or (isinstance(pct, float) and math.isnan(pct)) else f"{pct:.2f}"
#     if close is None or (isinstance(close, float) and math.isnan(close)):
#         c = ""
#     else:
#         c = f"{close:.4f}".rstrip("0").rstrip(".")
#     if not r and not p and not c:
#         return ""
#     return f"{r}|{p}|{c}"
def fmt_cell(rank, pct, close, turnover=None, up=None, down=None, leader=None):
    r = "" if rank is None else str(int(rank))
    p = "" if pct is None or (isinstance(pct, float) and math.isnan(pct)) else f"{pct:.2f}"

    if close is None or (isinstance(close, float) and math.isnan(close)):
        c = ""
    else:
        c = f"{close:.4f}".rstrip("0").rstrip(".")

    t = "" if turnover is None or (isinstance(turnover, float) and math.isnan(turnover)) else f"{turnover:.2f}"
    u = "" if up in (None, "") else str(up)
    d = "" if down in (None, "") else str(down)
    l = "" if not leader else str(leader)

    parts = [r, p, c, t, u, d, l]
    if all(x == "" for x in parts):
        return ""
    return "|".join(parts)


# =============== write helpers ===============
def write_baseline(all_rows: list, out_csv: str):
    all_df = pd.concat(all_rows, ignore_index=True)
    all_df["trade_date"] = pd.to_datetime(all_df["trade_date"]).dt.date
    all_df["rank"] = all_df.groupby("trade_date")["pct_chg"].rank(ascending=False, method="first")
    all_df["row_key"] = all_df["bk_code"] + "|" + all_df["bk_name"]
    all_df["cell"] = all_df.apply(lambda r: fmt_cell(int(r["rank"]) if pd.notna(r["rank"]) else None,
                                                     r["pct_chg"], r["close"]), axis=1)
    pv = all_df.pivot_table(index="row_key", columns="trade_date", values="cell", aggfunc="first")
    pv.columns = [c.strftime("%Y-%m-%d") for c in pv.columns]
    pv = pv.sort_index(axis=1)
    pv.to_csv(out_csv, encoding="utf-8-sig", index_label="row_key")
    print(f"[done] wrote baseline to {out_csv}")


# def patch_today(wide: pd.DataFrame, df_today: pd.DataFrame, today_col: str, out_csv: str):
#     """将 df_today 写入今天列；新值为空则保留旧值。"""
#     df_today = df_today.copy()
#     if df_today["pct_chg"].notna().any():
#         df_today["rank"] = df_today["pct_chg"].rank(ascending=False, method="first")
#     else:
#         df_today["rank"] = pd.NA
#     df_today["row_key"] = df_today["bk_code"] + "|" + df_today["bk_name"]
#     df_today["cell_new"] = df_today.apply(
#         lambda r: fmt_cell(int(r["rank"]) if pd.notna(r["rank"]) else None, r["pct_chg"], r["close"]), axis=1
#     )

#     old_col = wide.get(today_col)
#     series_new = df_today.set_index("row_key")["cell_new"]
#     if old_col is None:
#         wide[today_col] = series_new
#     else:
#         combined = old_col.copy()
#         mask = series_new.notna() & (series_new.str.len() > 0)
#         combined.loc[mask] = series_new.loc[mask]
#         wide[today_col] = combined

#     # 列排序（按日期）
#     def _to_dt(x):
#         try: return pd.to_datetime(x)
#         except Exception: return pd.NaT
#     cols_dt = pd.Series({c: _to_dt(c) for c in wide.columns})
#     date_cols  = cols_dt[cols_dt.notna()].sort_values().index.tolist()
#     other_cols = [c for c in wide.columns if c not in date_cols]
#     wide = wide[date_cols + other_cols] if other_cols else wide[date_cols]
#     wide.to_csv(out_csv, encoding="utf-8-sig", index_label="row_key")
#     print(f"[done] patched today({today_col}) into {out_csv}")

def patch_today(wide: pd.DataFrame, df_today: pd.DataFrame, today_col: str, out_csv: str):
    """
    TODAY 更新规则：
    1) 接口有的数据 → 更新
    2) 接口缺失但 CSV 里今天已有 → 保留
    3) 接口缺失且 CSV 里今天为空 → 保持空
    4) 接口新增板块 → 追加新行
    """

    df_today = df_today.copy()

    # ===== 1. rank =====
    if df_today["pct_chg"].notna().any():
        df_today["rank"] = df_today["pct_chg"].rank(
            ascending=False, method="first"
        )
    else:
        df_today["rank"] = pd.NA

    df_today["row_key"] = df_today["bk_code"] + "|" + df_today["bk_name"]
    df_today["cell_new"] = df_today.apply(
    lambda r: fmt_cell(
            int(r["rank"]) if pd.notna(r["rank"]) else None,
            r["pct_chg"],
            r["close"],
            r.get("turnover"),
            r.get("up_count"),
            r.get("down_count"),
            r.get("leader"),
        ),
        axis=1,
    )


    # df_today["cell_new"] = df_today.apply(
    #     lambda r: fmt_cell(
    #         int(r["rank"]) if pd.notna(r["rank"]) else None,
    #         r["pct_chg"],
    #         r["close"],
    #     ),
    #     axis=1,
    # )

    new_series = df_today.set_index("row_key")["cell_new"]

    # ===== 2. 确保 wide index =====
    if wide.index.name != "row_key":
        wide.index.name = "row_key"

    # ===== 3. 追加新增板块 =====
    new_keys = new_series.index.difference(wide.index)
    if len(new_keys) > 0:
        print(f"[info] new boards today: {len(new_keys)}")
        for k in new_keys:
            print(f"  + {k}")

        # 创建空行并 append
        append_df = pd.DataFrame(index=new_keys, columns=wide.columns)
        wide = pd.concat([wide, append_df])

    # ===== 异常检测：接口缺失但 CSV 今天已有 =====
    if today_col in wide.columns:
        existing_today = wide[today_col]
        missing_keys = wide.index.difference(new_series.index)

        # CSV 里今天有值，但接口没返回
        abnormal_keys = [
            k for k in missing_keys
            if pd.notna(existing_today.get(k)) and str(existing_today.get(k)).strip() != ""
        ]

        if abnormal_keys:
            print(f"[warn] {len(abnormal_keys)} boards missing from API but kept from CSV:")
            for k in abnormal_keys:
                print(f"  ! {k}")


    # ===== 4. 写 today 列（仅覆盖接口返回的）=====
    if today_col not in wide.columns:
        wide[today_col] = ""

    # 对齐索引
    aligned_new = new_series.reindex(wide.index)

    # 只覆盖「接口实际返回的非空值」
    mask = aligned_new.notna() & (aligned_new.str.len() > 0)
    wide.loc[mask, today_col] = aligned_new.loc[mask]

    # ===== 5. 日期列排序 =====
    def _to_dt(x):
        try:
            return pd.to_datetime(x)
        except Exception:
            return pd.NaT

    cols_dt = pd.Series({c: _to_dt(c) for c in wide.columns})
    date_cols = cols_dt[cols_dt.notna()].sort_values().index.tolist()
    other_cols = [c for c in wide.columns if c not in date_cols]
    wide = wide[date_cols + other_cols] if other_cols else wide[date_cols]

    wide.to_csv(out_csv, encoding="utf-8-sig", index_label="row_key")
    print(f"[done] patched today({today_col}) into {out_csv}")



def get_latest_trade_date():
    today = datetime.now().strftime("%Y-%m-%d")
    try:
        trade_cal = ak.tool_trade_date_hist_sina()
        trade_dates = trade_cal[trade_cal['trade_date'].notna()]['trade_date'].tolist()
        trade_dates = [d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d) for d in trade_dates]
        if today in trade_dates:
            return today
        for d in reversed(trade_dates):
            if d < today:
                return d
    except Exception as e:
        print(f"[WARN] 获取交易日失败，使用今天: {e}")
    return today




# =============== main ===============
def build_csv(
    fs: str,
    n_days: int,
    out_csv: str,
    sleep_s: float,
    jitter_s: float,
    rpm: float,
    cooldown_after: int,
    cooldown_secs: float,
    timeout_s: float,
    pass2_timeout: float,
    pass2_sleep: float,
    verbose_http: bool,
    today_mode: str,
):
    session = build_session(timeout_s)

    # today = datetime.now().date()
    # today_str = today.strftime("%Y%m%d")
    # today_col = today.strftime("%Y-%m-%d")
        # 获取今天日期
    # now = datetime.now().date()
    # # 假设东财接口返回的是最近交易日（若今天非交易日，返回前一个交易日）
    # # pd.Timestamp + BDay(0) 会返回最近交易日（包括今天）
    # last_trade_day = (pd.Timestamp(now) - BDay(0)).date()
    # # 接口请求的日期字符串
    # today_str = last_trade_day.strftime("%Y%m%d")
    # # 对应写入的列名
    # today_col = last_trade_day.strftime("%Y-%m-%d")


    # ================== 获取最近交易日（统一入口） ==================
    latest_trade_date_str = get_latest_trade_date()   # 'YYYY-MM-DD'

    # 统一派生三个变量（保持原语义）
    last_trade_day = datetime.strptime(latest_trade_date_str, "%Y-%m-%d").date()
    today_col = latest_trade_date_str                 # 写入 DataFrame 的列名
    today_str = last_trade_day.strftime("%Y%m%d")     # 接口参数


    print(f"[info] updating data for trading day: {today_col}")







    # 读旧CSV
    if os.path.exists(out_csv):
        wide = pd.read_csv(out_csv, index_col=0)
        wide.columns = [str(c) for c in wide.columns]
        print(f"[info] CSV exists → update TODAY via '{today_mode}'.")
    else:
        wide = pd.DataFrame()
        print("[info] No CSV found → full fetch to build baseline.")

    # ====== 如果没有基线，先建一次（push2his；可能慢，但只做一次）======
    if wide.empty:
        print("[stage] bootstrap baseline (push2his, may take time)…")
        boards = fetch_board_list_basic(session, fs, verbose_http)
        total = len(boards)
        print(f"[info] total boards (list): {total}")

        # rpm 限速器
        min_interval_s = 60.0 / rpm if rpm and rpm > 0 else 0.0
        last_ts_holder = [None]

        all_rows = []
        consec_fail = 0
        for i, (code, name) in enumerate(boards, start=1):
            if INTERRUPTED:
                print("[warn] interrupted, flushing baseline…")
                break
            polite_sleep(min_interval_s, sleep_s, jitter_s, last_ts_holder)

            ok = False; err = None; recs = None
            backoff = 0.6
            for attempt in range(RETRY_TIMES):
                try:
                    beg = (last_trade_day - pd.Timedelta(days=n_days*2)).strftime("%Y%m%d")
                    recs = _fetch_range(session, code, beg, today_str, verbose_http)
                    ok = True; break
                except (ProxyError, ReqConnErr, ReadTimeout) as e:
                    err = e; time.sleep(backoff + random.uniform(0, 0.35)); backoff *= 2
                except Exception as e:
                    err = e; time.sleep(backoff); backoff *= 2

            if ok and recs:
                df = pd.DataFrame(recs, columns=["trade_date", "pct_chg", "close"])
                df["bk_code"] = code; df["bk_name"] = name
                all_rows.append(df); consec_fail = 0
                print(f"[full {i:02d}/{total}] {code}|{name} ok ({len(recs)})")
            else:
                consec_fail += 1
                print(f"[full {i:02d}/{total}] {code}|{name} FAIL: {err}")
                if consec_fail >= cooldown_after:
                    print(f"[cooldown] consecutive fails={consec_fail} → sleep {cooldown_secs}s")
                    time.sleep(cooldown_secs); consec_fail = 0

        if all_rows:
            write_baseline(all_rows, out_csv)
        return

    # ====== 有基线：今天列采用“批量抓”方案（默认）======
    if today_mode == "list":
        print("[stage] fetch TODAY via clist/get (one-shot for all boards)…")
        try:
            df_today = fetch_board_list_today(session, fs, verbose_http)
            if df_today.empty:
                print("[warn] clist/get returns empty, keep old today column.")
            else:
                patch_today(wide, df_today, today_col, out_csv)
            ok_n = df_today["pct_chg"].notna().sum() if not df_today.empty else 0
            total = len(df_today) if not df_today.empty else 0
            print(f"[summary] today (list): ok={ok_n}, total={total}, date={today_col}")
        except Exception as e:
            print(f"[error] clist/get failed: {e}  -> keep old today column")
        return

    # ====== 兼容：如果你强制 today_mode=his，仍走逐板块 his 日K ======
    print("[stage] fetch TODAY via push2his per-board (compat mode)…")
    boards = fetch_board_list_basic(session, fs, verbose_http)
    total = len(boards)
    print(f"[info] total boards (list): {total}")

    min_interval_s = 60.0 / rpm if rpm and rpm > 0 else 0.0
    last_ts_holder = [None]

    rows_today: Dict[str, Dict] = {}
    fails: List[Tuple[str, str]] = []
    consec_fail = 0

    # Pass1
    for i, (code, name) in enumerate(boards, start=1):
        if INTERRUPTED:
            print("[warn] interrupted by user during Pass1, flushing partial…")
            break
        polite_sleep(min_interval_s, sleep_s, jitter_s, last_ts_holder)

        ok = False; err = None; recs = None
        backoff = 0.6
        for attempt in range(RETRY_TIMES):
            try:
                recs = _fetch_range(session, code, today_str, today_str, verbose_http)
                ok = True; break
            except (ProxyError, ReqConnErr, ReadTimeout) as e:
                err = e; time.sleep(backoff + random.uniform(0, 0.35)); backoff *= 2
            except Exception as e:
                err = e; time.sleep(backoff); backoff *= 2

        if ok and recs:
            _, pct, close = recs[0]
            rows_today[code] = {"bk_code": code, "bk_name": name, "pct_chg": pct, "close": close}
            consec_fail = 0
            print(f"[today {i:02d}/{total}] {code}|{name} ok")
        else:
            rows_today[code] = {"bk_code": code, "bk_name": name, "pct_chg": None, "close": None}
            fails.append((code, name))
            consec_fail += 1
            print(f"[today {i:02d}/{total}] {code}|{name} FAIL: {err}")
            if consec_fail >= cooldown_after:
                print(f"[cooldown] consecutive fails={consec_fail} → sleep {cooldown_secs}s")
                time.sleep(cooldown_secs); consec_fail = 0

    # Pass2（可选）
    if fails and not INTERRUPTED:
        print(f"[info] Pass2 retry for {len(fails)} failed boards (slower pacing)…")
        session2 = build_session(pass2_timeout)
        min_interval_s2 = 60.0 / (rpm/2.0) if rpm and rpm > 0 else 0.0
        last_ts_holder2 = [None]

        for j, (code, name) in enumerate(fails, start=1):
            if INTERRUPTED:
                print("[warn] interrupted by user during Pass2, flushing partial…")
                break

            polite_sleep(min_interval_s2, pass2_sleep, pass2_sleep/2, last_ts_holder2)

            ok = False; err = None; recs = None
            backoff = 1.0
            for attempt in range(RETRY_TIMES + 1):
                try:
                    recs = _fetch_range(session2, code, today_str, today_str, verbose_http)
                    ok = True; break
                except (ProxyError, ReqConnErr, ReadTimeout) as e:
                    err = e; time.sleep(backoff + random.uniform(0, 0.5)); backoff *= 2
                except Exception as e:
                    err = e; time.sleep(backoff); backoff *= 2

            if ok and recs:
                _, pct, close = recs[0]
                rows_today[code] = {"bk_code": code, "bk_name": name, "pct_chg": pct, "close": close}
                print(f"[pass2 {j:02d}/{len(fails)}] {code}|{name} ok")
            else:
                print(f"[pass2 {j:02d}/{len(fails)}] {code}|{name} FAIL: {err}")

    # 写回
    # 确保索引齐全
    all_index = pd.Index([f"{c}|{n}" for c, n in boards], name="row_key")
    if wide.index.name != "row_key":
        wide.index.name = "row_key"
    wide = wide.reindex(all_index)

    df_today = pd.DataFrame(list(rows_today.values()))
    if not df_today.empty:
        patch_today(wide, df_today, today_col, out_csv)
    else:
        print("[warn] nothing fetched; skip writing.")

    ok_n = sum(1 for v in rows_today.values() if v["pct_chg"] is not None)
    print(f"[summary] today (his): ok={ok_n}, fail={total-ok_n}, date={today_col}")


# =============== CLI ===============
def parse_args():
    ap = argparse.ArgumentParser(description="Batch TODAY via clist/get; full bootstrap via push2his when CSV absent.")
    ap.add_argument("--fs", default=BOARD_FS, help="industry='m:90+t:2', concept='m:90+t:3', region='m:90+t:1'")
    ap.add_argument("--days", type=int, default=N_DAYS, help="recent trading days for first full fetch")
    ap.add_argument("--out", default=OUTPUT_CSV, help="output CSV path")

    # 频控（主要用于基线 & 兼容 his 模式）
    ap.add_argument("--sleep", type=float, default=5.0, help="base sleep seconds (Pass1)")
    ap.add_argument("--jitter", type=float, default=0.4, help="random jitter seconds")
    ap.add_argument("--rpm", type=float, default=10.0, help="max requests per minute (0=disable)")

    # 稳健性
    ap.add_argument("--cooldown-after", type=int, default=20, help="global cooldown after N consecutive fails")
    ap.add_argument("--cooldown-secs", type=float, default=8.0, help="global cooldown seconds")
    ap.add_argument("--timeout", type=float, default=10, help="per-request timeout seconds (Pass1)")

    # Pass2（仅 his 模式会用到）
    ap.add_argument("--pass2-timeout", type=float, default=9.0, help="per-request timeout seconds (Pass2)")
    ap.add_argument("--pass2-sleep", type=float, default=2.0, help="base sleep seconds (Pass2)")

    # HTTP 细节
    ap.add_argument("--verbose-http", action="store_true", help="print each HTTP GET details")

    # 今天抓取模式：list（默认，强烈推荐）/ his（逐板块历史接口，兼容用）
    ap.add_argument("--today-mode", choices=["list","his"], default="list", help="how to fetch today's column")
    return ap.parse_args()


if __name__ == "__main__":
    args = parse_args()
    build_csv(
        fs=args.fs,
        n_days=args.days,
        out_csv=args.out,
        sleep_s=args.sleep,
        jitter_s=args.jitter,
        rpm=args.rpm,
        cooldown_after=args.cooldown_after,
        cooldown_secs=args.cooldown_secs,
        timeout_s=args.timeout,
        pass2_timeout=args.pass2_timeout,
        pass2_sleep=args.pass2_sleep,
        verbose_http=args.verbose_http,
        today_mode=args.today_mode,
    )

