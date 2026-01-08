#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import pandas as pd
import time
from datetime import datetime

LIST_URL  = "https://push2.eastmoney.com/api/qt/clist/get"
KLINE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"

LOOKBACK_DAYS = 90
BASE_NET = 10000.0
OUT_CSV = "concept_board_90d.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://quote.eastmoney.com/"
}


# =========================
# 日志打印函数
# =========================
def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


# =========================
# 1. 获取所有概念板块
# =========================
def fetch_all_concept_boards():
    rows = []
    pn = 1
    pz = 200

    while True:
        log(f"概念板块列表：请求第 {pn} 页")

        params = {
            "pn": pn,
            "pz": pz,
            "fs": "m:90+t:3",
            "fields": "f12,f14",
            "_": int(time.time() * 1000)
        }

        try:
            r = requests.get(LIST_URL, params=params, headers=HEADERS, timeout=10)
            js = r.json()
        except Exception as e:
            log(f"❌ 列表接口异常，第 {pn} 页：{e}")
            time.sleep(3)
            continue

        data = js.get("data")
        if not data or not data.get("diff"):
            log(f"📌 第 {pn} 页无数据，结束分页")
            break

        diff = data["diff"]
        rows.extend(diff)
        log(f"✔ 第 {pn} 页获取 {len(diff)} 条，累计 {len(rows)}")

        pn += 1
        time.sleep(3)  # 每页列表请求间隔 3 秒

    df = pd.DataFrame(rows)
    df.rename(columns={"f12": "code", "f14": "name"}, inplace=True)
    return df


# =========================
# 2. 获取单个板块K线
# =========================
def fetch_board_kline(code):
    params = {
        "secid": f"90.{code}",
        "klt": 101,     # 日K
        "fqt": 1,
        "lmt": LOOKBACK_DAYS,
        "fields1": "f1,f2,f3,f4,f5",
        "fields2": "f51,f52,f53,f54",
        "_": int(time.time() * 1000)
    }

    try:
        r = requests.get(KLINE_URL, params=params, headers=HEADERS, timeout=10)
        js = r.json()
    except Exception as e:
        log(f"❌ K线接口异常 {code}：{e}")
        return None

    data = js.get("data")
    if not data or not data.get("klines"):
        log(f"⚠️ 无K线数据：{code}")
        return None

    klines = data["klines"]
    df = pd.DataFrame(
        [k.split(",") for k in klines],
        columns=["date", "open", "close", "high", "low"]
    )
    df["date"] = pd.to_datetime(df["date"])
    df["close"] = df["close"].astype(float)
    df["pct"] = df["close"].pct_change() * 100
    return df


# =========================
# 3. 主流程
# =========================
def main():
    log("开始获取概念板块列表")
    boards = fetch_all_concept_boards()
    log(f"概念板块总数：{len(boards)}")

    all_daily = []

    for idx, row in boards.iterrows():
        code, name = row["code"], row["name"]
        log(f"[{idx + 1}/{len(boards)}] 拉取K线：{code} {name}")

        df = fetch_board_kline(code)
        if df is None or len(df) < 2:
            time.sleep(3)  # 异常也等待 3 秒
            continue

        df = df.tail(LOOKBACK_DAYS)
        df["net"] = BASE_NET * (df["close"] / df["close"].iloc[0])
        df["code"] = code
        df["name"] = name

        all_daily.append(df[["date", "code", "name", "pct", "net"]])
        time.sleep(3)  # K线请求间隔 3 秒

    if not all_daily:
        log("❌ 没有任何有效K线数据，程序终止")
        return

    all_df = pd.concat(all_daily, ignore_index=True)

    # =========================
    # 4. 每日排名
    # =========================
    log("计算每日排名")
    all_df["rank"] = (
        all_df.groupby("date")["pct"]
        .rank(method="min", ascending=False)
        .astype(int)
    )

    # =========================
    # 5. 生成 CSV
    # =========================
    log("生成 CSV 文件")

    all_df["cell"] = (
        all_df["rank"].astype(str) + "|" +
        all_df["pct"].round(2).astype(str) + "|" +
        all_df["net"].round(2).astype(str)
    )

    all_df["row_key"] = all_df["code"] + "|" + all_df["name"]

    pivot = all_df.pivot(
        index="row_key",
        columns="date",
        values="cell"
    )

    pivot.columns = [d.strftime("%Y-%m-%d") for d in pivot.columns]
    pivot.reset_index(inplace=True)

    pivot.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    log(f"✅ 完成，生成文件：{OUT_CSV}")


if __name__ == "__main__":
    main()
