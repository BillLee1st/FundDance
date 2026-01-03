#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
按日 TopK 版块分布（颜色=名次 1~K）
- TOPK：每日取前 K 名
- MIN_TIMES：只展示进入 TopK 次数 ≥ MIN_TIMES 的版块
- N_DAYS：只统计最近 N 天（按列名日期解析并排序后截取）
- y 轴排序：先“第一名次数”降序，再“进入TopK总次数”降序，再按名称
- x 轴标签仅显示“月-日”；仅在周一打刻度；但“每天”画一条虚线分割线
- 悬停十字：开启 x/y 轴 spikes，显示所在行与列
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.colors import qualitative

# ===== 可调参数 =====
TOPK = 5         # 每天展示前 K 名：3、5、10...
MIN_TIMES = 10   # 只展示在整个区间内进入 TopK 次数 ≥ MIN_TIMES 的版块
N_DAYS = 90      # 只统计最近 N 天
INPUT_CSV = "bk_a_rank_pct_value.csv"
OUTPUT_HTML = "bk_a_topK_daily_dot.html"

def parse_triplet(cell):
    """cell: 'rank|pct|value' -> (rank:int/NaN, pct:float小数/NaN, val:float/NaN)"""
    if pd.isna(cell):
        return (np.nan, np.nan, np.nan)
    try:
        parts = str(cell).split("|")
        rank_raw = float(parts[0])
        rank = int(rank_raw) if rank_raw > 0 and abs(rank_raw - int(rank_raw)) < 1e-9 else np.nan
        pct = float(parts[1]) / 100.0
        val = float(parts[2])
        return (rank, pct, val)
    except Exception:
        return (np.nan, np.nan, np.nan)

def main():
    df = pd.read_csv(INPUT_CSV)

    # 拆出板块名
    key_split = df["row_key"].str.split("|", n=1, expand=True)
    df["board_name"] = key_split[1].fillna(key_split[0]).astype(str).str.strip()

    # 解析日期列 → 排序 → 截取最近 N_DAYS
    all_cols = df.columns.tolist()
    non_date_cols = {"row_key", "board_code", "board_name"}
    raw_date_cols = [c for c in all_cols if c not in non_date_cols]
    col_dt = pd.to_datetime(raw_date_cols, errors="coerce")
    valid_pairs = [(c, d) for c, d in zip(raw_date_cols, col_dt) if pd.notna(d)]
    if not valid_pairs:
        raise RuntimeError("未检测到有效的日期列，请检查列名格式（YYYY-MM-DD）。")
    valid_pairs.sort(key=lambda x: x[1])
    if N_DAYS > 0:
        valid_pairs = valid_pairs[-N_DAYS:]
    date_cols = [c for c, _ in valid_pairs]
    dt_index = [d for _, d in valid_pairs]  # 与 date_cols 同顺序

    # 解析 rank/pct/val
    parsed = {
        c: pd.DataFrame(df[c].apply(parse_triplet).tolist(),
                        columns=["rank", "pct", "val"], index=df.index)
        for c in date_cols
    }

    # 每日 TopK
    records = []
    for c in date_cols:
        dframe = parsed[c].dropna(subset=["rank"]).copy()
        if dframe.empty:
            continue
        # 同名次时，以涨幅高者优先（极小扰动保证稳定顺序）
        dframe["_order"] = dframe["rank"] + (1 - dframe["pct"].fillna(0.0)) * 1e-6
        top = dframe.nsmallest(TOPK, ["rank", "_order"]).drop(columns=["_order"])
        for idx, row in top.iterrows():
            records.append({
                "date": c,
                "board_name": df.at[idx, "board_name"],
                "rank": int(row["rank"]),
                "pct": row["pct"],
                "val": row["val"],
            })

    long_df = pd.DataFrame(records)
    if long_df.empty:
        print(f"[warn] 选取最近 {len(date_cols)} 天后没有任何 Top{TOPK} 记录。")
        return

    # 转日期 & 排序
    long_df["date"] = pd.to_datetime(long_df["date"])
    long_df.sort_values(["date", "rank"], inplace=True)

    # 只保留进入 TopK 次数 ≥ MIN_TIMES 的版块
    topk_cnt_all = long_df.groupby("board_name")["rank"].size().rename("topk_cnt")
    keep_names = topk_cnt_all[topk_cnt_all >= MIN_TIMES].index.tolist()
    long_df = long_df[long_df["board_name"].isin(keep_names)]
    if long_df.empty:
        print(f"[warn] 最近 {len(date_cols)} 天内，无版块满足 进入 Top{TOPK} ≥ {MIN_TIMES} 次 的条件。")
        return

    # y 轴排序：第一名次数 ↓，进入TopK次数 ↓，名称 ↑
    first_cnt = (long_df[long_df["rank"] == 1]
                 .groupby("board_name")["rank"].size()
                 .rename("first_cnt"))
    topk_cnt = long_df.groupby("board_name")["rank"].size().rename("topk_cnt")
    order_df = pd.concat([first_cnt, topk_cnt], axis=1).fillna(0).astype(int)
    order_df = order_df.sort_values(
        by=["first_cnt", "topk_cnt", "board_name"],
        ascending=[False, False, True]
    )
    board_order = order_df.index.tolist()

    # 仅在“周一”显示刻度标签
    monday_dt = [d for d in dt_index if d.weekday() == 0]

    # 颜色映射（名次 → 颜色）
    base_palette = qualitative.Set1 + qualitative.Set3 + qualitative.Bold + qualitative.Dark24
    rank_colors = {r: base_palette[(r - 1) % len(base_palette)] for r in range(1, TOPK + 1)}

    # 画布
    fig = go.Figure()

    # 按名次分图层（同一日期列不同颜色）
    for r in range(1, TOPK + 1):
        sub = long_df[long_df["rank"] == r]
        if sub.empty:
            continue
        fig.add_trace(go.Scatter(
            x=sub["date"],
            y=sub["board_name"],                # 字符串，避免分类映射错位
            mode="markers",
            name=f"第{r}名",
            marker=dict(size=8, color=rank_colors[r]),
            hovertemplate=(
                "版块：%{y}<br>"
                "日期：%{x|%Y-%m-%d}<br>"
                "名次：%{customdata[0]}<br>"
                "涨跌幅：%{customdata[1]:.2%}<br>"
                "指数：%{customdata[2]:,.2f}<extra></extra>"
            ),
            customdata=np.stack([sub["rank"], sub["pct"], sub["val"]], axis=-1)
        ))

    # === 每一天都画虚线竖线（淡色，避免喧宾夺主） ===
    for d in dt_index:
        fig.add_vline(x=d, line_width=1, line_dash="dot", opacity=0.25)

    # 统一网格样式 + 十字指示（spikes）
    grid_color = "rgba(0,0,0,0.15)"
    grid_width = 1

    title = f"按日 Top{TOPK} · 版块分布（最近 {len(date_cols)} 天，进入前{TOPK} ≥ {MIN_TIMES} 次）"

    fig.update_layout(
        title=title,
        hovermode="closest",  # 使用 'closest'，兼容老版本
        xaxis=dict(
            type="date",
            title=None,
            tickmode="array",
            tickvals=monday_dt if monday_dt else None,  # 仅周一显示标签
            tickformat="%m-%d",                         # 只显示 月-日
            showgrid=True,
            gridcolor=grid_color,
            gridwidth=grid_width,
            ticks="outside",
            # 十字：x 轴 spike
            showspikes=True,
            spikemode="across",
            spikesnap="cursor",
            spikedash="dot",
            spikecolor="rgba(0,0,0,0.35)",
            spikethickness=1,
        ),
        yaxis=dict(
            title="（优先：第一名次数↓，次之：入榜次数↓）",
            type="category",
            categoryorder="array",
            categoryarray=board_order[::-1],  # Plotly 从上到下渲染
            showgrid=True,
            gridcolor=grid_color,
            gridwidth=grid_width,
            # 十字：y 轴 spike
            showspikes=True,
            spikemode="across",
            spikesnap="cursor",
            spikedash="dot",
            spikecolor="rgba(0,0,0,0.35)",
            spikethickness=1,
        ),
        legend=dict(y=1.0, x=1.02, title=f"名次（1~{TOPK}）"),
        margin=dict(l=100, r=200, t=70, b=50),
        height=860
    )

    fig.write_html(OUTPUT_HTML, include_plotlyjs="cdn")
    print(f"[ok] 已生成：{OUTPUT_HTML}")

if __name__ == "__main__":
    main()