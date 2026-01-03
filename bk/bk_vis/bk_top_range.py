#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
按日 TopK 版块分布（可视化=每个版块一条水平基准线；基准线上方红柱=上涨，下方绿柱=下跌）
- TOPK：每日取前 K 名（仅用于“筛选哪些板块显示”）
- MIN_TIMES：只展示在整个区间内进入 TopK 次数 ≥ MIN_TIMES 的版块（筛选规则不变）
- N_DAYS：只统计最近 N 天（按列名日期解析并排序后截取）
- y 轴排序：按“进入 TopK 的总出现次数”降序（出现次数最多的在最上面），然后按“第一名次数”降序，再按名称升序
- x 轴标签仅显示“月-日”；仅在周一打刻度；但“每天”画一条虚线分割线
- 可视化：对“被保留的板块”在“所有天”都绘制涨跌柱（不再只画进 TopK 的那几天）
- 悬停十字：开启 x/y 轴 spikes，显示所在行与列
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go

# ===== 可调参数 =====
TOPK = 1         # 每天统计前 K 名：3、5、10...
MIN_TIMES = 3    # 只展示在整个区间内进入 TopK 次数 ≥ MIN_TIMES 的版块
N_DAYS = 90      # 只统计最近 N 天
INPUT_CSV = "bk_a_rank_pct_value.csv"
OUTPUT_HTML = "bk_a_topK_daily_bar_all_days.html"

# 柱形宽度（按“天”的比例，0.8 表示占一天的 80%）
BAR_DAY_FRACTION = 0.8
# 最大绝对涨跌幅映射到“行高”的占比（0~0.49 推荐），避免柱子跨行
ROW_HALF_HEIGHT = 0.45


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

    # —— 抽取板块名 —— #
    if "row_key" not in df.columns:
        raise RuntimeError("缺少列 row_key。需要形如 'BKxxxx|板块名称' 的键。")

    key_split = df["row_key"].astype(str).str.split("|", n=1, expand=True)
    df["board_name"] = key_split[1].fillna(key_split[0]).astype(str).str.strip()

    # —— 识别日期列、排序并截取最近 N_DAYS —— #
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
    dt_index = [d for _, d in valid_pairs]

    # —— 解析所有日期列为 (rank, pct, val) —— #
    parsed = {
        c: pd.DataFrame(df[c].apply(parse_triplet).tolist(),
                        columns=["rank", "pct", "val"], index=df.index)
        for c in date_cols
    }

    # —— 基于“每日 TopK”统计，确定要显示的板块（筛选规则不变）—— #
    topk_records = []
    for c in date_cols:
        dframe = parsed[c].copy()
        dframe = dframe.dropna(subset=["rank"])
        if dframe.empty:
            continue
        # 同名次时，以涨幅高者优先（极小扰动保证稳定顺序）
        dframe["_order"] = dframe["rank"] + (1 - dframe["pct"].fillna(0.0)) * 1e-6
        top = dframe.nsmallest(TOPK, ["rank", "_order"]).drop(columns=["_order"])
        for idx, row in top.iterrows():
            topk_records.append({
                "date": c,
                "board_name": df.at[idx, "board_name"],
                "rank": int(row["rank"]),
                "pct": row["pct"],
                "val": row["val"],
            })

    topk_df = pd.DataFrame(topk_records)
    if topk_df.empty:
        print(f"[warn] 选取最近 {len(date_cols)} 天后没有任何 Top{TOPK} 记录。")
        return

    topk_df["date"] = pd.to_datetime(topk_df["date"])
    topk_df.sort_values(["date", "rank"], inplace=True)

    # —— 只保留进入 TopK 次数 ≥ MIN_TIMES 的板块 —— #
    topk_cnt_all = topk_df.groupby("board_name")["rank"].size().rename("topk_cnt")
    keep_names = topk_cnt_all[topk_cnt_all >= MIN_TIMES].index.tolist()
    if not keep_names:
        print(f"[warn] 最近 {len(date_cols)} 天内，无版块满足 进入 Top{TOPK} ≥ {MIN_TIMES} 次 的条件。")
        return

    # —— y 轴排序：按 “TopK 总出现次数” ↓，再按 “第一名次数” ↓，再按 名称 ↑ —— #
    first_cnt = (topk_df[topk_df["rank"] == 1]
                 .groupby("board_name")["rank"].size()
                 .rename("first_cnt"))
    topk_cnt = topk_df.groupby("board_name")["rank"].size().rename("topk_cnt")
    order_df = pd.concat([first_cnt, topk_cnt], axis=1).fillna(0).astype(int)
    order_df = order_df.loc[order_df.index.intersection(keep_names)]
    order_df = order_df.sort_values(
        by=["topk_cnt", "first_cnt", "board_name"],   # ⬅️ 关键修改：先按出现次数
        ascending=[False,      False,       True]
    )
    boards = order_df.index.tolist()
    n_board = len(boards)

    # —— long_df：对“被保留的板块”，在“所有天”都拼接 pct（每天都显示） —— #
    records_full = []
    # 为了从 df 找到该板块的行索引，先建映射（同名取首个）
    name2idx = {}
    for idx, bname in enumerate(df["board_name"].tolist()):
        if bname not in name2idx and bname in keep_names:
            name2idx[bname] = idx

    for b in boards:
        if b not in name2idx:
            continue
        idx = name2idx[b]
        for c, d in zip(date_cols, dt_index):
            r, p, v = parsed[c].loc[idx, ["rank", "pct", "val"]]
            records_full.append({
                "date": d,
                "board_name": b,
                "rank": r,
                "pct": p,
                "val": v,
            })

    long_df = pd.DataFrame(records_full)
    long_df["date"] = pd.to_datetime(long_df["date"])
    long_df.sort_values(["date", "board_name"], inplace=True)

    # —— 可视化：每行一个板块的“基准线”，每天一个红/绿柱 —— #
    board_ypos = {b: i + 1 for i, b in enumerate(boards)}  # 行号从 1 开始（列表前面的在最上面）
    # 根据“被保留板块在所有天”的最大绝对涨跌幅设置缩放
    if long_df["pct"].notna().any():
        max_abs_pct = np.nanmax(np.abs(long_df["pct"].values))
    else:
        max_abs_pct = 0.0
    scale = (ROW_HALF_HEIGHT / max_abs_pct) if (max_abs_pct and max_abs_pct > 0) else 0.0

    x_min = min(dt_index)
    x_max = max(dt_index)
    one_day_ms = 24 * 60 * 60 * 1000
    bar_half_ms = one_day_ms * BAR_DAY_FRACTION / 2.0

    fig = go.Figure()
    shapes = []
    hover_x, hover_y, hover_text = [], [], []

    # —— 画柱（矩形） —— #
    for _, row in long_df.iterrows():
        b = row["board_name"]
        d = row["date"]
        pct = row["pct"]
        val = row["val"]
        if pd.isna(pct):
            continue
        y0 = board_ypos[b]
        y1 = y0 + scale * pct
        if pct == 0:
            continue

        x0 = d - pd.Timedelta(milliseconds=bar_half_ms)
        x1 = d + pd.Timedelta(milliseconds=bar_half_ms)
        y_bottom, y_top = (min(y0, y1), max(y0, y1))
        color = "rgba(220,0,0,0.85)" if pct > 0 else "rgba(0,140,0,0.85)"

        shapes.append(dict(
            type="rect",
            x0=x0, x1=x1,
            y0=y_bottom, y1=y_top,
            line=dict(width=0),
            fillcolor=color,
            layer="above"
        ))

        hover_x.append(d)
        hover_y.append(y1)
        hover_text.append(
            f"版块：{b}<br>"
            f"日期：{pd.to_datetime(d).strftime('%Y-%m-%d')}<br>"
            f"涨跌幅：{pct:+.2%}<br>"
            f"指数：{(val if pd.notna(val) else float('nan')):,.2f}"
        )

    # —— 每个板块的水平基准线 —— #
    for b in boards:
        y = board_ypos[b]
        shapes.append(dict(
            type="line",
            x0=x_min, x1=x_max,
            y0=y, y1=y,
            line=dict(color="rgba(0,0,0,0.25)", width=1)
        ))

    # —— 每天的淡色竖向分割线 —— #
    for d in dt_index:
        shapes.append(dict(
            type="line",
            x0=d, x1=d,
            y0=0.5, y1=n_board + 0.5,
            line=dict(color="rgba(0,0,0,0.15)", width=1, dash="dot"),
            layer="below"
        ))

    fig.update_layout(shapes=shapes)

    # —— 透明散点（承载 hover） —— #
    if hover_x:
        fig.add_trace(go.Scatter(
            x=hover_x,
            y=hover_y,
            mode="markers",
            name="",
            marker=dict(size=1, opacity=0),
            hoverinfo="text",
            hovertext=hover_text,
            showlegend=False
        ))

    # —— X 轴仅周一打刻度 —— #
    monday_dt = [d for d in dt_index if d.weekday() == 0]

    # —— 图例（红/绿） —— #
    fig.add_trace(go.Scatter(
        x=[None], y=[None],
        mode="markers",
        name="上涨（红）",
        marker=dict(size=10, color="rgba(220,0,0,0.85)")
    ))
    fig.add_trace(go.Scatter(
        x=[None], y=[None],
        mode="markers",
        name="下跌（绿）",
        marker=dict(size=10, color="rgba(0,140,0,0.85)")
    ))

    grid_color = "rgba(0,0,0,0.15)"
    grid_width = 1
    title = (
        f"按日 Top{TOPK} · 涨跌柱分布（最近 {len(dt_index)} 天；"
        f"显示板块：进入前{TOPK} ≥ {MIN_TIMES} 次；排序=出现次数↓→第一名次数↓→名称↑）"
    )

    tickvals = [board_ypos[b] for b in boards]
    ticktext = boards

    fig.update_layout(
        title=title,
        hovermode="closest",
        xaxis=dict(
            type="date",
            title=None,
            tickmode="array",
            tickvals=monday_dt if monday_dt else None,
            tickformat="%m-%d",
            showgrid=True,
            gridcolor=grid_color,
            gridwidth=grid_width,
            ticks="outside",
            showspikes=True,
            spikemode="across",
            spikesnap="cursor",
            spikethickness=1,
            spikedash="dot",
            spikecolor="rgba(0,0,0,0.35)",
            range=[x_min - pd.Timedelta(days=1), x_max + pd.Timedelta(days=1)],
        ),
        yaxis=dict(
            title="（排序：出现次数↓ → 第一名次数↓ → 名称↑）",
            type="linear",
            tickmode="array",
            tickvals=tickvals,
            ticktext=ticktext,
            range=[0.5, n_board + 0.5],
            showgrid=False,
            showspikes=True,
            spikemode="across",
            spikesnap="cursor",
            spikethickness=1,
            spikedash="dot",
            spikecolor="rgba(0,0,0,0.35)",
        ),
        legend=dict(y=1.0, x=1.02, title=None),
        margin=dict(l=120, r=180, t=70, b=50),
        height=860
    )

    fig.write_html(OUTPUT_HTML, include_plotlyjs="cdn")
    print(f"[ok] 已生成：{OUTPUT_HTML}")

if __name__ == "__main__":
    main()
