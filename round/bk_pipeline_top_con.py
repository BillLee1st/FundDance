#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
综合版块可视化：
1) rank_*_*.html：Scatter 点 + 名次颜色 + 右侧累计涨幅
2) range_*_*.html：红绿柱 + 水平基准线 + 右侧累计涨幅
支持多 LOOKBACK 输出
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.colors import qualitative
from datetime import datetime

# ================== 参数 ==================
TOP_N_RANK = 30  # rank 页面 TOP
TOP_N_RANGE = 20 # range 页面 TOP
N_DAYS = 90
# LOOKBACK_LIST = [1, 5, 10, 20, 30, 60, 90]
LOOKBACK_LIST = [1, 5]
END_DATE = None

today = datetime.now().strftime("%m%d")
INPUT_CSV = "data_concept.csv"

# range 图专用参数
BAR_DAY_FRACTION = 0.8
ROW_HALF_HEIGHT = 0.45

# ================== 工具函数 ==================
def parse_triplet(cell):
    if pd.isna(cell):
        return (np.nan, np.nan, np.nan)
    try:
        r, p, v = str(cell).split("|")
        r = int(float(r)) if float(r) > 0 else np.nan
        return r, float(p)/100.0, float(v)
    except Exception:
        return (np.nan, np.nan, np.nan)

# ================== 数据准备 ==================
def prepare_data():
    df = pd.read_csv(INPUT_CSV)
    key_split = df["row_key"].astype(str).str.split("|", n=1, expand=True)
    df["board_name"] = key_split[1].fillna(key_split[0]).str.strip()

    # ===== 新增：过滤掉不需要的板块 =====
    EXCLUDE_BOARDS = {
        "昨日涨停_含一字",
        "昨日连板_含一字",
        "昨日涨停",
        "昨日连板",
        "次新股",
        "注册制次新股",
        "基金重仓",
        "百元股",
        "参股新三板",
        "2025中报扭亏",
        "2025三季报预增",
        "并购重组概念",
    }
    df = df[~df["board_name"].isin(EXCLUDE_BOARDS)].reset_index(drop=True)
    # ==========================================

    non_date = {"row_key", "board_code", "board_name"}
    raw_date_cols = [c for c in df.columns if c not in non_date]
    col_dt = pd.to_datetime(raw_date_cols, errors="coerce")

    all_dates = [(c, d) for c, d in zip(raw_date_cols, col_dt) if pd.notna(d)]
    all_dates.sort(key=lambda x: x[1])

    # 页面展示日期
    display_dates = all_dates[-N_DAYS:] if N_DAYS > 0 else all_dates
    display_cols = [c for c, _ in display_dates]
    display_dt = [d for _, d in display_dates]

    # 计算日期
    calc_dates = all_dates
    if END_DATE:
        end_dt = pd.to_datetime(END_DATE)
        calc_dates = [(c, d) for c, d in calc_dates if d <= end_dt]
    calc_dates = calc_dates[-N_DAYS:] if N_DAYS > 0 else calc_dates
    calc_cols = [c for c, _ in calc_dates]
    calc_dt = [d for _, d in calc_dates]

    # 解析 rank/pct/val
    parsed = {
        c: pd.DataFrame(
            df[c].apply(parse_triplet).tolist(),
            columns=["rank", "pct", "val"],
            index=df.index
        )
        for c in display_cols
    }

    # 构建 long_df
    records = []
    for c, d in zip(display_cols, display_dt):
        p = parsed[c]
        for i in p.index:
            records.append({
                "date": d,
                "board_name": df.at[i, "board_name"],
                "rank": p.at[i, "rank"],
                "pct": p.at[i, "pct"],
                "val": p.at[i, "val"]
            })

    long_df = pd.DataFrame(records)
    latest_date = display_dt[-1]  # display_dt 已经是排序后的日期列表
    latest_date_str = latest_date.strftime("%m%d")
    return long_df, display_dt, calc_dt, latest_date_str





# ================== 累计涨幅计算 ==================
def compute_cum_pct(long_df, calc_dt, LOOKBACK, top_n):
    pivot = long_df.pivot_table(
        index="board_name",
        columns="date",
        values="pct",
        aggfunc="mean"
    ).fillna(0.0)

    lookback_dt = calc_dt[-LOOKBACK:] if len(calc_dt) >= LOOKBACK else calc_dt
    cum_pct = pivot[lookback_dt].sum(axis=1).sort_values(ascending=False)
    top_boards = cum_pct.head(top_n).index.tolist()
    cum_pct = cum_pct.loc[top_boards]

    return top_boards, cum_pct

# ================== rank 图 ==================
def generate_rank_html(long_df, display_dt, calc_dt, LOOKBACK, file_date_str):
    OUTPUT_HTML = f"html/kcon{LOOKBACK}_{file_date_str}.html"
    top_boards, cum_pct = compute_cum_pct(long_df, calc_dt, LOOKBACK, TOP_N_RANK)
    board_order = top_boards
    df_plot = long_df[long_df["board_name"].isin(top_boards)]

    monday_dt = [d for d in display_dt if d.weekday() == 0]

    # 名次颜色
    MARK_TOP = 15
    palette = qualitative.Set1 + qualitative.Set3 + qualitative.Bold + qualitative.Dark24
    rank_colors = {r: palette[(r-1)%len(palette)] for r in range(1, MARK_TOP+1)}

    fig = go.Figure()

    # Scatter 点
    # Scatter 点
    for r in range(1, MARK_TOP+1):
        sub = df_plot[df_plot["rank"]==r]
        if sub.empty:
            continue
        color = rank_colors[r] if r <= MARK_TOP else "rgba(0,0,0,0)"  # 前10彩色，其余透明
        fig.add_trace(go.Scatter(
            x=sub["date"],
            y=sub["board_name"],
            mode="markers",
            name=f"第{r}名" if r <= MARK_TOP else None,
            marker=dict(size=8, color=color),
            customdata=np.stack([sub["rank"], sub["pct"], sub["val"]], axis=-1),
            hovertemplate=(
                "版块：%{y}<br>"
                "日期：%{x|%Y-%m-%d}<br>"
                "名次：%{customdata[0]}<br>"
                "涨跌幅：%{customdata[1]:.2%}<br>"
                "指数：%{customdata[2]:,.2f}"
                "<extra></extra>"
            ),
            showlegend=r <= MARK_TOP  # 只显示前10名的图例
        ))

    # 处理不在前10的点
    sub_other = df_plot[df_plot["rank"] > MARK_TOP]
    if not sub_other.empty:
        fig.add_trace(go.Scatter(
            x=sub_other["date"],
            y=sub_other["board_name"],
            mode="markers",
            name="其它",
            marker=dict(size=6, color="rgba(0,0,0,0)"),  # 透明点
            customdata=np.stack([sub_other["rank"], sub_other["pct"], sub_other["val"]], axis=-1),
            hovertemplate=(
                "版块：%{y}<br>"
                "日期：%{x|%Y-%m-%d}<br>"
                "名次：%{customdata[0]}<br>"
                "涨跌幅：%{customdata[1]:.2%}<br>"
                "指数：%{customdata[2]:,.2f}"
                "<extra></extra>"
            ),
            showlegend=False
        ))


    for d in display_dt:
        fig.add_vline(x=d, line_width=1, line_dash="dot", opacity=0.25)

    # ---------- 右侧累计涨幅 ----------
    annotations = []
    board_y = {b:i+1 for i,b in enumerate(board_order)}
    for b in board_order:
        annotations.append(dict(
            x=1,
            y=board_y[b]-1,
            xref="paper",
            yref="y",
            text=f"{cum_pct[b]:+.2%}",
            showarrow=False,
            xanchor="left",
            font=dict(size=12, color="rgba(80,80,80,1)")
        ))

    fig.update_layout(
        title=(
            f"最近{LOOKBACK}日累计涨幅前{TOP_N_RANK}个板块 "
            f"（计算截止 {END_DATE or '最新'}）"
        ),
        hovermode="closest",
        xaxis=dict(
            type="date",
            tickmode="array",
            tickvals=monday_dt,
            tickformat="%m-%d",
            showgrid=True,
            gridcolor="rgba(0,0,0,0.15)",
            showspikes=True,
            # autorange="reversed",
        ),
        yaxis=dict(
            type="category",
            categoryorder="array",
            categoryarray=board_order,
            autorange="reversed",
            title=f"按最近{LOOKBACK}日累计涨幅↓",
            showgrid=True,
            gridcolor="rgba(0,0,0,0.15)",
        ),
        legend=dict(x=1.05, y=1.0),
        height=900,
        margin=dict(l=150, r=220, t=90, b=50),
        annotations=annotations
    )

    fig.write_html(OUTPUT_HTML, include_plotlyjs="inline")

       # ---------- 增加一键复制按钮 ----------
    with open(OUTPUT_HTML, "r+", encoding="utf-8") as f:
        html_text = f.read()
        f.seek(0)
        extra_html = f"""
        <div style="position:absolute; top:60px; right:250px; z-index:1000; transform: scale(0.7); transform-origin: top right;">
            <button onclick="copyBoards(5)">前5</button>
            <button onclick="copyBoards(10)">前10</button>
        </div>
        <script>
        function copyBoards(n){{
            const boards = {board_order[:10]!r}.slice(0,n);
            const text = boards.join('\\n');  // 每行一个名字
            navigator.clipboard.writeText(text);
        }}
        </script>
        """
        html_text = html_text.replace("</body>", extra_html + "</body>")
        f.write(html_text)

    print(f"[OK] rank HTML 已生成：{OUTPUT_HTML}")

# ================== range 图 ==================
def generate_range_html(long_df, display_dt, calc_dt, LOOKBACK, file_date_str):
    OUTPUT_HTML = f"html/gcon{LOOKBACK}_{file_date_str}.html"
    top_boards, cum_pct = compute_cum_pct(long_df, calc_dt, LOOKBACK, TOP_N_RANGE)
    df_plot = long_df[long_df["board_name"].isin(top_boards)]
    board_order = top_boards
    board_y = {b:i+1 for i,b in enumerate(board_order)}

    max_abs = np.nanmax(np.abs(df_plot["pct"]))
    scale = ROW_HALF_HEIGHT / max_abs if max_abs>0 else 0
    fig = go.Figure()
    shapes = []

    one_day_ms = 24*3600*1000
    half_bar = BAR_DAY_FRACTION*one_day_ms/2
    x_min, x_max = min(display_dt), max(display_dt)

    hover_x, hover_y, hover_text = [], [], []

    # 红绿柱
    for _, r in df_plot.iterrows():
        if pd.isna(r["pct"]) or r["pct"]==0:
            continue
        y0 = board_y[r["board_name"]]
        y1 = y0 - r["pct"]*scale
        x0 = r["date"] - pd.Timedelta(milliseconds=half_bar)
        x1 = r["date"] + pd.Timedelta(milliseconds=half_bar)
        shapes.append(dict(
            type="rect",
            x0=x0, x1=x1,
            y0=min(y0,y1), y1=max(y0,y1),
            fillcolor="rgba(220,0,0,0.85)" if r["pct"]>0 else "rgba(0,140,0,0.85)",
            line=dict(width=0)
        ))
        hover_x.append(r["date"])
        hover_y.append(y1)
        hover_text.append(
            f"版块：{r['board_name']}<br>"
            f"日期：{r['date'].date()}<br>"
            f"涨跌幅：{r['pct']:+.2%}<br>"
            f"指数：{r['val']:,.2f}"
        )

    # 水平基准线
    for b, y in board_y.items():
        shapes.append(dict(
            type="line",
            x0=x_min, x1=x_max,
            y0=y, y1=y,
            line=dict(color="rgba(0,0,0,0.25)", width=1)
        ))

    fig.update_layout(shapes=shapes)

    # hover
    fig.add_trace(go.Scatter(
        x=hover_x, y=hover_y,
        mode="markers",
        marker=dict(size=1, opacity=0),
        hovertext=hover_text,
        hoverinfo="text",
        showlegend=False
    ))

    # 右侧累计涨幅
    annotations = []
    for b in board_order:
        annotations.append(dict(
            x=1.01,
            y=board_y[b],
            xref="paper",
            yref="y",
            text=f"{cum_pct[b]:+.2%}",
            showarrow=False,
            xanchor="left",
            font=dict(size=12, color="rgba(80,80,80,1)")
        ))

    fig.update_layout(annotations=annotations)

    # 坐标轴
    fig.update_yaxes(
        tickmode="array",
        tickvals=list(board_y.values()),
        ticktext=board_order,
        range=[0.5, len(board_order)+0.5],
        autorange="reversed",   # ★ 排名靠前的在最上
    )

    monday_dt = [d for d in display_dt if d.weekday()==0]
    fig.update_xaxes(
        type="date",
        tickmode="array",
        tickvals=monday_dt,
        tickformat="%m-%d",
        # autorange="reversed",   # ★ 最新在左
    )

    fig.update_layout(
        title=f"最近 {LOOKBACK} 天累计涨幅前 {TOP_N_RANGE} 版块（计算截止：{END_DATE or '最新'}）",
        height=880,
        margin=dict(l=140, r=260, t=90, b=50),
        hovermode="closest"
    )

    fig.write_html(OUTPUT_HTML, include_plotlyjs="inline")
    print(f"[OK] range HTML 已生成：{OUTPUT_HTML}")

# ================== 批量执行 ==================
if __name__ == "__main__":
    long_df, display_dt, calc_dt, latest_date_str = prepare_data()
    for lb in LOOKBACK_LIST:
        generate_rank_html(long_df, display_dt, calc_dt, lb, latest_date_str)
        generate_range_html(long_df, display_dt, calc_dt, lb, latest_date_str)