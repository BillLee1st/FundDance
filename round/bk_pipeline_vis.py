#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
双轴 Plotly HTML（图例按最新一天涨跌幅从大到小排序 + 精简控件）：
- 上图：左轴=排名（dot虚线，倒序，并在点附近标注当日涨跌幅），右轴=收盘净值（实线+圆点）
- 下图：涨跌幅柱（涨=红、跌=绿、平=灰），高度≈上图的1/10
- X轴：仅每周一打刻度与竖线；不显示“交易日”标题
- 右侧图例：按“最新一天的涨跌幅”从大到小（NaN 置后）
- 顶部按钮：当前 / 模式(单/多) / 上一个 / 下一个（按钮栏整体不变，仅按钮适度放大）
- 悬浮提示：合并为一条（仅在“排名”折线上显示）
- 上一个/下一个：按“最新一天排名”从小到大切换（NaN 置后）
"""

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

# ========== 基本配置 ==========
INPUT  = "data_daily.csv"
OUTPUT = "html/vis_all.html"
TITLE  = "A股板块 · 排名 vs 净值（双轴）"
YL     = "排名（越小越靠前）"
YR     = "收盘净值"
YB     = "涨跌幅（%）"

# 仅显示最近多少个“交易日列”（默认 90 天）
SHOW_LAST_DAYS = 90

# ---------- 读取并解析 ----------
df_raw = pd.read_csv(INPUT, index_col=0)

def parse_triple(cell):
    if not isinstance(cell, str):
        return (None, None, None)
    p = cell.split("|")
    if len(p) != 3:
        return (None, None, None)
    try:
        r = int(p[0])
    except:
        r = None
    try:
        c = float(p[1])
    except:
        c = None
    try:
        v = float(p[2])
    except:
        v = None
    return (r, c, v)

rank_df  = pd.DataFrame(index=df_raw.index, columns=df_raw.columns, dtype="float")
chg_df   = pd.DataFrame(index=df_raw.index, columns=df_raw.columns, dtype="float")
close_df = pd.DataFrame(index=df_raw.index, columns=df_raw.columns, dtype="float")

for idx in df_raw.index:
    for col in df_raw.columns:
        r, c, v = parse_triple(df_raw.loc[idx, col])
        rank_df.loc[idx, col]  = r
        chg_df.loc[idx, col]   = c
        close_df.loc[idx, col] = v

# 索引“BKxxxx|中文名” -> 中文名
boards = [rk.split("|")[1] if "|" in rk else rk for rk in df_raw.index]
rank_df.index = chg_df.index = close_df.index = boards

# ---------- 仅保留最近 SHOW_LAST_DAYS 天 ----------
def _last_n_cols_by_date(cols, n):
    s = pd.to_datetime(pd.Index(cols), errors="coerce")
    order = pd.Series(cols, index=s).sort_index()
    valid = order.index.notna()
    cols_sorted = order[valid].tolist()
    if n is None or n <= 0 or n >= len(cols_sorted):
        return cols_sorted
    return cols_sorted[-n:]

sel_cols = _last_n_cols_by_date(chg_df.columns, SHOW_LAST_DAYS)
rank_df  = rank_df.loc[:, sel_cols]
chg_df   = chg_df.loc[:, sel_cols]
close_df = close_df.loc[:, sel_cols]

# 宽转长
rank_long  = rank_df.T.reset_index().melt(id_vars="index", var_name="板块", value_name="排名").rename(columns={"index": "日期"})
chg_long   = chg_df.T.reset_index().melt(id_vars="index", var_name="板块", value_name="涨跌幅").rename(columns={"index": "日期"})
close_long = close_df.T.reset_index().melt(id_vars="index", var_name="板块", value_name="收盘净值").rename(columns={"index": "日期"})
plot_df    = rank_long.merge(chg_long, on=["日期", "板块"]).merge(close_long, on=["日期", "板块"])
plot_df["日期"] = pd.to_datetime(plot_df["日期"], errors="coerce")

# ---------- 排序（图例用）：按“窗口内最新一天”的涨跌幅从大到小（NaN 置后） ----------
latest_date = chg_df.columns[-1] if len(chg_df.columns) > 0 else None
if latest_date is None:
    order = sorted(boards)
    sort_hint = ""
else:
    s = chg_df[latest_date].copy()
    order = list(s.sort_values(ascending=False, na_position="last").index)
    sort_hint = f" · 默认按 最新一天（{latest_date}）涨跌幅降序 · 显示近 {len(sel_cols)} 天"

# ---------- 导航顺序（按钮用）：按“最新一天”的排名从小到大（NaN 置后） ----------
if latest_date is not None and latest_date in rank_df.columns:
    _ranks_today = rank_df[latest_date]
    nav_order = list(_ranks_today.sort_values(ascending=True, na_position="last").index)
else:
    nav_order = order[:]  # 兜底

# ---------- 调色（沿用涨跌幅序的 order，以保证与图例顺序一致的配色） ----------
palette = px.colors.qualitative.Vivid + px.colors.qualitative.Set2 + px.colors.qualitative.Dark24
if len(palette) < len(order):
    times = (len(order) + len(palette) - 1) // len(palette)
    palette = (palette * times)[:len(order)]
color_map = {b: palette[i] for i, b in enumerate(order)}

# ---------- 子图 ----------
fig = make_subplots(
    rows=2, cols=1, shared_xaxes=True,
    vertical_spacing=0.012,
    row_heights=[0.9, 0.1],
    specs=[[{"secondary_y": True}], [{}]]
)

# ---------- 绘图 ----------
for board in order:
    d = plot_df[plot_df["板块"] == board].sort_values("日期")
    col = color_map[board]

    # 合并 hover 的 customdata: [收盘净值, 涨跌幅]
    combined_custom = list(zip(d["收盘净值"], d["涨跌幅"]))

    # 文本：恢复涨跌幅标注（尽量避开线）
    texts = [f"{v:+.2f}%" if pd.notna(v) else "" for v in d["涨跌幅"]]
    textpos = []
    for v in d["涨跌幅"]:
        if pd.isna(v):
            textpos.append("top center")
        elif v > 0:
            textpos.append("top center")      # 涨：点上方
        elif v < 0:
            textpos.append("bottom center")   # 跌：点下方
        else:
            textpos.append("middle right")    # 平：点右侧

    # 1) 排名：dot 虚线 + 圆点 + 文本（左轴；进入图例且承载 hover）
    fig.add_trace(go.Scatter(
        x=d["日期"], y=d["排名"],
        mode="lines+markers+text",
        name=board, legendgroup=board, showlegend=True,
        line=dict(width=1.5, dash="dot", color=col),
        marker=dict(size=6, color=col, line=dict(width=0), symbol="circle"),
        text=texts,
        textposition=textpos,
        textfont=dict(size=10, color=col),
        cliponaxis=False,
        customdata=combined_custom,
        hovertemplate=(
            f"<b>{board}</b><br>"
            "日期=%{x|%Y-%m-%d}<br>"
            "排名=%{y}<br>"
            "收盘净值=%{customdata[0]:.2f}<br>"
            "涨跌幅=%{customdata[1]:+.2f}%<extra></extra>"
        ),
        visible="legendonly"
    ), row=1, col=1, secondary_y=False)

    # 2) 净值：实线 + 圆点（右轴；关闭独立 hover）
    fig.add_trace(go.Scatter(
        x=d["日期"], y=d["收盘净值"],
        mode="lines+markers",
        name=board, legendgroup=board, showlegend=False,
        line=dict(width=2, color=col),
        marker=dict(size=5, color=col),
        hoverinfo="skip",
        visible="legendonly"
    ), row=1, col=1, secondary_y=True)

    # 3) 涨跌幅柱（关闭 hover）
    bar_colors = [
        ("#d62728" if (pd.notna(v) and v > 0) else "#2ca02c" if (pd.notna(v) and v < 0) else "#9e9e9e")
        for v in d["涨跌幅"].values
    ]
    fig.add_trace(go.Bar(
        x=d["日期"], y=d["涨跌幅"],
        name=board, legendgroup=board, showlegend=False,
        marker=dict(color=bar_colors, line=dict(width=0)),
        opacity=0.65,
        hoverinfo="skip",
        visible="legendonly"
    ), row=2, col=1)

# 周一刻度（基于窗口内日期）
all_dates = pd.to_datetime(plot_df["日期"].dropna().unique())
mondays = pd.DatetimeIndex(all_dates).sort_values()
mondays = mondays[mondays.weekday == 0]

# ---------- 布局 ----------
fig.update_layout(
    title=f"{TITLE}{sort_hint}",
    template="plotly_white",
    hovermode="x unified",
    autosize=True,
    font=dict(size=13),
    margin=dict(l=56, r=20, t=50, b=28),
    legend_title="板块",
    legend=dict(
        traceorder="normal",
        orientation="v",
        x=1.005, xanchor="left",
        y=1.0,  yanchor="top",
        itemsizing="constant",
        itemwidth=30,
        tracegroupgap=0
    ),
    barmode="group"
)

fig.update_yaxes(title_text=YL, autorange="reversed",
                 showgrid=True, gridcolor="rgba(220,220,220,0.40)",
                 row=1, col=1, secondary_y=False)
fig.update_yaxes(title_text=YR, showgrid=False,
                 row=1, col=1, secondary_y=True)
fig.update_yaxes(title_text=YB, ticksuffix="%",
                 showgrid=True, gridcolor="rgba(220,220,220,0.35)",
                 zeroline=True, zerolinecolor="rgba(120,120,120,0.6)",
                 row=2, col=1)

for r in (1, 2):
    fig.update_xaxes(
        title_text="",
        tickmode="array",
        tickvals=mondays,
        ticktext=[d.strftime("%b %d") for d in mondays],
        showgrid=True,
        gridwidth=0.8,
        gridcolor="rgba(200,200,200,0.35)",
        row=r, col=1
    )

# ---------- 导出 & 顶部控件 ----------
html = fig.to_html(include_plotlyjs="inline", full_html=True)

# 导航顺序按排名（nav_order），而图例顺序仍按涨跌幅（order）
board_js = "[" + ",".join([f"'{b}'" for b in nav_order]) + "]"

controls = f"""
<style>
html,body {{ height:100%; margin:0; }}
.js-plotly-plot {{ height:96vh !important; }}
#ctrl-btns {{
  position: fixed; top: 8px; right: 8px; z-index: 1000;
  background: rgba(255,255,255,0.78); border-radius: 10px;
  padding: 10px 12px; box-shadow: 0 6px 24px rgba(0,0,0,0.12), 0 1px 6px rgba(0,0,0,0.08);
  backdrop-filter: blur(6px);
  transform: scale(0.8);              /* 保持栏整体大小不变 */
  transform-origin: top right;
  font-family: -apple-system, Segoe UI, Roboto, Helvetica, Arial, "Noto Sans", "PingFang SC", "Microsoft YaHei", sans-serif;
}}
#ctrl-btns button {{
  font-size: 13px;                    /* 适度增大字体 */
  margin: 3px 4px;
  padding: 7px 13px;                  /* 适度增大点击面积 */
  border: 1px solid #c7c7c7; border-radius: 9px; background: #fff; cursor: pointer;
}}
#ctrl-btns button:hover {{ background: #f1f5fb; }}
#ctrl-btns .primary {{ background:#1976d2; color:#fff; border-color:#1976d2; }}
#ctrl-btns .primary:hover {{ background:#0d47a1; }}
#ctrl-btns label, #ctrl-btns span {{ font-size:13px; }}
#current-name {{ font-weight:600; }}
</style>

<div id="ctrl-btns">
  <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;">
    <label>当前：<span id="current-name">（未选）</span></label>
    <button id="btnMode" class="primary">模式：单选</button>
    <button id="btnPrev">上一个</button>
    <button id="btnNext">下一个</button>
  </div>
</div>

<script>
document.addEventListener('DOMContentLoaded', function() {{
  const gd = document.querySelector('.js-plotly-plot');
  const boards = {board_js}; // 按“今日排名升序”的顺序

  function groupIdx(name) {{
    const idx=[]; for (let i=0;i<gd.data.length;i++) if (gd.data[i].legendgroup===name) idx.push(i);
    return idx;
  }}
  function hideAll() {{ Plotly.restyle(gd,'visible', new Array(gd.data.length).fill('legendonly')); }}

  let current=-1;
  async function showBoard(i) {{
    current = (i+boards.length)%boards.length;
    hideAll();
    const name = boards[current];
    const idxs = groupIdx(name);
    const vis = new Array(gd.data.length).fill('legendonly'); idxs.forEach(k=>vis[k]=true);
    await Plotly.restyle(gd,'visible',vis);
    document.getElementById('current-name').textContent=name;
  }}

  document.getElementById('btnPrev').onclick = ()=>{{ if(current===-1) current=0; showBoard(current-1); }};
  document.getElementById('btnNext').onclick = ()=>{{ if(current===-1) current=-1; showBoard(current+1); }};

  let singleMode = true;
  const btnMode = document.getElementById('btnMode');
  btnMode.onclick = ()=>{{ singleMode=!singleMode; btnMode.textContent='模式：'+(singleMode?'单选':'多选'); }};

  gd.on('plotly_legendclick', function(ev){{
    try {{
      const tr = gd.data[ev.curveNumber];
      const name = tr.legendgroup || tr.name;
      if (!singleMode) return true; // 多选：默认行为
      const idxs = groupIdx(name);
      const vis = new Array(gd.data.length).fill('legendonly'); idxs.forEach(k=>vis[k]=true);
      Plotly.restyle(gd,'visible',vis);
      document.getElementById('current-name').textContent=name;

      // 同步 current 为该 name 在 boards（按排名序）中的位置，便于继续按排名序 Prev/Next
      const pos = boards.indexOf(name);
      if (pos >= 0) current = pos;

      return false;
    }} catch(e) {{ return false; }}
  }});

  hideAll();
}});
</script>
"""

html = html.replace("</body>", controls + "\n</body>")
with open(OUTPUT, "w", encoding="utf-8") as f:
    f.write(html)

print(f"已生成交互网页：{OUTPUT}（近 {len(sel_cols)} 天；按钮按当日排名升序切换）")