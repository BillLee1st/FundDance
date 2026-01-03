#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
A股板块 涨跌幅趋势图 (Plotly HTML)
- 横坐标：日期
- 纵坐标：涨跌幅 (%)
- 每个板块一条曲线
- 悬停框包含：日期 / 板块 / 涨跌幅 / 排名
- 默认所有板块不选中（点击图例显示）
- 页面右上角新增：选中所有 / 清除所有 按钮（悬浮样式）
"""

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

# ============================================================
# 1️⃣ 读取 CSV
# ============================================================
df = pd.read_csv("board_last30days_rank_pct.csv", index_col=0)

# ============================================================
# 2️⃣ 解析数据列 “排名|涨跌幅”
# ============================================================
def parse_rank(cell):
    if isinstance(cell, str) and "|" in cell:
        try:
            return int(cell.split("|")[0])
        except ValueError:
            return None
    return None

def parse_chg(cell):
    if isinstance(cell, str) and "|" in cell:
        try:
            return float(cell.split("|")[1])
        except ValueError:
            return None
    return None

# ✅ 使用新版兼容写法，避免 FutureWarning
rank_df = df.apply(lambda col: col.map(parse_rank))
chg_df = df.apply(lambda col: col.map(parse_chg))

# ============================================================
# 3️⃣ 提取板块中文名
# ============================================================
boards = [x.split("|")[1] if "|" in x else x for x in df.index]
rank_df.index = boards
chg_df.index = boards

# ============================================================
# 4️⃣ 转换为可绘制长表
# ============================================================
rank_melt = rank_df.T.reset_index().melt(
    id_vars="index", var_name="板块", value_name="排名"
)
chg_melt = chg_df.T.reset_index().melt(
    id_vars="index", var_name="板块", value_name="涨跌幅"
)
plot_df = pd.merge(rank_melt, chg_melt, on=["index", "板块"])
plot_df.rename(columns={"index": "日期"}, inplace=True)

# 日期转换为时间序列，确保横轴按时间排序
plot_df["日期"] = pd.to_datetime(plot_df["日期"])

# ============================================================
# 5️⃣ 绘制折线图 (Plotly)
# ============================================================
fig = go.Figure()
colors = px.colors.qualitative.Vivid

for i, board in enumerate(plot_df["板块"].unique()):
    df_b = plot_df[plot_df["板块"] == board]

    fig.add_trace(go.Scatter(
        x=df_b["日期"],
        y=df_b["涨跌幅"],
        mode="lines+markers+text",
        name=board,
        text=[f"{chg:+.2f}%  (#{int(rank)})" if pd.notna(chg) and pd.notna(rank) else ""
              for chg, rank in zip(df_b["涨跌幅"], df_b["排名"])],
        textposition="top center",
        textfont=dict(size=9),
        hovertemplate=(
            f"<b>{board}</b><br>"
            "日期=%{x|%Y-%m-%d}<br>"
            "涨跌幅=%{y:+.2f}%<br>"
            "排名=%{text}<extra></extra>"
        ),
        visible="legendonly",  # 默认隐藏
        line=dict(color=colors[i % len(colors)], width=2)
    ))

# ============================================================
# 6️⃣ 美化布局
# ============================================================
fig.update_xaxes(title="交易日期")
fig.update_yaxes(title="涨跌幅 (%)")
fig.update_layout(
    title="A股板块涨跌幅趋势图（含排名信息）",
    template="plotly_white",
    hovermode="x unified",
    legend_title="板块",
    font=dict(size=13)
)

# ============================================================
# 7️⃣ 导出 HTML + 插入右上角按钮 JS
# ============================================================
output_html = "rank_board_chg_trend_with_buttons.html"
html_content = fig.to_html(include_plotlyjs="cdn", full_html=True)

# 🔧 注入右上角按钮控制 JS（浮动样式）
js_controls = """
<style>
#control-buttons {
    position: fixed;
    top: 12px;
    right: 12px;
    z-index: 1000;
    background-color: rgba(255,255,255,0.8);
    border-radius: 6px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.2);
    padding: 4px 8px;
    font-family: "Microsoft YaHei", sans-serif;
    font-size: 12px;
}
#control-buttons button {
    margin: 2px;
    padding: 2px 6px;
    border: none;
    border-radius: 4px;
    background-color: #1976d2;
    color: white;
    cursor: pointer;
}
#control-buttons button:hover {
    background-color: #0d47a1;
}
</style>

<div id="control-buttons">
  <button id="btnAll">✅ 全选</button>
  <button id="btnNone">🚫 清空</button>
</div>

<script>
document.addEventListener('DOMContentLoaded', function() {
    const gd = document.querySelector('.js-plotly-plot');
    const btnAll = document.getElementById('btnAll');
    const btnNone = document.getElementById('btnNone');

    btnAll.onclick = function() {
        Plotly.restyle(gd, {visible: true});
    };
    btnNone.onclick = function() {
        Plotly.restyle(gd, {visible: 'legendonly'});
    };
});
</script>
"""

# 插入到 HTML 结束 body 前
html_content = html_content.replace("</body>", js_controls + "\n</body>")

with open(output_html, "w", encoding="utf-8") as f:
    f.write(html_content)

print(f"✅ 已生成交互网页：{output_html}")
print("💡 打开后，右上角有『全选 / 清空』按钮（悬浮、不遮挡主图）。")