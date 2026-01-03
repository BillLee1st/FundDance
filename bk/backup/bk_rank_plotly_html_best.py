#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Plotly HTML 可交互折线图：
- 默认不选中任何板块；
- 每个数据点显示涨跌幅文字；
- 悬停框也显示详细数据；
- 页面右上角新增极小『全选 / 清空』按钮（不遮挡主图）
"""

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

# 1️⃣ 读取 CSV
df = pd.read_csv("board_last30days_rank_pct.csv", index_col=0)

# 2️⃣ 解析“排名|涨跌幅”
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

rank_df = df.apply(lambda col: col.map(parse_rank))
chg_df = df.apply(lambda col: col.map(parse_chg))

# 提取板块中文名
boards = [x.split("|")[1] if "|" in x else x for x in df.index]
rank_df.index = boards
chg_df.index = boards

# 3️⃣ 构造展开 DataFrame
plot_df = rank_df.T.reset_index().melt(
    id_vars="index", var_name="板块", value_name="排名"
)
plot_df.rename(columns={"index": "日期"}, inplace=True)

chg_melt = chg_df.T.reset_index().melt(
    id_vars="index", var_name="板块", value_name="涨跌幅"
)
chg_melt.rename(columns={"index": "日期"}, inplace=True)

plot_df = plot_df.merge(chg_melt, on=["日期", "板块"], how="left")

# 4️⃣ 绘制折线 + 点 + 文本
fig = go.Figure()
colors = px.colors.qualitative.Vivid

for board in plot_df["板块"].unique():
    df_b = plot_df[plot_df["板块"] == board]
    fig.add_trace(go.Scatter(
        x=df_b["日期"],
        y=df_b["排名"],
        mode="lines+markers+text",
        name=board,
        text=[f"{chg:+.2f}%" if pd.notna(chg) else "" for chg in df_b["涨跌幅"]],
        textposition="top center",
        textfont=dict(size=10),
        hovertemplate=(
            f"<b>{board}</b><br>日期=%{{x}}<br>"
            "排名=%{y}<br>涨跌幅=%{text}"
        ),
        visible="legendonly",
        line=dict(width=2)
    ))

# 5️⃣ 坐标轴 & 样式
fig.update_yaxes(autorange="reversed", title="排名（越小越靠前）")
fig.update_xaxes(title="交易日")
fig.update_layout(
    title="A股板块排名趋势（每点显示涨跌幅）",
    hovermode="x unified",
    template="plotly_white",
    legend_title="板块",
    font=dict(size=13)
)

# 6️⃣ 导出 HTML + 注入按钮控件
output_html = "rank_board_trend_with_text_buttons.html"
html_content = fig.to_html(include_plotlyjs="cdn", full_html=True)

# 🔧 极小右上角按钮
js_controls = """
<style>
#ctrl-btns {
  position: fixed;
  top: 8px;
  right: 8px;
  z-index: 1000;
  background-color: rgba(255,255,255,0.6);
  border-radius: 4px;
  padding: 2px 4px;
  box-shadow: 0 1px 3px rgba(0,0,0,0.15);
  font-family: sans-serif;
}
#ctrl-btns button {
  font-size: 10px;
  margin: 1px;
  padding: 1px 4px;
  border: none;
  border-radius: 3px;
  background-color: #1976d2;
  color: white;
  cursor: pointer;
}
#ctrl-btns button:hover { background-color: #0d47a1; }
</style>

<div id="ctrl-btns">
  <button id="btnAll">全选</button>
  <button id="btnNone">清空</button>
</div>

<script>
document.addEventListener('DOMContentLoaded', function() {
  const gd = document.querySelector('.js-plotly-plot');
  document.getElementById('btnAll').onclick = () => Plotly.restyle(gd, {visible: true});
  document.getElementById('btnNone').onclick = () => Plotly.restyle(gd, {visible: 'legendonly'});
});
</script>
"""

html_content = html_content.replace("</body>", js_controls + "\n</body>")

with open(output_html, "w", encoding="utf-8") as f:
    f.write(html_content)

print(f"✅ 已生成交互网页：{output_html}")
print("💡 打开后右上角有极小『全选 / 清空』按钮，不遮挡主图。")