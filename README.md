
板块轮动数据
所有数据在round目录下

网页存储位置 round/html  按日期排序 找到最新文件

图表类型1 Top数据 ++++++++++++++++++++++++++++++++++++
kcon1_0128
kcon5_0128
gcon1_0128
gcon5_0128

kind1_0128
kind5_0128
gind1_0128
gind5_0128

con为概念板块 
ind为行业版快

k为topN的排名点云分布图
g为topN的涨跌数据柱状图

1为当天的数据
5为近5天的累计数据

数据源
data_concept.csv
data_industry.csv





图标类型2 全部排名数据  ++++++++++++++++++++++++++++++++++++++

数据源
bk_day.xlsx

bk_day_industry_rank.html
bk_day_concept_rank.html

rank	01-28	   info		
1	    黄金概念	7.51 / 5.78 / 66 / 4 / 湖南黄金

板块数据格式                  
涨跌幅 / 换手率 / 上涨个数 / 下跌个数 / 领涨股票
7.51   / 5.78  / 66      / 4         / 湖南黄金

数据源
bk_day.xlsx





图标类型3 全部板块排名折线图  +++++++++++++++++++++++++++++++

vcon_all.html
vind_all.html

实线和点为板块净值及走势
虚线和点为板块排名顺序及涨跌幅标注





数据更新方法  ++++++++++++++++++++++++++++++++++++++

注 东方财富数据接口每小时限制调用1次 优先更新概念板块

#进入目录
cd round

#执行对应脚本           

#只更新概念板块数据

command_con.sh

#只更新行业板块数据

command_ind.sh

#只更新bk_day图表excel数据

command_day.sh

#更新所有数据

command.sh   

