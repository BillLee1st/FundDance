
crontab -e



# 周一~周五 15:05
5 15 * * 1-5 /bin/bash -lc '/home/dc/homework/fund_dance/round/scripts/run_pipeline.sh' >/dev/null 2>&1

# 周一~周五 15:05
3 15 * * 1-5 /bin/bash -lc '/home/dc/homework/fund_dance/round/scripts/run_pipeline_con.sh' >/dev/null 2>&1

# 周一~周五 16:05
5 16 * * 1-5 /bin/bash -lc '/home/dc/homework/fund_dance/round/scripts/run_pipeline_ind.sh' >/dev/null 2>&1
