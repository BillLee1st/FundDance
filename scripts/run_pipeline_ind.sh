#!/bin/bash
# === 自动更新行业板块排名 CSV（批量抓“今天”） ===
# 使用 conda 环境 fund311，日志同时输出到控制台和文件（tee）


LOG_DIR="/home/dc/homework/fund_dance/round/logs"
LOG_FILE="$LOG_DIR/pipeline.log"     # 与你的示例一致的保存方式（目录+tee），文件名可自定义
mkdir -p "$LOG_DIR"

# 激活 Anaconda 环境（保持与示例一致）
source /home/dc/anaconda3/etc/profile.d/conda.sh
conda activate fund311

# 进入代码目录
cd /home/dc/homework/fund_dance/round || exit 1

# ====== 日志输出函数（与示例相同的写法）======
log() {
  echo "[$(date '+%F %T')] $1" | tee -a "$LOG_FILE"
}

# ====== 执行（参数可按需追加；脚本也支持透传 "$@"）======
log " +++++++++++++++++++   ind start  +++++++++++++++++++++"
log "Running industry pipeline (today column by clist/get)..."
python bk_data_ind.py "$@" 2>&1 | tee -a "$LOG_FILE"
python bk_top_ind.py "$@" 2>&1 | tee -a "$LOG_FILE"
log " +++++++++++++++++++   ind end  +++++++++++++++++++++"


RET=${PIPESTATUS[0]}
if [[ $RET -eq 0 ]]; then
  log "Done. (exit=$RET)"
else
  log "Failed. (exit=$RET)"
fi
