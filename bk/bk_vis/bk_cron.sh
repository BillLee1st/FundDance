
@echo off
REM === 自动更新行业板块排名 CSV（批量抓“今天”） ===
REM 使用 conda 环境 fund311，日志同时输出到控制台和文件

set BASE_DIR=D:\fund_dance\bk\rank_vis
set LOG_DIR=%BASE_DIR%\logs
set LOG_FILE=%LOG_DIR%\bk_a_get_change_value_local_rank.log
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

REM 激活 conda 环境
call "%UserProfile%\anaconda3\Scripts\activate.bat" fund311

cd /d "%BASE_DIR%" || exit /b 1

echo [%date% %time%] Running bk_a_get_change_value_local_rank (today column by clist/get)... | tee -a "%LOG_FILE%"

python bk_a_get_change_value_local_rank.py %* 2>&1 | tee -a "%LOG_FILE%"

if %errorlevel%==0 (
  echo [%date% %time%] Done. (exit=%errorlevel%) | tee -a "%LOG_FILE%"
) else (
  echo [%date% %time%] Failed. (exit=%errorlevel%) | tee -a "%LOG_FILE%"
)

exit /b 0