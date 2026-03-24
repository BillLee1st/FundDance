#在数据过多时 用以截取最近N日数据
# python bk_ahead.py

from datetime import datetime
import shutil
import pandas as pd
import os
from shutil import copyfile

# CSV_PATH = "data/data_concept.csv"
CSV_PATH = "data/data_industry.csv"
KEEP_DAYS = 90   # 保留最近90个交易日



def backup_csv():
        # ==========================
    # 1️⃣ 备份原文件
    # ==========================
    timestamp = datetime.now().strftime("%Y%m%d")
    dir_name = os.path.dirname(CSV_PATH)
    base_name = os.path.basename(CSV_PATH)
    name_no_ext, ext = os.path.splitext(base_name)

    backup_file = os.path.join(
        dir_name,
        f"{name_no_ext}_{timestamp}{ext}"
    )

    shutil.copy2(CSV_PATH, backup_file)
    print(f"已备份原文件 → {backup_file}")


def main():
    if not os.path.exists(CSV_PATH):
        print("❌ data_concept.csv 不存在")
        return

    # ========= 1️⃣ 备份 =========
    # print("🔹 备份原始文件...")
    # copyfile(CSV_PATH, BACKUP_PATH)
    # print(f"✅ 已备份为 {BACKUP_PATH}")
    backup_csv()

    # ========= 2️⃣ 读取 =========
    print("🔹 读取CSV...")
    df = pd.read_csv(CSV_PATH, index_col=0)

    # 所有日期列（排除 row_key）
    date_cols = list(df.columns)

    if len(date_cols) <= KEEP_DAYS:
        print("⚠️ 当前交易日数量不足90天，无需裁剪")
        return

    # ========= 3️⃣ 取最近90列 =========
    last_cols = date_cols[-KEEP_DAYS:]
    df_new = df[last_cols]

    print(f"🔹 原列数: {len(date_cols)}")
    print(f"🔹 保留列数: {len(last_cols)}")
    print(f"🔹 最早保留日期: {last_cols[0]}")
    print(f"🔹 最新日期: {last_cols[-1]}")

    # ========= 4️⃣ 覆盖保存 =========
    df_new.to_csv(CSV_PATH, encoding="utf-8-sig")

    print("✅ CSV已瘦身完成，只保留最近90个交易日数据")


if __name__ == "__main__":
    main()