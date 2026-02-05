import pandas as pd
import os
from shutil import copyfile

CSV_PATH = "data_concept.csv"
BACKUP_PATH = "data_concept_bk.csv"
# CSV_PATH = "data_industry.csv"
# BACKUP_PATH = "data_industry_bk.csv"
KEEP_DAYS = 90   # 保留最近90个交易日


def main():
    if not os.path.exists(CSV_PATH):
        print("❌ data_concept.csv 不存在")
        return

    # ========= 1️⃣ 备份 =========
    print("🔹 备份原始文件...")
    copyfile(CSV_PATH, BACKUP_PATH)
    print(f"✅ 已备份为 {BACKUP_PATH}")

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
