# python bk_ahead_merge.py



import pandas as pd
from datetime import datetime
import shutil
import os


def merge_only_duplicate_boards(csv_path):
    """
    功能：
    1. 先备份原始 CSV
    2. 仅合并 code 出现多行的板块
    3. 生成同名 CSV（覆盖原文件）
    4. 输出关键日志
    """

    print("========== 开始板块合并 ==========")

    # ==========================
    # 1️⃣ 备份原文件
    # ==========================
    timestamp = datetime.now().strftime("%Y%m%d")
    dir_name = os.path.dirname(csv_path)
    base_name = os.path.basename(csv_path)
    name_no_ext, ext = os.path.splitext(base_name)

    backup_file = os.path.join(
        dir_name,
        f"{name_no_ext}_{timestamp}.bak{ext}"
    )

    shutil.copy2(csv_path, backup_file)
    print(f"已备份原文件 → {backup_file}")

    # ==========================
    # 2️⃣ 读取 CSV
    # ==========================
    df = pd.read_csv(csv_path, dtype=str)

    rowkey_col = df.columns[0]
    date_cols = df.columns[1:]

    df[['code', 'name']] = df[rowkey_col].str.split('|', expand=True)

    code_counts = df['code'].value_counts()
    duplicate_codes = code_counts[code_counts > 1].index

    print(f"检测到重复板块数量: {len(duplicate_codes)}")

    df_dup = df[df['code'].isin(duplicate_codes)].copy()
    df_single = df[~df['code'].isin(duplicate_codes)].copy()

    if df_dup.empty:
        print("没有需要合并的板块")
        print("恢复原文件（无修改）")
        return

    def parse_date(col):
        return datetime.strptime(col, "%Y-%m-%d")

    sorted_dates = sorted(date_cols, key=parse_date)

    merged_rows = []

    # ==========================
    # 3️⃣ 合并重复板块
    # ==========================
    for code, group in df_dup.groupby("code"):

        print("\n----------------------------------")
        print(f"开始合并板块: {code}")
        print(f"原始行数: {len(group)}")

        original_names = group["name"].unique().tolist()
        print(f"原名称列表: {original_names}")

        new_row = group.iloc[0].copy()
        merged_dates = []

        for col in sorted_dates:
            values = group[col].dropna()
            values = values[values != ""]

            if len(values) > 0:
                new_row[col] = values.iloc[-1]
                merged_dates.append(col)
            else:
                new_row[col] = ""

        latest_name = None
        latest_date_used = None

        for col in reversed(sorted_dates):
            sub = group[group[col].notna() & (group[col] != "")]
            if not sub.empty:
                latest_name = sub.iloc[-1]["name"]
                latest_date_used = col
                break

        if latest_name is None:
            latest_name = group.iloc[-1]["name"]

        new_row[rowkey_col] = f"{code}|{latest_name}"

        print(f"最终名称: {latest_name}")
        print(f"名称来源日期: {latest_date_used}")
        print(f"有数据日期数: {len(merged_dates)}")

        merged_rows.append(new_row)

    # ==========================
    # 4️⃣ 生成最终结果
    # ==========================
    merged_df = pd.DataFrame(merged_rows)
    merged_df = merged_df.drop(columns=["code", "name"])
    df_single = df_single.drop(columns=["code", "name"])

    final_df = pd.concat([df_single, merged_df], ignore_index=True)

    # ==========================
    # 5️⃣ 覆盖写回原文件
    # ==========================
    final_df.to_csv(csv_path, index=False)

    print("\n========== 合并完成 ==========")
    print(f"已覆盖原文件: {csv_path}")




    # 使用示例
if __name__ == "__main__":
    # 输入文件路径
    # input_file = "data/data_industry.csv"  # 替换为你的CSV文件路径
    input_file = "data/data_concept.csv"  # 替换为你的CSV文件路径

    merge_only_duplicate_boards(input_file)
