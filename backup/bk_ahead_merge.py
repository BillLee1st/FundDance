# python bk_ahead_merge.py

import pandas as pd
import os
from datetime import datetime



def merge_duplicate_rows_by_code(input_csv, output_csv, backup=True):
    """
    根据板块代码合并重复行，处理板块名字变动的情况

    参数:
    input_csv: 输入CSV文件路径
    output_csv: 输出CSV文件路径
    backup: 是否备份原始数据，默认为True
    """

    # 1. 备份原始数据
    if backup:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = f"{input_csv}.backup_{timestamp}"
        # 读取原始数据并备份
        original_df = pd.read_csv(input_csv)
        original_df.to_csv(backup_file, index=False, encoding='utf-8-sig')
        print(f"原始数据已备份到: {backup_file}")

    # 2. 读取数据
    df = pd.read_csv(input_csv)
    print(f"原始数据行数: {len(df)}")
    print(f"原始数据列: {list(df.columns)}")

    # 3. 检查是否存在code列
    if 'code' not in df.columns:
        print("错误：CSV文件中没有找到'code'列")
        print("当前列:", list(df.columns))
        return

    # 4. 按code分组
    grouped = df.groupby('code')

    # 5. 准备合并后的数据
    merged_rows = []

    for code, group in grouped:
        print(f"\n处理板块: {code}")
        print(f"  原始行数: {len(group)}")

        if len(group) == 1:
            # 只有一行，直接转换为字典并保留
            row_dict = group.iloc[0].to_dict()
            merged_rows.append(row_dict)
            print(f"  保留单行数据")
        else:
            # 多行数据，需要合并
            print(f"  发现 {len(group)} 行数据，开始合并...")

            # 5.1 确定板块名字
            # 规则：优先选择有数据的行中的名字，如果都有数据，选择最新的（假设行按时间顺序）
            name_candidates = []
            has_data_rows = []

            for idx, row in group.iterrows():
                # 检查该行是否有日期数据（非空的日期列）
                date_cols = [col for col in df.columns if col not in ['code', 'name']]
                has_data = False
                for col in date_cols:
                    if pd.notna(row[col]) and str(row[col]).strip() != "":
                        has_data = True
                        break

                if has_data:
                    has_data_rows.append(idx)
                    if pd.notna(row['name']) and str(row['name']).strip() != "":
                        name_candidates.append(row['name'])

            # 选择板块名字
            if name_candidates:
                # 如果有多个候选，选择最后一个（假设最新的）
                selected_name = name_candidates[-1]
                print(f"  选择板块名字: {selected_name} (从 {len(name_candidates)} 个候选中)")
            else:
                # 如果都没有数据，选择第一个非空的名字
                non_empty_names = [row['name'] for _, row in group.iterrows()
                                  if pd.notna(row['name']) and str(row['name']).strip() != ""]
                selected_name = non_empty_names[0] if non_empty_names else ""
                print(f"  选择板块名字: {selected_name} (无数据行，使用第一个非空名字)")

            # 5.2 合并数据
            # 创建新字典，code和name使用合并后的值
            merged_row = {'code': code, 'name': selected_name}

            # 合并所有其他列（包括日期列和其他可能的列）
            other_cols = [col for col in df.columns if col not in ['code', 'name']]
            for col in other_cols:
                # 收集该列的所有非空值
                values = []
                for idx, row in group.iterrows():
                    if pd.notna(row[col]) and str(row[col]).strip() != "":
                        values.append(str(row[col]).strip())

                # 如果有多个值，选择最后一个（假设最新的）
                if values:
                    merged_row[col] = values[-1]
                    if len(values) > 1:
                        print(f"  列 {col} 有 {len(values)} 个值，选择最后一个: {values[-1]}")
                else:
                    merged_row[col] = ""

            merged_rows.append(merged_row)
            print(f"  合并完成，保留1行数据")

    # 6. 创建合并后的DataFrame
    merged_df = pd.DataFrame(merged_rows)

    # 7. 调整列顺序：确保code和name在前面
    # 获取所有列名
    all_columns = merged_df.columns.tolist()
    # 移除code和name
    all_columns.remove('code')
    all_columns.remove('name')
    # 重新排序：code, name, 其他列
    new_columns = ['code', 'name'] + all_columns
    merged_df = merged_df[new_columns]

    # 8. 保存结果
    merged_df.to_csv(output_csv, index=False, encoding='utf-8-sig')

    print(f"\n处理完成!")
    print(f"合并后数据行数: {len(merged_df)}")
    print(f"输出文件: {output_csv}")

    # 9. 输出统计信息
    print("\n=== 统计信息 ===")
    print(f"原始唯一板块数: {len(df['code'].unique())}")
    print(f"合并后唯一板块数: {len(merged_df['code'].unique())}")

    # 检查是否有代码重复
    duplicate_codes = merged_df['code'].value_counts()
    duplicate_codes = duplicate_codes[duplicate_codes > 1]
    if len(duplicate_codes) > 0:
        print(f"警告: 合并后仍有重复代码: {list(duplicate_codes.index)}")
    else:
        print("检查通过: 所有板块代码唯一")

    return merged_df


def analyze_duplicates(input_csv):
    """
    分析CSV文件中的重复板块代码
    """
    df = pd.read_csv(input_csv)

    if 'code' not in df.columns:
        print("错误：CSV文件中没有找到'code'列")
        return

    # 找出重复的code
    code_counts = df['code'].value_counts()
    duplicates = code_counts[code_counts > 1]

    if len(duplicates) == 0:
        print("没有发现重复的板块代码")
        return

    print(f"\n发现 {len(duplicates)} 个重复的板块代码:")
    print("=" * 60)

    for code, count in duplicates.items():
        print(f"\n板块代码: {code} (出现 {count} 次)")
        print("-" * 40)

        # 显示该code的所有行
        rows = df[df['code'] == code]
        for idx, row in rows.iterrows():
            # 找出有数据的日期列
            date_cols = [col for col in df.columns if col not in ['code', 'name']]
            data_cols = []
            for col in date_cols:
                if pd.notna(row[col]) and str(row[col]).strip() != "":
                    data_cols.append(col)

            print(f"  行 {idx+1}: name='{row['name']}', 数据列: {len(data_cols)}个")
            if data_cols:
                print(f"    有数据的日期列: {data_cols[:3]}..." if len(data_cols) > 3 else f"    有数据的日期列: {data_cols}")

    return duplicates


# 使用示例
if __name__ == "__main__":
    # 输入文件路径
    input_file = "data/data_industry_split.csv"  # 替换为你的CSV文件路径
    output_file = "data/data_industry_merged.csv"
    # input_file = "data/data_concept_split.csv"  # 替换为你的CSV文件路径
    # output_file = "data/data_concept_merged.csv"

    # 1. 先分析重复情况
    print("=" * 60)
    print("第一步：分析重复的板块代码")
    print("=" * 60)
    duplicates = analyze_duplicates(input_file)

    # 2. 合并重复行
    print("\n" + "=" * 60)
    print("第二步：合并重复行")
    print("=" * 60)
    result_df = merge_duplicate_rows_by_code(input_file, output_file, backup=True)

    # 3. 验证合并结果
    print("\n" + "=" * 60)
    print("第三步：验证合并结果")
    print("=" * 60)
    if result_df is not None:
        print(f"合并后数据预览（前5行）:")
        print(result_df.head())

        # 检查是否有剩余重复
        print(f"\n检查合并后是否有重复代码...")
        code_counts = result_df['code'].value_counts()
        if code_counts.max() > 1:
            print(f"警告: 仍有重复代码: {code_counts[code_counts > 1].to_dict()}")
        else:
            print("✓ 合并成功，所有板块代码唯一！")
