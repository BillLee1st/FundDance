# python bk_ahead_merge.py


import pandas as pd


def split_row_key_with_order(input_csv, output_csv):
    df = pd.read_csv(input_csv)

    # 分割row_key
    df[['code', 'name']] = df['row_key'].str.split('|', n=1, expand=True)

    # 删除row_key列
    df.drop('row_key', axis=1, inplace=True)

    # 调整列顺序：将code和name移到前面
    cols = df.columns.tolist()
    # 将code和name移到前面
    cols = ['code', 'name'] + [col for col in cols if col not in ['code', 'name']]
    df = df[cols]

    # 保存
    df.to_csv(output_csv, index=False, encoding='utf-8-sig')

    return df

# 使用
# df = split_row_key_with_order("data/data_concept.csv", "data/data_concept_split.csv")
df = split_row_key_with_order("data/data_industry.csv", "data/data_industry_split.csv")
print(df.head())
