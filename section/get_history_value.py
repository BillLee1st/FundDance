# === Step 1: Download historical index data ===
# File: fetch_index_data.py
import pandas as pd
import datetime
from datetime import timedelta
import akshare as ak
from tqdm import tqdm
from config import N_YEARS, INPUT_CODE_CSV, OUTPUT_DATA_CSV

def fetch_index_data(symbol: str, colname: str, start_str: str, end_str: str) -> pd.DataFrame:
    df = ak.index_zh_a_hist(
        symbol=symbol,
        period="daily",
        start_date=start_str,
        end_date=end_str
    )
    df = df[["日期", "收盘"]].rename(columns={"收盘": colname})
    df["日期"] = pd.to_datetime(df["日期"])
    df.set_index("日期", inplace=True)
    return df

if __name__ == "__main__":
    end_date = datetime.date.today()
    start_date = end_date - timedelta(days=N_YEARS * 365)
    start_str = start_date.strftime("%Y%m%d")
    end_str = end_date.strftime("%Y%m%d")

    df_code = pd.read_csv(INPUT_CODE_CSV)
    code_map = dict(zip(df_code['code'].astype(str), df_code['section']))

    all_data = pd.DataFrame()
    for code, section in tqdm(code_map.items(), desc="Fetching index data from AkShare"):
        try:
            df = fetch_index_data(code, section, start_str, end_str)
            all_data = df if all_data.empty else all_data.join(df, how="outer")
        except Exception as e:
            print(f"⚠️ Failed to fetch {section} ({code}): {e}")

    all_data.sort_index(inplace=True)
    all_data = all_data.reset_index().rename(columns={"index": "date", "日期": "date"})
    all_data.to_csv(OUTPUT_DATA_CSV, encoding="utf-8-sig", index=False)
    print(f"✅ Saved to: {OUTPUT_DATA_CSV}")