# === Step 2: Compute and export daily percentage changes ===
# File: generate_index_changes.py
import pandas as pd
import datetime
from datetime import timedelta
from config import N_YEARS, OUTPUT_DATA_CSV, OUTPUT_CHANGE_XLSX

if __name__ == "__main__":
    df = pd.read_csv(OUTPUT_DATA_CSV, parse_dates=["date"])
    df.set_index("date", inplace=True)

    cutoff_date = datetime.datetime.now() - timedelta(days=365 * N_YEARS)
    df = df[df.index >= cutoff_date]

    pct_change_df = df.pct_change(fill_method=None) * 100
    pct_change_df = pct_change_df.round(2).dropna(how='all')

    with pd.ExcelWriter(OUTPUT_CHANGE_XLSX, engine="xlsxwriter", datetime_format='yyyy-mm-dd') as writer:
        pct_change_df.to_excel(writer, sheet_name="fluctuate")
        workbook = writer.book
        worksheet = writer.sheets["fluctuate"]

        max_row, max_col = pct_change_df.shape
        max_row += 1
        max_col += 1

        wrap_format = workbook.add_format({'text_wrap': True})
        worksheet.set_column(0, 0, 12, wrap_format)
        worksheet.set_column(1, max_col, 10, wrap_format)
        worksheet.freeze_panes(1, 1)

        worksheet.conditional_format(1, 1, max_row, max_col, {
            'type': '3_color_scale',
            'min_type': 'num',
            'mid_type': 'num',
            'max_type': 'num',
            'min_value': -8,
            'mid_value': 0,
            'max_value': 8,
            'min_color': '#63BE7B',
            'mid_color': '#FFFFFF',
            'max_color': '#F8696B',
        })

    print(f"✅ Successfully saved: {OUTPUT_CHANGE_XLSX}")