import duckdb
import os

# 虛擬 CSV 內容
csv_content = """交易日期,契約代碼,到期月份週別,履約價,買賣權,開盤價,最高價,最低價,收盤價,結算價,成交量,未沖銷契約數,交易時段
20250701,TX,202507,0,C,18000,18050,17950,18020,18020,1000,500,一般
20250701,TX,202507,0,P,200,220,180,210,210,800,400,一般
20250702,MTX,202507W1,0,C,9000,9050,8950,9020,9020,500,200,盤後
"""

raw_db_path = "dummy_raw.duckdb"
if os.path.exists(raw_db_path):
    os.remove(raw_db_path)

conn = duckdb.connect(raw_db_path)
conn.execute("""
CREATE TABLE raw_import_log (
    source_file VARCHAR,
    member_file VARCHAR,
    file_content_as_text VARCHAR,
    imported_at TIMESTAMPTZ DEFAULT now()
);
""")
conn.execute("INSERT INTO raw_import_log (source_file, member_file, file_content_as_text) VALUES (?, ?, ?)",
             ["dummy_source_1.csv", "member_1.zip", csv_content])
conn.execute("INSERT INTO raw_import_log (source_file, member_file, file_content_as_text) VALUES (?, ?, ?)",
             ["dummy_source_2.csv", "member_2.zip", csv_content]) # 插入兩次以模擬多個文件
conn.close()
print(f"'{raw_db_path}' created successfully with dummy data.")
