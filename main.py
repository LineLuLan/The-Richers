import duckdb
import os

# tạo folder nếu chưa có
os.makedirs(r"D:\Datathon_test\test\output", exist_ok=True)

con = duckdb.connect(r"D:\Datathon_test\test\test.duckdb")

con.execute("""
COPY dev.mart_temporal_operations 
TO 'D:/Datathon_test/test/output/mart_product_returns.parquet' 
(FORMAT PARQUET)
""")

con.close()

print("Export xong!")