import duckdb
import os

def ingest():
    # Connect to test.duckdb (tạo mới nếu chưa có)
    con = duckdb.connect("test.duckdb")

    # Tạo schema raw
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    print("✅ Schema 'raw' đã được tạo")

    # Danh sách các CSV trong thư mục data/
    csv_files = [
        "customers",
        "geography",
        "inventory",
        "order_items",
        "orders",
        "payments",
        "products",
        "promotions",
        "returns",
        "reviews",
        "sales",
        "shipments",
        "web_traffic",
    ]

    for table in csv_files:
        csv_path = f"data/{table}.csv"
        if os.path.exists(csv_path):
            con.execute(f"""
                CREATE OR REPLACE TABLE raw.{table} AS
                SELECT * FROM read_csv_auto('{csv_path}', header=true)
            """)
            count = con.execute(f"SELECT COUNT(*) FROM raw.{table}").fetchone()[0]
            print(f"✅ raw.{table:<20} — {count:>6} rows")
        else:
            print(f"⚠️  Không tìm thấy file: {csv_path}")

    # Kiểm tra tổng quan
    print("\n📋 Tất cả tables trong schema raw:")
    result = con.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'raw'
        ORDER BY table_name
    """).fetchdf()
    print(result.to_string(index=False))

    con.close()
    print("\n🎉 Hoàn thành! test.duckdb đã sẵn sàng để chạy dbt.")

if __name__ == "__main__":
    ingest()