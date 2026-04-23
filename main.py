import duckdb
import os

# ── Cấu hình ──────────────────────────────────────────────────────────────────
DB_PATH    = "test.duckdb"
SCHEMA     = "dev"           # schema dbt build vào (từ profiles.yml)
OUTPUT_DIR = "output"
MARTS = [
    "mart_product_returns",
    "mart_sales_customer",
    "mart_temporal_operations",
]
# ──────────────────────────────────────────────────────────────────────────────

def export_marts():
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"Không tìm thấy '{DB_PATH}'. Chạy script từ thư mục root của project.")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    con = duckdb.connect(DB_PATH, read_only=True)

    # Liệt kê tất cả bảng trong schema dev
    available = {
        row[0] for row in con.execute(f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{SCHEMA}'
        """).fetchall()
    }

    print(f"📦 Các bảng trong schema '{SCHEMA}': {sorted(available)}\n")

    if not available:
        print("❌ Schema 'dev' trống — hãy chạy 'dbt run' trước rồi thử lại.")
        con.close()
        return

    for mart in MARTS:
        if mart not in available:
            print(f"⚠️  Bỏ qua '{mart}' — không tìm thấy trong schema '{SCHEMA}'.")
            continue

        out_path = os.path.join(OUTPUT_DIR, f"{mart}.parquet").replace("\\", "/")

        con.execute(f"""
            COPY (SELECT * FROM {SCHEMA}.{mart})
            TO '{out_path}'
            (FORMAT PARQUET)
        """)

        row_count = con.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{mart}").fetchone()[0]
        file_size = os.path.getsize(out_path) / 1024
        print(f"✅ {mart}.parquet  —  {row_count:,} rows  |  {file_size:.1f} KB")

    con.close()
    print(f"\n🎉 Xuất xong! Files nằm trong: ./{OUTPUT_DIR}/")


if __name__ == "__main__":
    export_marts()