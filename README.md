# The Richers — Datathon 2026 Round 1

Bài dự thi **Datathon 2026 — Round 1** của team **The Richers**: phân tích và dự báo cho dataset e-commerce thời trang Việt Nam (2012-07 → 2022-12), gồm 3 phần: 10 câu trắc nghiệm (MCQ), EDA 4 cấp độ Descriptive–Diagnostic–Predictive–Prescriptive, và dự báo Revenue/COGS theo ngày cho 548 ngày (2023-01-01 → 2024-07-01).

**Team**: Trần Nam Anh · Đặng Minh Phát · Nguyễn Đức Hải · Ngô Thị Ánh Dương

---

## Kết quả chính

| Hạng mục | Kết quả | Artifact |
|----------|---------|----------|
| MCQ (Phần 1) | 10/10 câu có code logic + cross-check | `notebooks/MCQ_solutions.ipynb` |
| EDA (Phần 2) | 4 cấp Desc–Diag–Pred–Prescriptive trên 3 chủ đề | `notebooks/03_DA_eda_main.ipynb`, `figures/` |
| Forecast (Phần 3) | CV Revenue R² **0.7823**, COGS R² **0.7916** | `notebooks/04_MLE_KILLER_FINAL.ipynb` |
| Submission | 548 ngày, đúng format `sample_submission.csv` | `data/submission_final.csv` |
| Báo cáo | NeurIPS-style 7 trang (4 nội dung + 3 checklist) | `results/main.pdf` |

**Đáp án MCQ**: `Q1=C, Q2=D, Q3=B, Q4=C, Q5=C, Q6=A, Q7=C, Q8=A, Q9=A, Q10=C`

---

## Cấu trúc thư mục

```
The-Richers/
├── data/
│   ├── raw/                          # 13 file CSV gốc + sample_submission.csv (KHÔNG sửa)
│   ├── interim/                      # parquet sau clean (sinh từ src/clean_data.py)
│   ├── processed/                    # 5 marts (sinh từ src/build_marts.py)
│   └── submission_final.csv          # ★ Output forecast cuối cùng
├── notebooks/
│   ├── 01_data_profiling.ipynb       # Profile 13 bảng → profiling_report.md
│   ├── 02_DE_clean_and_marts.ipynb   # Chạy clean + build 5 marts
│   ├── 03_DA_eda_main.ipynb          # EDA 4 cấp Desc–Diag–Pred–Prescriptive
│   ├── 04_MLE_KILLER_FINAL.ipynb     # ★ Forecast Revenue/COGS (sinh submission)
│   ├── MCQ_solutions.ipynb           # ★ 10 câu trắc nghiệm + đáp án
│   └── baseline.ipynb                # Baseline ban đầu (tham khảo)
├── src/
│   ├── clean_data.py                 # Module clean 13 bảng raw
│   ├── build_marts.py                # Module build 5 marts
│   └── combine_predictive_figure.py  # Ghép figure cho báo cáo
├── figures/                          # Figures EDA + SHAP + forecast + metrics_summary.csv
├── results/
│   ├── main.tex / main.pdf           # ★ Báo cáo NeurIPS-style
│   ├── neurips_2025.sty              # Style file (KHÔNG sửa)
│   ├── figures/                      # Subset figures dùng trong PDF
│   └── README.md                     # Hướng dẫn compile LaTeX
├── requirements.txt
└── README.md                         # File này
```

---

## Yêu cầu hệ thống

- **Python**: 3.10+ (đã test trên Windows 10)
- **RAM**: ≥ 4 GB (peak ~2 GB khi build mart1 với 714K orderlines)
- **Disk**: ~250 MB cho cleaned parquet + marts
- **Optional**: TeX Live 2020+ với `xelatex` để compile lại `results/main.pdf`

---

## Cách reproduce (4 bước)

### Bước 1 — Cài môi trường

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate

pip install -r requirements.txt
```

### Bước 2 — Profile + Clean + Build marts (~30 giây)

```bash
jupyter nbconvert --execute --to notebook --inplace notebooks/01_data_profiling.ipynb
jupyter nbconvert --execute --to notebook --inplace notebooks/02_DE_clean_and_marts.ipynb
```

Notebook 02 sẽ gọi `src/clean_data.py` → `data/interim/*.parquet` (13 file) và `src/build_marts.py` → `data/processed/mart{1..5}_*.parquet`.

### Bước 3 — Verify đáp án MCQ

```bash
jupyter nbconvert --execute --to notebook --inplace notebooks/MCQ_solutions.ipynb
```

Cell cuối in `Q1..Q10` để cross-check với phần "Kết quả chính" ở trên.

### Bước 4 — Forecast → ghi `data/submission_final.csv` (~2-3 phút)

```bash
jupyter nbconvert --execute --to notebook --inplace notebooks/04_MLE_KILLER_FINAL.ipynb
```

Notebook 04 sinh ra:
- `data/submission_final.csv` (548 rows, 3 cột `Date,Revenue,COGS`)
- `figures/forecast_overview.png`, `figures/alpha_curves.png`
- `figures/shap_revenue.png`, `figures/shap_cogs_bar.png` (interpretability)

> EDA notebook (`03_DA_eda_main.ipynb`) sinh các figure dùng trong báo cáo (`A_*`, `B_*`, `C_*`); chạy nó nếu muốn re-render các hình. Không bắt buộc cho việc tạo submission.

---

## Kiến trúc forecasting (Phần 3)

Multi-layer hybrid với per-target optimization:

```
Layer 1: Baseline (geometric trend × seasonal index)         — R² 0.77
Layer 2: Cross-table dim features (region/payment/device/    — +0.5pp Rev R²
         source/status shares, 23 features)
Layer 3: Macro events (5 cờ regime: growth/2019_slowdown/    — +0.4pp COGS R²
         covid_lockdown/post_lockdown/inflation_2022)            (chỉ áp COGS)
Layer 4: ML residual + Alpha-shrinkage
         • Revenue : XGBoost (58 features), α = 0.350
         • COGS    : CatBoost (63 features), α = 0.275
```

### CV ablation (2 fold time-series, val=2021 và val=2022)

| Approach | Rev MAE | Rev R² | COGS MAE | COGS R² |
|----------|--------:|-------:|---------:|--------:|
| Baseline only | 597K | 0.7663 | 489K | 0.7821 |
| + Alpha-shrinkage | 569K | 0.7773 | 476K | 0.7871 |
| + Cross-table (Plan A) | 564K | 0.7823 | 476K | 0.7878 |
| **+ Macro events COGS (final)** | **564K** | **0.7823** | **471K** | **0.7916** |

### No-leakage guarantees

- Baseline được fit lại từ scratch trong mỗi fold (`upper_year = y - 1`).
- Cross-table dim features test được impute từ lookup `(month, dow)` trên train post-2020 và áp dụng đồng nhất train↔test.
- Macro flags cho test (post-2022) đều = 0 (không lookahead).
- Random seed = 42, deterministic CV splits.

---

## 5 Data Marts

| Mart | Grain | Use case |
|------|-------|----------|
| `mart1_order_lines` | 1 row = 1 orderline | EDA chéo nhiều chiều (region × segment × promo × age) |
| `mart2_orders` | 1 row = 1 order | Customer behavior, payment, shipment |
| `mart3_products` | 1 row = 1 product | Diagnostics, return rate, lifetime metrics |
| `mart4_daily_ops` | 1 row = 1 day | Forecasting backbone + date_dim (Tết, 11/11, 12/12, Black Friday…) |
| `mart5_promotion_perf` | 1 row = 1 promo | Promo ROI |

Coverage: 13/13 raw tables xuất hiện trong ít nhất 1 mart.

---

## Lưu ý quan trọng cho giám khảo

1. **`flag_order_before_signup = 1`** (477,453 đơn ≈ 73.8%): những đơn có `order_date < signup_date`. Code chủ động flag (không drop) — khi tính `customer_tenure_days` cần filter `=0`.
2. **`flag_status_shipment_mismatch = 1`** (564 đơn): status `delivered/shipped/returned` nhưng không có shipment record. Flag, không drop.
3. **`is_revenue_eligible = 1`** (89.7% đơn): dùng khi cần doanh thu thực thu (loại `cancelled`, `created`).
4. **Mart 4 test period (2023-01 → 2024-07)**: chỉ có date features; các cột non-date đều là NaN — đúng theo spec để tránh leakage.
5. **Reproducibility**: random seed = 42 trong notebook 04; CV splits xác định bởi năm.
6. **Sanity check tự động**: notebook 02 có cell `assert` cho Q5/Q9/promo duration sau khi clean → fail sớm nếu data drift.

---

## Báo cáo PDF

- File: `results/main.pdf` (đã build sẵn)
- Source: `results/main.tex` — compile bằng `xelatex` (do có Tiếng Việt):

```bash
cd results
xelatex main.tex
xelatex main.tex   # chạy 2 lần để cross-references đúng
```

Chi tiết style + font requirement: `results/README.md`.

---

## Repo

GitHub: https://github.com/LineLuLan/The-Richers
