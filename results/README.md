# Datathon 2026 — Round 1 Final Report (LaTeX)

Báo cáo chính thức gửi BTC Datathon 2026 — Team **The Richers**.

## Files

```
report/
├── main.tex                       ← LaTeX source chính (~17KB, 4 trang)
├── main.pdf                       ← Output cuối cùng để nộp BTC
├── neurips_2025.sty               ← Style file NeurIPS (KHÔNG SỬA)
├── README.md                      ← File này
├── combine_predictive_figure.py   ← Script ghép RFM + Refund thành 1 figure
└── figures/
    ├── A_1_1_category_overview.png       ← §1.1 Descriptive
    ├── A_2_1_return_reason_diagnostic.png ← §1.2 Diagnostic (panel a)
    ├── C_1_2_seasonal_revenue.png        ← §1.2 Diagnostic (panel b)
    ├── B_3_1_rfm_segmentation.png        ← §1.3 Predictive (input)
    ├── C_3_1_refund_forecast.png         ← §1.3 Predictive (input)
    ├── predictive_rfm_refund.png         ← §1.3 Predictive (ghép từ 2 file trên)
    ├── A_4_1_action_quadrant.png         ← §1.4 Prescriptive
    ├── shap_revenue.png                  ← §2.3 SHAP plot (panel a)
    └── forecast_overview.png             ← §2.3 Forecast plot (panel b)
```

## QUAN TRỌNG: Compile bằng XeLaTeX (KHÔNG dùng pdflatex)

```bash
xelatex main.tex
xelatex main.tex   # chạy 2 lần để cross-references và labels đúng
```

Lý do: báo cáo có tiếng Việt với dấu đầy đủ, pdflatex không support Unicode trực tiếp.

## Cấu trúc PDF (4 trang đúng giới hạn thể lệ)

| Trang | Nội dung |
|-------|----------|
| 1 | Title + Authors + Abstract + §1 intro + §1.1 **Descriptive** (Figure 1: Portfolio overview) |
| 2 | §1.2 **Diagnostic** (Figure 2: Wrong Size + Mega Sales subfigures) + §1.3 **Predictive** (Figure 3: RFM + Refund Forecast) |
| 3 | §1.3 text + §1.4 **Prescriptive** (Figure 4: Action Quadrant + Top 5 Actions) + §2 Sales Forecasting (§2.1 Pipeline) |
| 4 | §2.2 CV & Ablation (Table 1) + §2.3 SHAP (Figure 5: subfigures) + Compliance + GitHub link |

Báo cáo theo cấu trúc **4 cấp độ phân tích** đúng rubric chấm thi (Descriptive → Diagnostic → Predictive → Prescriptive) để giám khảo dễ tick từng tiêu chí.

## Quy trình tạo figures từ notebook

Hầu hết figures được sinh tự động khi chạy notebook trong thư mục `notebooks/`:

```bash
# Sinh các figures EDA (chạy notebook 03)
jupyter nbconvert --to notebook --execute ../notebooks/03_DA_eda_main.ipynb

# Sinh forecast_overview.png và shap_revenue.png (chạy notebook 04)
jupyter nbconvert --to notebook --execute ../notebooks/04_MLE_KILLER_FINAL.ipynb
```

Sau khi notebook chạy xong, copy các file PNG cần dùng vào `figures/` cạnh `main.tex`.

### Riêng `predictive_rfm_refund.png` (Figure 3)

Đây là figure **ghép từ 2 figure gốc** để tiết kiệm chiều dọc trang:
- `B_3_1_rfm_segmentation.png` (RFM scatter + bar, từ Cell B.3.1)
- `C_3_1_refund_forecast.png` (Holt-Winters forecast, từ Cell C.3.1)

Sau khi notebook đã sinh ra 2 file trên, ghép bằng:

```bash
pip install pillow
python combine_predictive_figure.py
```

Script tự tìm 2 file PNG trong thư mục hiện tại hoặc `figures/`, ghép xếp dọc (RFM trên, Refund dưới) và lưu thành `figures/predictive_rfm_refund.png`.

## Yêu cầu hệ thống

- **TeX Live 2020+** với XeLaTeX (cài qua [TUG](https://tug.org/texlive/) hoặc MiKTeX trên Windows)
- **Fonts:** mặc định `main.tex` dùng Times New Roman + Arial + Courier New (font hệ thống Windows/macOS).
- Nếu hệ thống không có Times New Roman (ví dụ Linux thuần), thay 3 dòng font trong `main.tex` bằng phiên bản TeX Gyre có sẵn trong TeX Live:
  ```latex
  \setmainfont{TeX Gyre Termes}
  \setsansfont{TeX Gyre Heros}
  \setmonofont{TeX Gyre Cursor}
  ```
  Layout vẫn giữ 4 trang vì TeX Gyre Termes có metric gần Times New Roman.

## Notes

- **Figure layout tối ưu space:** §1.2 và §2.3 dùng `subfigure` (2 panel cạnh nhau); §1.3 dùng figure ghép dọc qua script Python để giữ chữ trong figure đọc rõ.
- **Tránh tràn trang:** Trang 4 hiện còn ~25% chiều cao trống làm buffer — nếu chỉnh sửa text, cẩn thận không vượt quá ngưỡng này.
- **Tiếng Việt:** Mọi caption, body text đều có dấu đầy đủ. Đảm bảo XeLaTeX dùng font hỗ trợ Unicode (mặc định OK với Times New Roman/TeX Gyre Termes).
- **Cross-refs:** Báo cáo có refs giữa các section (ví dụ `(§1.2)`, `(§1.3)`) — đảm bảo chạy `xelatex` 2 lần để các refs và labels hiển thị đúng.
