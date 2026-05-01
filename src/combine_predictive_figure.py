"""
combine_predictive_figure.py
=============================
Ghép B_3_1_rfm_segmentation.png (RFM, 2-panel ngang)
   + C_3_1_refund_forecast.png (Refund forecast, 1-panel ngang)
thành 1 file predictive_rfm_refund.png xếp DỌC để dùng trong main.tex.

CÁCH DÙNG:
    1. Đặt 2 file PNG nguồn cùng thư mục với script này (hoặc trong figures/)
    2. python combine_predictive_figure.py
    3. Output: predictive_rfm_refund.png (cạnh script) hoặc figures/predictive_rfm_refund.png

YÊU CẦU:
    pip install pillow
"""

from PIL import Image
import os
import sys

# ==== Cấu hình ====
RFM_FILE = "B_3_1_rfm_segmentation.png"
REFUND_FILE = "C_3_1_refund_forecast.png"
OUTPUT_FILE = "predictive_rfm_refund.png"

# Tỷ lệ chiều cao: Refund chiếm ~70% chiều cao của RFM (vì RFM là 2-panel "nặng" hơn)
REFUND_HEIGHT_RATIO = 0.70
PAD_BETWEEN = 25  # px giữa 2 panel
PAD_TOP = 10
PAD_BOTTOM = 10


def find_file(filename):
    """Tìm file ở thư mục hiện tại hoặc figures/."""
    candidates = [filename, os.path.join("figures", filename)]
    for path in candidates:
        if os.path.exists(path):
            return path
    raise FileNotFoundError(
        f"Không tìm thấy '{filename}'. "
        f"Tìm trong: {', '.join(candidates)}. "
        f"Hãy đặt file PNG cùng thư mục với script, hoặc trong subfolder figures/."
    )


def determine_output_dir():
    """Output vào figures/ nếu thư mục đó tồn tại, không thì cạnh script."""
    if os.path.isdir("figures"):
        return "figures"
    return "."


def main():
    rfm_path = find_file(RFM_FILE)
    refund_path = find_file(REFUND_FILE)
    out_dir = determine_output_dir()
    out_path = os.path.join(out_dir, OUTPUT_FILE)

    print(f" Loading:")
    print(f"   - {rfm_path}")
    print(f"   - {refund_path}")

    rfm = Image.open(rfm_path).convert("RGB")
    refund = Image.open(refund_path).convert("RGB")

    print(f" Sizes gốc:")
    print(f"   - RFM:    {rfm.size}  (ratio {rfm.width/rfm.height:.2f})")
    print(f"   - Refund: {refund.size}  (ratio {refund.width/refund.height:.2f})")

    # Mục tiêu: cả 2 panel có CÙNG width (= width lớn hơn của 2 cái).
    # Refund có chiều cao thấp hơn RFM 30% để tổng figure không quá cao.
    target_width = max(rfm.width, refund.width)

    # Resize RFM: scale theo width, giữ aspect ratio
    rfm_h = int(rfm.height * target_width / rfm.width)
    rfm_resized = rfm.resize((target_width, rfm_h), Image.LANCZOS)

    # Refund: scale sao cho height = REFUND_HEIGHT_RATIO * rfm_h
    refund_target_h = int(rfm_h * REFUND_HEIGHT_RATIO)
    refund_target_w_native = int(refund.width * refund_target_h / refund.height)
    if refund_target_w_native > target_width:
        # nếu refund quá rộng, fit theo width thay vì height
        refund_target_w_native = target_width
        refund_target_h = int(refund.height * target_width / refund.width)
    refund_resized = refund.resize((refund_target_w_native, refund_target_h), Image.LANCZOS)

    # Compose canvas
    total_w = target_width
    total_h = PAD_TOP + rfm_h + PAD_BETWEEN + refund_target_h + PAD_BOTTOM
    canvas = Image.new("RGB", (total_w, total_h), color=(255, 255, 255))

    # Paste RFM ở trên, căn trái (vì width đã = target_width)
    canvas.paste(rfm_resized, (0, PAD_TOP))

    # Paste Refund ở dưới, căn giữa (vì có thể hẹp hơn target_width)
    refund_x = (target_width - refund_target_w_native) // 2
    refund_y = PAD_TOP + rfm_h + PAD_BETWEEN
    canvas.paste(refund_resized, (refund_x, refund_y))

    canvas.save(out_path, "PNG", optimize=True)

    print(f"   Saved: {out_path}")
    print(f"   Final size: {total_w}x{total_h}  (ratio {total_w/total_h:.2f})")
    print(f"   Panel trên: RFM        {target_width}x{rfm_h}")
    print(f"   Panel dưới: Refund     {refund_target_w_native}x{refund_target_h}  (centered)")


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as e:
        print(f"No {e}", file=sys.stderr)
        sys.exit(1)
