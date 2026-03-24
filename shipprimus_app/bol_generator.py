"""
Generate BOL PDF (letter) and 4×6 shipping label using ReportLab.
Mirrors patterns from SO.py (Code128 barcode) and address.py (4×6 canvas).
"""
import os
from reportlab.lib.pagesizes import letter, inch
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas
from reportlab.graphics import renderPDF
from reportlab.graphics.barcode import createBarcodeDrawing
from reportlab.lib.utils import ImageReader

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
ABF_LOGO_PATH = os.path.join(os.path.dirname(__file__), "..", "ABF.png")


def _barcode_drawing(value: str, bar_width=0.30, bar_height=10):
    return createBarcodeDrawing(
        "Code128",
        value=value,
        barWidth=bar_width * mm,
        barHeight=bar_height * mm,
    )


def generate_bol(
    bol_number: str,
    shipper: dict,
    consignee: dict,
    freight_items: list,
    carrier_name: str,
    quote_number: str,
    bill_to: dict = None,
    so_number: str = "",
) -> str:
    """
    Create BOL PDF on letter page. Returns local file path.

    shipper/consignee: {company, address, city, state, zip, country}
    freight_items: [{qty, description, weight, length, width, height, class}]
    bill_to: {name, address, city, state, zip} or None
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, f"BOL_{bol_number}.pdf")

    W, H = letter  # 612 x 792 pt
    c = canvas.Canvas(out_path, pagesize=letter)

    # ── Header ────────────────────────────────────────────────────────
    c.setFont("Helvetica-Bold", 16)
    c.drawString(0.5 * inch, H - 0.6 * inch, "BILL OF LADING")
    c.setFont("Helvetica", 10)
    c.drawString(0.5 * inch, H - 0.85 * inch, "International Home Miami")

    if so_number:
        c.setFont("Helvetica", 9)
        c.drawString(0.5 * inch, H - 1.05 * inch, f"Sales Order: {so_number}")

    # BOL barcode top-right
    bc = _barcode_drawing(bol_number, bar_width=0.25, bar_height=12)
    bc_x = W - bc.width - 0.5 * inch
    renderPDF.draw(bc, c, bc_x, H - 0.9 * inch)
    c.setFont("Helvetica-Bold", 9)
    c.drawCentredString(bc_x + bc.width / 2, H - 1.0 * inch, f"BOL: {bol_number}")

    # ── Divider ───────────────────────────────────────────────────────
    c.setStrokeColor(colors.black)
    c.line(0.5 * inch, H - 1.15 * inch, W - 0.5 * inch, H - 1.15 * inch)

    # ── Shipper / Consignee boxes ─────────────────────────────────────
    box_top = H - 1.2 * inch
    box_h = 1.4 * inch
    mid_x = W / 2

    # Shipper (left)
    c.rect(0.5 * inch, box_top - box_h, mid_x - 0.6 * inch, box_h)
    c.setFont("Helvetica-Bold", 9)
    c.drawString(0.6 * inch, box_top - 0.18 * inch, "SHIPPER:")
    c.setFont("Helvetica", 9)
    _draw_address_block(c, shipper, 0.6 * inch, box_top - 0.35 * inch)

    # Consignee (right)
    c.rect(mid_x + 0.1 * inch, box_top - box_h, mid_x - 0.6 * inch, box_h)
    c.setFont("Helvetica-Bold", 9)
    c.drawString(mid_x + 0.2 * inch, box_top - 0.18 * inch, "CONSIGNEE:")
    c.setFont("Helvetica", 9)
    _draw_address_block(c, consignee, mid_x + 0.2 * inch, box_top - 0.35 * inch)

    # ── Carrier / Quote row ───────────────────────────────────────────
    row_y = box_top - box_h - 0.1 * inch
    c.setFont("Helvetica-Bold", 9)
    c.drawString(0.5 * inch, row_y, f"Carrier: {carrier_name}")
    c.drawString(mid_x, row_y, f"Quote #: {quote_number}")

    # ── Freight Items Table ───────────────────────────────────────────
    table_top = row_y - 0.3 * inch
    col_x = [0.5 * inch, 1.2 * inch, 3.8 * inch, 4.6 * inch, 5.1 * inch, 5.6 * inch, 6.2 * inch]
    headers = ["QTY", "DESCRIPTION", "WEIGHT", "L", "W", "H", "CLASS"]

    c.setFont("Helvetica-Bold", 8)
    for i, h in enumerate(headers):
        c.drawString(col_x[i], table_top, h)

    c.line(0.5 * inch, table_top - 2, W - 0.5 * inch, table_top - 2)

    c.setFont("Helvetica", 8)
    row_y2 = table_top - 0.2 * inch
    for item in freight_items:
        c.drawString(col_x[0], row_y2, str(item.get("qty", "")))
        c.drawString(col_x[1], row_y2, str(item.get("description", ""))[:35])
        c.drawString(col_x[2], row_y2, str(item.get("weight", "")))
        c.drawString(col_x[3], row_y2, str(item.get("length", "")))
        c.drawString(col_x[4], row_y2, str(item.get("width", "")))
        c.drawString(col_x[5], row_y2, str(item.get("height", "")))
        c.drawString(col_x[6], row_y2, str(item.get("freight_class", "")))
        row_y2 -= 0.2 * inch

    # ── Bill To ───────────────────────────────────────────────────────
    if bill_to:
        bt_y = row_y2 - 0.2 * inch
        c.setFont("Helvetica-Bold", 9)
        c.drawString(0.5 * inch, bt_y, "BILL TO:")
        c.setFont("Helvetica", 9)
        _draw_address_block(c, bill_to, 0.5 * inch, bt_y - 0.18 * inch)
        sig_y = bt_y - 1.0 * inch
    else:
        sig_y = row_y2 - 0.5 * inch

    # ── Signature / Date ──────────────────────────────────────────────
    sig_y = max(sig_y, 1.0 * inch)
    c.line(0.5 * inch, sig_y, 3.5 * inch, sig_y)
    c.drawString(0.5 * inch, sig_y + 4, "Shipper Signature / Date")
    c.line(4.0 * inch, sig_y, W - 0.5 * inch, sig_y)
    c.drawString(4.0 * inch, sig_y + 4, "Carrier Signature / Date")

    c.save()
    return out_path


def generate_label(
    bol_number: str,
    shipper: dict,
    consignee: dict,
    pickup_date: str = "",
    po_number: str = "",
    so_number: str = "",
    freight_items: list | None = None,
    label_number: int = 1,
    total_labels: int = 1,
) -> str:
    """
    Create 4×6 shipping label PDF. Returns local file path.
    One label per page (print on 4×6 label printer).
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, f"label_{bol_number}.pdf")

    freight_items = freight_items or []
    total_weight = sum((float(item.get("weight", 0) or 0) * int(item.get("qty", 1) or 1)) for item in freight_items)
    total_pieces = sum(int(item.get("qty", 0) or 0) for item in freight_items) or 1

    W, H = 4 * inch, 6 * inch
    c = canvas.Canvas(out_path, pagesize=(W, H))
    margin = 0.3 * inch

    # Header / carrier block
    c.setFillColor(colors.white)
    c.rect(margin, H - 1.18 * inch, W - (2 * margin), 0.9 * inch, fill=1, stroke=0)
    if os.path.exists(ABF_LOGO_PATH):
        logo = ImageReader(ABF_LOGO_PATH)
        c.drawImage(
            logo,
            margin,
            H - 1.05 * inch,
            width=1.65 * inch,
            height=0.72 * inch,
            preserveAspectRatio=True,
            mask="auto",
        )
    else:
        c.setFont("Helvetica-Bold", 18)
        c.drawString(margin + 0.12 * inch, H - 0.68 * inch, "ABF")
        c.setFont("Helvetica-Bold", 10)
        c.drawString(margin + 0.72 * inch, H - 0.60 * inch, "ABF Freight")
    c.setFillColor(colors.black)
    c.setFont("Helvetica", 8)
    c.drawString(margin, H - 1.10 * inch, "arcb.com/abf")
    c.drawString(margin, H - 1.24 * inch, "1-800-610-5544")
    c.setFont("Helvetica-Bold", 10)
    c.drawRightString(W - margin - 0.08 * inch, H - 0.68 * inch, f"{label_number}/{total_labels}")

    # Shipment summary
    summary_top = H - 1.48 * inch
    c.setFont("Helvetica-Bold", 8)
    c.drawString(margin, summary_top, f"PICKUP DATE: {pickup_date or '-'}")
    c.drawRightString(W - margin, summary_top, f"PO #: {po_number or '-'}")
    c.drawString(margin, summary_top - 0.16 * inch, f"PIECES: {total_pieces}")
    c.drawRightString(W - margin, summary_top - 0.16 * inch, f"WEIGHT: {total_weight:.0f} LB")
    c.drawString(margin, summary_top - 0.32 * inch, f"SO #: {so_number or '-'}")

    # FROM
    from_top = H - 1.98 * inch
    c.setFont("Helvetica-Bold", 10)
    c.drawString(margin, from_top, "SHIP FROM")
    c.setFont("Helvetica", 9)
    _draw_address_block(c, shipper, margin, from_top - 0.18 * inch, line_height=11, extra_lines=[
        "Johanna Sifontes",
        "305-620-6500",
    ])

    c.line(margin, H - 2.88 * inch, W - margin, H - 2.88 * inch)

    # TO
    c.setFont("Helvetica-Bold", 12)
    c.drawString(margin, H - 3.14 * inch, "SHIP TO")
    c.setFont("Helvetica", 12)
    _draw_address_block(c, consignee, margin, H - 3.42 * inch, line_height=15)

    # BOL / tracking barcode bottom
    bc = _barcode_drawing(bol_number, bar_width=0.35, bar_height=13)
    bc_x = (W - bc.width) / 2
    renderPDF.draw(bc, c, bc_x, 0.70 * inch)
    c.setFont("Helvetica-Bold", 9)
    c.drawCentredString(W / 2, 0.52 * inch, f"BOL / TRACKING: {bol_number}")

    c.showPage()
    c.save()
    return out_path


def _draw_address_block(c, addr: dict, x: float, y: float, line_height: int = 11, extra_lines: list | None = None):
    lines = []
    company = addr.get("company") or addr.get("addressee") or addr.get("name") or ""
    if company:
        lines.append(company)
    addr1 = addr.get("address") or addr.get("address1") or ""
    if addr1:
        lines.append(addr1)
    city = addr.get("city", "")
    state = addr.get("state", "")
    zip_ = addr.get("zip", "")
    if city or state or zip_:
        lines.append(f"{city}, {state} {zip_}".strip(", "))
    country = addr.get("country", "")
    if country and country.upper() not in ("US", "USA"):
        lines.append(country)
    for extra in extra_lines or []:
        if extra:
            lines.append(extra)

    for line in lines:
        c.drawString(x, y, line)
        y -= line_height
