"""
ShipPrimus Freight Quoting App — Flask routes
"""
import os
import json
import logging

# File logger — writes to output/app.log
_log_path = os.path.join(os.path.dirname(__file__), "output", "app.log")
os.makedirs(os.path.dirname(_log_path), exist_ok=True)
logging.basicConfig(
    filename=_log_path,
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("shipprimus")

from flask import (
    Flask, render_template, request, redirect, url_for,
    session, flash, send_from_directory, jsonify,
)
from flask_session import Session
import config as cfg
from primus_client import client as primus
import netsuite as ns

app = Flask(__name__)
app.secret_key = cfg.FLASK_SECRET_KEY

# Server-side filesystem sessions — avoids the 4 KB cookie limit
_SESSION_DIR = os.path.join(os.path.dirname(__file__), ".sessions")
os.makedirs(_SESSION_DIR, exist_ok=True)
app.config["SESSION_TYPE"] = "filesystem"
app.config["SESSION_FILE_DIR"] = _SESSION_DIR
app.config["SESSION_PERMANENT"] = False
Session(app)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")


# ──────────────────────────────────────────────────────────────────────────────
# Dashboard
# ──────────────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    recent = _load_recent()
    return render_template("index.html", recent=recent)


# ──────────────────────────────────────────────────────────────────────────────
# Step 1 — Quote form
# ──────────────────────────────────────────────────────────────────────────────

@app.route("/quote", methods=["GET", "POST"])
def quote():
    so_data = None
    error = None

    if request.method == "POST":
        action = request.form.get("action")

        if action == "lookup":
            so_number = request.form.get("so_number", "").strip()
            if not so_number:
                error = "Please enter a Sales Order number."
            else:
                try:
                    so_data = ns.get_so(so_number)
                    so_id = str(so_data.get("id", ""))
                    session["so_internal_id"] = so_id
                    session["so_tranid"] = so_number
                    session["so_phone"] = so_data.get("phone", "")
                    session["so_po_number"] = so_data.get("po_number", "")

                    # Auto-load packing specs from Minisoft records
                    try:
                        freight_from_ns = ns.get_so_freight(so_id)
                        if freight_from_ns:
                            session["freight_items"] = freight_from_ns
                    except Exception as fe:
                        flash(f"Packing data unavailable: {fe}", "warning")
                except Exception as e:
                    error = str(e)

        elif action == "get_rates":
            # Build freight items from form
            freight_items = _parse_freight_form(request.form)
            if not freight_items:
                flash("Add at least one freight line.", "warning")
                return redirect(url_for("quote"))

            origin = cfg.SHIPPER.copy()
            consignee = {
                "addressee": request.form.get("addressee", ""),
                "address1": request.form.get("address1", ""),
                "city": request.form.get("city", ""),
                "state": request.form.get("state", ""),
                "zip": request.form.get("zip", ""),
                "country": request.form.get("country", "US"),
                "phone": request.form.get("phone", "") or session.get("so_phone", ""),
            }
            accessorials = request.form.getlist("accessorials[]")

            session["origin"] = origin
            session["consignee"] = consignee
            session["freight_items"] = freight_items
            session["accessorials"] = accessorials

            try:
                rates = primus.get_rates(origin, consignee, freight_items, accessorials)
                # Render rates.html directly — do NOT store rates in session
                # (16+ rate objects easily exceed the 4 KB cookie limit)
                shipper = cfg.SHIPPER
                return render_template("rates.html", rates=rates,
                                       consignee=consignee, shipper=shipper,
                                       accessorials=accessorials)
            except Exception as e:
                flash(f"Rating error: {e}", "danger")

    # Pre-fill consignee from session if available
    consignee = session.get("consignee", {})
    freight_items = session.get("freight_items", [_default_freight_row()])
    shipper = cfg.SHIPPER
    return render_template(
        "quote.html",
        so_data=so_data,
        consignee=consignee,
        freight_items=freight_items,
        shipper=shipper,
        error=error,
        so_tranid=session.get("so_tranid", ""),
        accessorials=session.get("accessorials", []),
    )


# ──────────────────────────────────────────────────────────────────────────────
# Step 2 — Rate results
# ──────────────────────────────────────────────────────────────────────────────

@app.route("/rates", methods=["GET", "POST"])
def rates():
    # GET: user navigated here directly — no rates to show
    if request.method == "GET":
        flash("Submit the quote form to get rates.", "info")
        return redirect(url_for("quote"))

    # POST: user selected a rate from the rates.html form
    rate_id = request.form.get("rate_id")
    if not rate_id:
        flash("Select a carrier rate.", "warning")
        return redirect(url_for("quote"))

    try:
        result = primus.save_rate(rate_id, session.get("accessorials", []))
        # Response: {"data": {"results": {"quoteNumber": "...", "quoteId": ...}}}
        _r = result.get("data", result)
        _r = _r.get("results", _r) if isinstance(_r, dict) else _r
        quote_number = (
            _r.get("quoteNumber") or _r.get("quote_number")
            or result.get("quoteNumber", "")
        )
        quote_cost = request.form.get("rate_total", 0)
        carrier_name = request.form.get("carrier_name", "")

        carrier_scac = request.form.get("carrier_scac", "")
        session["quote_number"] = str(quote_number)
        session["quote_cost"] = quote_cost
        session["carrier_name"] = carrier_name
        session["carrier_scac"] = carrier_scac
        session["selected_rate_id"] = rate_id

        # Write back to NetSuite
        so_id = session.get("so_internal_id")
        if so_id:
            try:
                ns.write_quote(so_id, quote_number, float(quote_cost or 0), carrier_name)
                flash("Quote saved to NetSuite SO.", "success")
            except Exception as e:
                flash(f"NS quote update skipped: {e}", "warning")

        return redirect(url_for("book"))
    except Exception as e:
        flash(f"Save rate error: {e}", "danger")
        return redirect(url_for("quote"))


# ──────────────────────────────────────────────────────────────────────────────
# Step 3 — Book
# ──────────────────────────────────────────────────────────────────────────────

@app.route("/book", methods=["GET", "POST"])
def book():
    if not session.get("quote_number"):
        flash("Select a rate first.", "warning")
        return redirect(url_for("rates"))

    if request.method == "POST":
        pickup_date      = request.form.get("pickup_date", "")
        pickup_time_from = request.form.get("pickup_time_from", "08:00")
        pickup_time_to   = request.form.get("pickup_time_to", "17:00")
        notes = request.form.get("notes", "")
        accessorials = request.form.getlist("accessorials[]") or session.get("accessorials", [])

        # Human-readable text for BOLInstructions (prints on the BOL)
        _ACC_TEXT = {
            "LFO": "LIFTGATE AT PICKUP",
            "LFD": "LIFTGATE AT DELIVERY",
            "RSO": "RESIDENTIAL PICKUP",
            "RSD": "RESIDENTIAL DELIVERY",
            "INO": "INSIDE PICKUP",
            "IND": "INSIDE DELIVERY",
            "NTD": "CALL BEFORE DELIVERY",
        }
        acc_text = " - ".join(_ACC_TEXT[a] for a in accessorials if a in _ACC_TEXT)
        bol_instructions = " - ".join(filter(None, [acc_text, notes]))
        special_instructions = bol_instructions  # kept for legacy compatibility
        log.debug("BOOK accessorials=%s acc_text=%r special_instructions=%r", accessorials, acc_text, special_instructions)
        origin = session.get("origin", cfg.SHIPPER)
        consignee = session.get("consignee", {})
        freight_items = session.get("freight_items", [])

        payload = {
            "quoteNumber": session.get("quote_number"),
            "pickupDate": pickup_date,
            "BOLInstructions": bol_instructions,
            "specialInstructions": special_instructions,
            "accessorialsList": accessorials,
            "shipper": {
                "name": origin.get("company", ""),
                "contact": origin.get("attn", ""),
                "address": origin.get("address", ""),
                "city": origin.get("city", ""),
                "state": origin.get("state", ""),
                "zipCode": origin.get("zip", ""),
                "country": origin.get("country", "US"),
                "phone": origin.get("phone", ""),
                "attn": origin.get("attn", ""),
            },
            "consignee": {
                "name": consignee.get("addressee", ""),
                "contact": consignee.get("addressee", ""),
                "address": consignee.get("address1", ""),
                "city": consignee.get("city", ""),
                "state": consignee.get("state", ""),
                "zipCode": consignee.get("zip", "").split("-")[0],
                "country": consignee.get("country", "US"),
                "phone": consignee.get("phone", "") or session.get("so_phone", ""),
                "referenceNumber": session.get("so_po_number", ""),
                "refNumber": session.get("so_po_number", ""),
            },
            "lineItems": freight_items,
            "referenceNumber": session.get("so_tranid", ""),
            "referenceNumber2": session.get("so_po_number", ""),
        }

        try:
            result = primus.book(payload)
            # Response: {"data": {"results": [{"BOLNmbr": "...", "BOLId": ..., "documents": [...]}]}}
            _res = result.get("data", result)
            _res_list = _res.get("results", []) if isinstance(_res, dict) else []
            first = _res_list[0] if _res_list else {}
            bol_number = first.get("BOLNmbr") or first.get("bolNumber") or ""
            bol_id        = str(first.get("BOLId") or first.get("bolId") or "")
            bol_documents = first.get("documents", [])

            # Dispatch the shipment now that we have a BOL ID
            dispatch_confirmation = ""
            dispatch_bill_to = ""
            try:
                if not bol_id:
                    raise ValueError("No BOLId returned from booking — cannot dispatch.")
                # SET pickup window on BOL (required before dispatch)
                bol_update = {
                    "pickupDate": pickup_date,
                    "pickupInformation": {
                        "date":     pickup_date,
                        "type":     "PO",
                        "timeFrom": pickup_time_from,
                        "timeTo":   pickup_time_to,
                    },
                }
                primus.update_bol(bol_id, bol_update)
                disp = primus.dispatch(bol_id)
                _d = disp.get("data", disp)
                _d = _d.get("results", _d) if isinstance(_d, dict) else _d
                if isinstance(_d, list):
                    _d = _d[0] if _d else {}
                dispatch_confirmation = (
                    _d.get("confirmation") or _d.get("confirmationNumber")
                    or _d.get("confirmNumber") or _d.get("dispatchConfirmation") or ""
                )
                _bt = _d.get("billTo") or _d.get("bill_to") or ""
                if isinstance(_bt, dict):
                    dispatch_bill_to = ", ".join(filter(None, [
                        _bt.get("name", ""),
                        _bt.get("address") or _bt.get("address1", ""),
                        _bt.get("city", ""),
                        _bt.get("state", ""),
                        _bt.get("zipcode") or _bt.get("zip", ""),
                    ]))
                else:
                    dispatch_bill_to = str(_bt)
                if dispatch_confirmation:
                    flash("Shipment dispatched successfully.", "success")
            except Exception as de:
                if "404" in str(de):
                    flash("Dispatch (V2) returned 404 — confirm endpoint with ShipPrimus support.", "warning")
                else:
                    flash(f"Dispatch warning: {de}", "warning")

            # Write to NetSuite
            so_id = session.get("so_internal_id")
            if so_id:
                try:
                    ns.write_bol(so_id, bol_number)
                    ns.write_dispatch(so_id, dispatch_confirmation or bol_number)
                    carrier_scac = session.get("carrier_scac", "")
                    if carrier_scac:
                        ns.patch_so(so_id, {"custbody_sps_carrieralphacode": carrier_scac})
                except Exception as e:
                    flash(f"NS update skipped: {e}", "warning")

            _save_recent({
                "bol_number": bol_number,
                "confirmation": dispatch_confirmation or bol_number,
                "carrier": session.get("carrier_name", ""),
                "so": session.get("so_tranid", ""),
                "cost": session.get("quote_cost", ""),
            })

            # ShipPrimus-hosted document URLs
            primus_bol_url = next((d["url"] for d in bol_documents if d.get("type") == "BOL"), "")
            primus_lbl_url = next((d["url"] for d in bol_documents if d.get("type") == "LBL"), "")

            return render_template(
                "confirm.html",
                bol_number=bol_number,
                dispatch_confirmation=dispatch_confirmation,
                dispatch_bill_to=dispatch_bill_to,
                carrier_name=session.get("carrier_name", ""),
                quote_number=session.get("quote_number", ""),
                so_tranid=session.get("so_tranid", ""),
                primus_bol_url=primus_bol_url,
                primus_lbl_url=primus_lbl_url,
                pickup_date=pickup_date,
            )
        except Exception as e:
            flash(f"Booking error: {e}", "danger")

    return render_template(
        "book.html",
        origin=session.get("origin", cfg.SHIPPER),
        consignee=session.get("consignee", {}),
        freight_items=session.get("freight_items", []),
        quote_number=session.get("quote_number", ""),
        carrier_name=session.get("carrier_name", ""),
        accessorials=session.get("accessorials", []),
    )


@app.route("/dispatch")
def dispatch():
    return redirect(url_for("book"))



# ──────────────────────────────────────────────────────────────────────────────
# Tracking
# ──────────────────────────────────────────────────────────────────────────────

@app.route("/track", methods=["GET", "POST"])
def track():
    tracking_result = None
    bol_input = ""
    error = None

    if request.method == "POST":
        bol_input = request.form.get("bol_number", "").strip()
        if bol_input:
            try:
                tracking_result = primus.track(bol_input)
            except Exception as e:
                if "404" in str(e):
                    error = f"Tracking not available for BOL '{bol_input}'. The shipment may not have tracking events yet, or tracking is not supported in this environment."
                else:
                    error = str(e)
        else:
            error = "Enter a BOL number."

    return render_template("track.html", result=tracking_result, bol_input=bol_input, error=error)


# ──────────────────────────────────────────────────────────────────────────────
# File downloads
# ──────────────────────────────────────────────────────────────────────────────

@app.route("/output/<filename>")
def download_file(filename):
    return send_from_directory(OUTPUT_DIR, filename, as_attachment=True)


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _parse_freight_form(form) -> list:
    items = []
    qtys = form.getlist("qty[]")
    descs = form.getlist("description[]")
    weights = form.getlist("weight[]")
    lengths = form.getlist("length[]")
    widths = form.getlist("width[]")
    heights = form.getlist("height[]")
    classes = form.getlist("freight_class[]")

    for i in range(len(qtys)):
        qty = qtys[i].strip()
        desc = descs[i].strip() if i < len(descs) else ""
        if not qty and not desc:
            continue
        items.append({
            "qty": int(qty) if qty else 1,
            "description": desc,
            "weight": float(weights[i]) if i < len(weights) and weights[i] else 0,
            "length": float(lengths[i]) if i < len(lengths) and lengths[i] else 0,
            "width": float(widths[i]) if i < len(widths) and widths[i] else 0,
            "height": float(heights[i]) if i < len(heights) and heights[i] else 0,
            "freight_class": classes[i] if i < len(classes) else "70",
        })
    return items


def _default_freight_row():
    return {"qty": 1, "description": "", "weight": 0, "length": 0, "width": 0, "height": 0, "freight_class": "70"}


_RECENT_FILE = os.path.join(os.path.dirname(__file__), "output", "recent.json")


def _load_recent() -> list:
    try:
        with open(_RECENT_FILE) as f:
            return json.load(f)
    except Exception:
        return []


def _save_recent(entry: dict):
    recent = _load_recent()
    recent.insert(0, entry)
    recent = recent[:20]  # keep last 20
    os.makedirs(os.path.dirname(_RECENT_FILE), exist_ok=True)
    with open(_RECENT_FILE, "w") as f:
        json.dump(recent, f, indent=2)


# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(debug=True, port=5000)
