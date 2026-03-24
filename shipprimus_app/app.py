"""
ShipPrimus Freight Quoting App - Flask routes
"""
import json
import logging
import os

# File logger - writes to output/app.log
_log_path = os.path.join(os.path.dirname(__file__), "output", "app.log")
os.makedirs(os.path.dirname(_log_path), exist_ok=True)
logging.basicConfig(
    filename=_log_path,
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("shipprimus")

from flask import (  # noqa: E402
    Flask,
    flash,
    redirect,
    render_template,
    request,
    send_from_directory,
    session,
    url_for,
)
from flask_session import Session  # noqa: E402

import config as cfg  # noqa: E402
import netsuite as ns  # noqa: E402
from arcbest_client import client as arcbest  # noqa: E402
from bol_generator import generate_label  # noqa: E402
from ceva_client import client as ceva  # noqa: E402
from primus_client import client as primus  # noqa: E402

app = Flask(__name__)
app.secret_key = cfg.FLASK_SECRET_KEY

# Server-side filesystem sessions - avoids the 4 KB cookie limit
_SESSION_DIR = os.path.join(os.path.dirname(__file__), ".sessions")
os.makedirs(_SESSION_DIR, exist_ok=True)
app.config["SESSION_TYPE"] = "filesystem"
app.config["SESSION_FILE_DIR"] = _SESSION_DIR
app.config["SESSION_PERMANENT"] = False
Session(app)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
ALL_PROVIDER_OPTIONS = [
    {"id": "primus", "label": "GLB Solutions", "description": "Multi-carrier rating, booking, and documents."},
    {"id": "arcbest", "label": "ArcBest", "description": "Direct ArcBest XML rating and pickup requests."},
    {"id": "ceva", "label": "CEVA Logistics", "description": "CEVA Matrix LTL - white glove & standard delivery."},
]
PROVIDER_OPTIONS = [
    option for option in ALL_PROVIDER_OPTIONS
    if option["id"] != "ceva" or cfg.CEVA_ENABLED
]
ENABLED_PROVIDER_IDS = {option["id"] for option in PROVIDER_OPTIONS}
PROVIDER_OPTIONS = [
    {"id": "primus", "label": "GLB Solutions", "description": "Multi-carrier rating, booking, and documents."},
    {"id": "arcbest", "label": "ArcBest", "description": "Direct ArcBest XML rating and pickup requests."},
    {"id": "ceva", "label": "CEVA Logistics", "description": "CEVA Matrix LTL — white glove & standard delivery."},
]


PROVIDER_OPTIONS = [
    option for option in ALL_PROVIDER_OPTIONS
    if option["id"] != "ceva" or cfg.CEVA_ENABLED
]
ENABLED_PROVIDER_IDS = {option["id"] for option in PROVIDER_OPTIONS}


def _normalize_provider(value: str | None) -> str:
    provider = (value or "").strip().lower()
    if provider in ENABLED_PROVIDER_IDS:
        return provider
    fallback = cfg.FREIGHT_PROVIDER if cfg.FREIGHT_PROVIDER in ENABLED_PROVIDER_IDS else "primus"
    return fallback


def _provider_name(provider: str) -> str:
    if provider == "arcbest":
        return "ArcBest"
    if provider == "ceva":
        return "CEVA Logistics"
    return "GLB Solutions"


def _get_provider_client(provider: str):
    if provider == "arcbest":
        return arcbest
    if provider == "ceva":
        return ceva
    return primus


def _fetch_rate_groups(origin: dict, consignee: dict, freight_items: list, accessorials: list) -> tuple[list[dict], list[str]]:
    groups: list[dict] = []
    errors: list[str] = []
    for option in PROVIDER_OPTIONS:
        provider = option["id"]
        try:
            rates = _get_provider_client(provider).get_rates(origin, consignee, freight_items, accessorials)
            groups.append(
                {
                    "id": provider,
                    "label": option["label"],
                    "description": option["description"],
                    "rates": rates or [],
                }
            )
        except Exception as exc:
            errors.append(f'{option["label"]}: {exc}')
            groups.append(
                {
                    "id": provider,
                    "label": option["label"],
                    "description": option["description"],
                    "rates": [],
                }
            )
    return groups, errors


def _get_active_provider() -> str:
    requested = request.args.get("provider") if request.method == "GET" else request.form.get("freight_provider")
    provider = _normalize_provider(requested or session.get("freight_provider") or cfg.FREIGHT_PROVIDER)
    session["freight_provider"] = provider
    return provider


@app.context_processor
def inject_provider_context():
    provider = _normalize_provider(session.get("freight_provider", cfg.FREIGHT_PROVIDER))
    return {
        "active_provider": provider,
        "active_provider_name": _provider_name(provider),
        "provider_options": PROVIDER_OPTIONS,
    }


@app.route("/provider/<provider>")
def set_provider(provider: str):
    session["freight_provider"] = _normalize_provider(provider)
    next_endpoint = request.args.get("next", "quote")
    if next_endpoint not in {"index", "quote", "track"}:
        next_endpoint = "quote"
    return redirect(url_for(next_endpoint))


@app.route("/queue", methods=["POST"])
def queue_start():
    """Accept a list of SO#s, store in session, auto-load the first one."""
    raw = request.form.get("so_numbers", "").replace(",", "\n")
    sos = [s.strip().upper() for s in raw.splitlines() if s.strip()]
    if not sos:
        flash("Enter at least one SO number.", "warning")
        return redirect(url_for("index"))
    session["queue"] = sos[1:]
    session["queue_skipped"] = []
    return redirect(url_for("quote") + f"?so={sos[0]}")


@app.route("/queue/next")
def queue_next():
    """Advance to the next SO in the queue, or return to dashboard when done."""
    queue = list(session.get("queue", []))
    if not queue:
        skipped = session.pop("queue_skipped", [])
        session.pop("queue", None)
        if skipped:
            flash(f"Queue complete. Skipped {len(skipped)} SO(s): {', '.join(skipped)}", "warning")
        else:
            flash("All shipments in queue have been processed.", "success")
        return redirect(url_for("index"))
    next_so = queue.pop(0)
    session["queue"] = queue
    return redirect(url_for("quote") + f"?so={next_so}")


@app.route("/queue/clear")
def queue_clear():
    session.pop("queue", None)
    session.pop("queue_skipped", None)
    flash("Queue cleared.", "info")
    return redirect(url_for("index"))


@app.route("/")
def index():
    recent = _load_recent()
    return render_template(
        "index.html",
        recent=recent,
        provider=_get_active_provider(),
        provider_switch_target="index",
        queue=session.get("queue", []),
        queue_skipped=session.get("queue_skipped", []),
    )


@app.route("/quote", methods=["GET", "POST"])
def quote():
    so_data = None
    error = None
    provider = _get_active_provider()

    if request.method == "POST":
        action = request.form.get("action")
        provider = _normalize_provider(request.form.get("freight_provider", provider))
        session["freight_provider"] = provider

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

                    session["consignee"] = {
                        "addressee": so_data.get("addressee", ""),
                        "address1": so_data.get("address1", ""),
                        "city": so_data.get("city", ""),
                        "state": so_data.get("state", ""),
                        "zip": so_data.get("zip", ""),
                        "country": so_data.get("country", "US"),
                        "phone": so_data.get("phone", ""),
                    }

                    try:
                        freight_from_ns = ns.get_so_freight(so_id)
                        if freight_from_ns:
                            session["freight_items"] = freight_from_ns
                    except Exception as exc:
                        flash(f"Packing data unavailable: {exc}", "warning")
                except Exception as exc:
                    error = str(exc)

        elif action == "get_rates":
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

            rate_groups, rate_errors = _fetch_rate_groups(origin, consignee, freight_items, accessorials)
            if any(group["rates"] for group in rate_groups):
                return render_template(
                    "rates.html",
                    rate_groups=rate_groups,
                    rate_errors=rate_errors,
                    consignee=consignee,
                    shipper=cfg.SHIPPER,
                    accessorials=accessorials,
                    provider_switch_target="quote",
                )
            for message in rate_errors or ["No rates returned from any provider."]:
                flash(f"Rating error: {message}", "danger")

    # Auto-lookup when redirected from queue (/quote?so=SOXXXXX)
    if request.method == "GET":
        auto_so = request.args.get("so", "").strip()
        if auto_so:
            try:
                so_data = ns.get_so(auto_so)
                so_id = str(so_data.get("id", ""))
                session["so_internal_id"] = so_id
                session["so_tranid"] = auto_so
                session["so_phone"] = so_data.get("phone", "")
                session["so_po_number"] = so_data.get("po_number", "")
                session["consignee"] = {
                    "addressee": so_data.get("addressee", ""),
                    "address1": so_data.get("address1", ""),
                    "city": so_data.get("city", ""),
                    "state": so_data.get("state", ""),
                    "zip": so_data.get("zip", ""),
                    "country": so_data.get("country", "US"),
                    "phone": so_data.get("phone", ""),
                }
                try:
                    freight_from_ns = ns.get_so_freight(so_id)
                    if freight_from_ns:
                        session["freight_items"] = freight_from_ns
                except Exception as exc:
                    flash(f"Packing data unavailable: {exc}", "warning")
            except Exception as exc:
                skipped = list(session.get("queue_skipped", []))
                skipped.append(auto_so)
                session["queue_skipped"] = skipped
                flash(f"SO {auto_so} not found — skipping. ({exc})", "warning")
                return redirect(url_for("queue_next"))

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
        provider=provider,
        provider_name=_provider_name(provider),
        provider_switch_target="quote",
    )


@app.route("/rates", methods=["GET", "POST"])
def rates():
    if request.method == "GET":
        flash("Submit the quote form to get rates.", "info")
        return redirect(url_for("quote"))

    rate_id = request.form.get("rate_id")
    if not rate_id:
        flash("Select a carrier rate.", "warning")
        return redirect(url_for("quote"))

    try:
        provider = _normalize_provider(
            request.form.get("provider") or request.form.get(f"provider_{rate_id}")
        )
        session["freight_provider"] = provider
        client = _get_provider_client(provider)
        result = client.save_rate(rate_id, session.get("accessorials", []))
        _r = result.get("data", result)
        _r = _r.get("results", _r) if isinstance(_r, dict) else _r
        quote_number = _r.get("quoteNumber") or _r.get("quote_number") or result.get("quoteNumber", "")
        quote_cost = (
            request.form.get("rate_total")
            or request.form.get(f"rate_total_{rate_id}")
            or 0
        )
        carrier_name = (
            request.form.get("carrier_name")
            or request.form.get(f"carrier_name_{rate_id}")
            or ""
        )
        carrier_scac = (
            request.form.get("carrier_scac")
            or request.form.get(f"carrier_scac_{rate_id}")
            or ""
        )

        session["quote_number"] = str(quote_number)
        session["quote_cost"] = quote_cost
        session["carrier_name"] = carrier_name
        session["carrier_scac"] = carrier_scac
        session["selected_rate_id"] = rate_id
        # CEVA: store service level so book() can include it in the order XML
        ceva_service = _r.get("serviceLevel", "")
        if ceva_service:
            session["ceva_service_level"] = ceva_service

        so_id = session.get("so_internal_id")
        if so_id:
            try:
                ns.write_quote(so_id, quote_number, float(quote_cost or 0), carrier_name,
                               broker=_provider_name(provider))
                flash("Quote saved to NetSuite SO.", "success")
            except Exception as exc:
                flash(f"NS quote update skipped: {exc}", "warning")

        return redirect(url_for("book"))
    except Exception as exc:
        flash(f"Save rate error: {exc}", "danger")
        return redirect(url_for("quote"))


@app.route("/book", methods=["GET", "POST"])
def book():
    if not session.get("quote_number"):
        flash("Select a rate first.", "warning")
        return redirect(url_for("rates"))

    provider = _normalize_provider(session.get("freight_provider", cfg.FREIGHT_PROVIDER))

    if request.method == "POST":
        pickup_date = request.form.get("pickup_date", "")
        pickup_time_from = request.form.get("pickup_time_from", "08:00")
        pickup_time_to = request.form.get("pickup_time_to", "17:00")
        notes = request.form.get("notes", "")
        accessorials = request.form.getlist("accessorials[]") or session.get("accessorials", [])
        origin = session.get("origin", cfg.SHIPPER)
        consignee = session.get("consignee", {})
        freight_items = session.get("freight_items", [])

        _ACC_TEXT = {
            "LFO": "LIFTGATE AT PICKUP",
            "LFD": "LIFTGATE AT DELIVERY",
            "RSO": "RESIDENTIAL PICKUP",
            "RSD": "RESIDENTIAL DELIVERY",
            "INO": "INSIDE PICKUP",
            "IND": "INSIDE DELIVERY",
            "APD": "APPOINTMENT REQUIRED AT DESTINATION",
            "NTD": "CALL BEFORE DELIVERY",
        }
        acc_text = " - ".join(_ACC_TEXT[a] for a in accessorials if a in _ACC_TEXT)
        delivery_call_text = ""
        delivery_phone = consignee.get("phone", "") or session.get("so_phone", "")
        if delivery_phone:
            delivery_call_text = f"PLEASE FOR DELIVERY CALL TO COMPLETE PHONE NUMBER {delivery_phone}"
        bol_instructions = " - ".join(filter(None, [acc_text, notes, delivery_call_text]))
        special_instructions = bol_instructions
        log.debug("BOOK accessorials=%s acc_text=%r special_instructions=%r", accessorials, acc_text, special_instructions)

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
            client = _get_provider_client(provider)
            if provider == "ceva":
                payload["_ceva_service_level"] = session.get("ceva_service_level", "IDD")
            result = client.book(payload)
            _res = result.get("data", result)
            _res_list = _res.get("results", []) if isinstance(_res, dict) else []
            first = _res_list[0] if _res_list else {}
            bol_number = first.get("BOLNmbr") or first.get("bolNumber") or ""
            # PRO# = carrier-assigned tracking number.
            # ArcBest: BOLNmbr IS the PRO# (assigned at booking).
            # GLB/ShipPrimus: PRO# is not returned at booking (assigned by carrier at pickup).
            #   We check all known field names; if none present, fall back to BOL#.
            pro_number = (
                first.get("proNumber")
                or first.get("pro_number")
                or first.get("carrierPro")
                or first.get("carrierPRO")
                or first.get("pro")
                or first.get("trackingNumber")
                or first.get("tracking_number")
                or ""
            )
            if provider == "arcbest":
                # ArcBest BOLNmbr is the PRO# — always use it
                tracking_number = bol_number
            else:
                # GLB: use PRO# if returned, otherwise BOL#
                tracking_number = pro_number or bol_number
            bol_id = str(first.get("BOLId") or first.get("bolId") or "")
            bol_documents = first.get("documents", [])

            dispatch_confirmation = ""
            dispatch_bill_to = ""
            try:
                if provider == "arcbest":
                    dispatch_confirmation = first.get("confirmation") or bol_number
                elif not bol_id:
                    raise ValueError("No BOLId returned from booking - cannot dispatch.")
                else:
                    bol_update = {
                        "pickupDate": pickup_date,
                        "pickupInformation": {
                            "date": pickup_date,
                            "type": "PO",
                            "timeFrom": pickup_time_from,
                            "timeTo": pickup_time_to,
                        },
                    }
                    client.update_bol(bol_id, bol_update)
                    disp = client.dispatch(bol_id)
                    _d = disp.get("data", disp)
                    _d = _d.get("results", _d) if isinstance(_d, dict) else _d
                    if isinstance(_d, list):
                        _d = _d[0] if _d else {}
                    dispatch_confirmation = (
                        _d.get("confirmation")
                        or _d.get("confirmationNumber")
                        or _d.get("confirmNumber")
                        or _d.get("dispatchConfirmation")
                        or ""
                    )
                    _bt = _d.get("billTo") or _d.get("bill_to") or ""
                    if isinstance(_bt, dict):
                        dispatch_bill_to = ", ".join(
                            filter(
                                None,
                                [
                                    _bt.get("name", ""),
                                    _bt.get("address") or _bt.get("address1", ""),
                                    _bt.get("city", ""),
                                    _bt.get("state", ""),
                                    _bt.get("zipcode") or _bt.get("zip", ""),
                                ],
                            )
                        )
                    else:
                        dispatch_bill_to = str(_bt)
                if dispatch_confirmation:
                    flash("Shipment dispatched successfully.", "success")
            except Exception as exc:
                if "404" in str(exc):
                    flash("Dispatch (V2) returned 404 - confirm endpoint with ShipPrimus support.", "warning")
                else:
                    flash(f"Dispatch warning: {exc}", "warning")

            # Fetch carrier PRO# post-dispatch.
            # Direct ABF (arcbest provider): BOLNmbr returned by pickupxml.asp IS the PRO#.
            # GLB Solutions (primus provider): all carriers (ABF, FedEx Freight, XPO, ODW, etc.)
            #   — PRO is in vendor.PRO on the booking record, available after dispatch.
            pro_number = ""
            if provider == "arcbest":
                pro_number = bol_number
            elif provider == "primus" and bol_id:
                try:
                    pro_number = client.get_pro(bol_id)
                except Exception as exc:
                    log.warning("Could not fetch PRO# for BOL %s: %s", bol_id, exc)

            so_id = session.get("so_internal_id")
            if so_id:
                try:
                    broker_name = _provider_name(provider)
                    carrier_name_session = session.get("carrier_name", "")
                    ns.write_bol(so_id, bol_number,
                                 carrier=carrier_name_session, broker=broker_name,
                                 pro_number=pro_number)
                    ns.write_dispatch(so_id, dispatch_confirmation or bol_number)
                    carrier_scac = session.get("carrier_scac", "")
                    if carrier_scac:
                        ns.patch_so(so_id, {"custbody_sps_carrieralphacode": carrier_scac})
                except Exception as exc:
                    flash(f"NS update skipped: {exc}", "warning")

            primus_bol_url = next((d["url"] for d in bol_documents if d.get("type") == "BOL"), "")
            primus_lbl_url = next((d["url"] for d in bol_documents if d.get("type") in {"LBL", "PROLBL"}), "")
            if not primus_lbl_url and bol_number:
                try:
                    local_label_path = generate_label(
                        bol_number,
                        origin,
                        consignee,
                        pickup_date=pickup_date,
                        po_number=session.get("so_po_number", ""),
                        so_number=session.get("so_tranid", ""),
                        freight_items=freight_items,
                        label_number=1,
                        total_labels=1,
                    )
                    primus_lbl_url = url_for("download_file", filename=os.path.basename(local_label_path))
                except Exception as exc:
                    flash(f"Label generation skipped: {exc}", "warning")

            _save_recent(
                {
                    "bol_number": bol_number,
                    "tracking_number": tracking_number or bol_number,
                    "confirmation": dispatch_confirmation or bol_number,
                    "carrier": session.get("carrier_name", ""),
                    "so": session.get("so_tranid", ""),
                    "cost": session.get("quote_cost", ""),
                    "provider": provider,
                    "bol_url": primus_bol_url,
                    "label_url": primus_lbl_url,
                }
            )

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
                provider=provider,
                provider_name=_provider_name(provider),
                provider_switch_target="quote",
                queue_remaining=len(session.get("queue", [])),
            )
        except Exception as exc:
            flash(f"{_provider_name(session.get('freight_provider', cfg.FREIGHT_PROVIDER))} booking error: {exc}", "danger")

    return render_template(
        "book.html",
        origin=session.get("origin", cfg.SHIPPER),
        consignee=session.get("consignee", {}),
        freight_items=session.get("freight_items", []),
        quote_number=session.get("quote_number", ""),
        carrier_name=session.get("carrier_name", ""),
        accessorials=session.get("accessorials", []),
        provider=provider,
        provider_name=_provider_name(provider),
        provider_switch_target="quote",
    )


@app.route("/dispatch")
def dispatch():
    return redirect(url_for("book"))


@app.route("/track", methods=["GET", "POST"])
def track():
    tracking_result = None
    bol_input = ""
    error = None
    provider = _get_active_provider()

    if request.method == "POST":
        bol_input = request.form.get("bol_number", "").strip()
        if bol_input:
            try:
                client = _get_provider_client(provider)
                tracking_result = client.track(bol_input)
            except Exception as exc:
                if "404" in str(exc):
                    error = (
                        f"Tracking not available for BOL '{bol_input}'. "
                        "The shipment may not have tracking events yet, or tracking is not supported in this environment."
                    )
                else:
                    error = str(exc)
        else:
            error = "Enter a BOL number."

    return render_template(
        "track.html",
        result=tracking_result,
        bol_input=bol_input,
        error=error,
        provider=provider,
        provider_name=_provider_name(provider),
        provider_switch_target="track",
    )


@app.route("/output/<filename>")
def download_file(filename):
    return send_from_directory(OUTPUT_DIR, filename, as_attachment=True)


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
        items.append(
            {
                "qty": int(qty) if qty else 1,
                "description": desc,
                "weight": float(weights[i]) if i < len(weights) and weights[i] else 0,
                "length": float(lengths[i]) if i < len(lengths) and lengths[i] else 0,
                "width": float(widths[i]) if i < len(widths) and widths[i] else 0,
                "height": float(heights[i]) if i < len(heights) and heights[i] else 0,
                "freight_class": classes[i] if i < len(classes) else "70",
            }
        )
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
    recent = recent[:20]
    os.makedirs(os.path.dirname(_RECENT_FILE), exist_ok=True)
    with open(_RECENT_FILE, "w") as f:
        json.dump(recent, f, indent=2)


if __name__ == "__main__":
    app.run(debug=True, port=5000)
