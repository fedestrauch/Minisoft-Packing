"""NetSuite helpers: SO lookup (SuiteQL) + field write-back (REST Record API)."""
import re
import sys
import os
import requests
from requests_oauthlib import OAuth1

# Allow importing netsuite_client from parent dev folder
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import config as cfg


def _normalize_phone(phone: str) -> str:
    """
    Clean US phone for ShipPrimus: strip country code, keep extension.
    '+1 213-442-1463 ext. 88030' -> '2134421463 ext 88030'
    """
    if not phone:
        return ""
    # Capture extension separately
    ext_match = re.search(r'(?i)\s*(ext\.?|x)\s*(\d+)', phone)
    ext = ext_match.group(2) if ext_match else ""
    # Strip extension from main number
    base = re.sub(r'(?i)\s*(ext\.?|x)\s*\d+', '', phone)
    digits = re.sub(r'\D', '', base)
    # Strip leading country code if 11 digits starting with 1
    if len(digits) == 11 and digits[0] == '1':
        digits = digits[1:]
    clean = digits[:10]
    return f"{clean} ext {ext}" if ext else clean


def _account_url():
    return cfg.NS_ACCOUNT_ID.lower().replace("_", "-")


def _base_url():
    return f"https://{_account_url()}.suitetalk.api.netsuite.com/services/rest"


def _auth():
    return OAuth1(
        client_key=cfg.NS_CONSUMER_KEY,
        client_secret=cfg.NS_CONSUMER_SECRET,
        resource_owner_key=cfg.NS_TOKEN_ID,
        resource_owner_secret=cfg.NS_TOKEN_SECRET,
        realm=cfg.NS_ACCOUNT_ID,
        signature_method="HMAC-SHA256",
    )


def _ns_configured():
    return all([cfg.NS_ACCOUNT_ID, cfg.NS_CONSUMER_KEY, cfg.NS_CONSUMER_SECRET,
                cfg.NS_TOKEN_ID, cfg.NS_TOKEN_SECRET])


def _suiteql(sql: str, limit: int = 200) -> list:
    """Run a SuiteQL query, return list of row dicts."""
    resp = requests.post(
        f"{_base_url()}/query/v1/suiteql",
        auth=_auth(),
        headers={"Prefer": "transient"},
        params={"limit": limit, "offset": 0},
        json={"q": sql},
        timeout=30,
    )
    if not resp.ok:
        raise RuntimeError(f"SuiteQL {resp.status_code}: {resp.text[:300]}")
    return resp.json().get("items", [])


def get_so(tranid: str) -> dict:
    """
    Look up a Sales Order by transaction ID (e.g. 'SO12345').
    Returns dict with: id, tranid, customer, addressee, address1, city, state, zip, country.
    Uses SuiteQL to resolve the internal ID, then Record API for the shipping address
    (more reliable than the transactionshippingaddress SuiteQL table which only contains
    manually-overridden addresses).
    Raises RuntimeError if not found or NS not configured.
    """
    if not _ns_configured():
        raise RuntimeError("NetSuite credentials not configured. Add NS_* vars to .env")

    # Step 1: resolve internal ID + customer via SuiteQL
    sql = f"""
        SELECT t.id, t.tranid, BUILTIN.DF(t.entity) as customer
        FROM transaction t
        WHERE t.type = 'SalesOrd'
          AND t.tranid = '{tranid}'
          AND t.void = 'F'
    """
    url = f"{_base_url()}/query/v1/suiteql"
    resp = requests.post(
        url,
        auth=_auth(),
        headers={"Prefer": "transient"},
        params={"limit": 5, "offset": 0},
        json={"q": sql},
        timeout=30,
    )
    if not resp.ok:
        raise RuntimeError(f"SuiteQL error {resp.status_code}: {resp.text}")
    items = resp.json().get("items", [])
    if not items:
        raise RuntimeError(f"Sales Order '{tranid}' not found in NetSuite")

    so = items[0]
    internal_id = so["id"]

    # Step 2: fetch shippingAddress + custbody19 (phone) via Record API
    rec_resp = requests.get(
        f"{_base_url()}/record/v1/salesorder/{internal_id}",
        auth=_auth(),
        timeout=30,
    )
    so["phone"] = ""
    so["po_number"] = ""
    if rec_resp.ok:
        rec = rec_resp.json()
        so["phone"] = _normalize_phone(rec.get("custbody19", "") or "")
        so["po_number"] = rec.get("otherRefNum", "") or ""

    addr_resp = requests.get(
        f"{_base_url()}/record/v1/salesorder/{internal_id}/shippingAddress",
        auth=_auth(),
        timeout=30,
    )
    if addr_resp.ok:
        sa = addr_resp.json()
        country = sa.get("country", {})
        so["addressee"] = sa.get("addressee", "")
        so["address1"] = sa.get("addr1", "") or sa.get("addr2", "")
        so["city"] = sa.get("city", "")
        so["state"] = sa.get("state", "")
        so["zip"] = sa.get("zip", "")
        so["country"] = country.get("id", "US") if isinstance(country, dict) else str(country)
    else:
        for field in ("addressee", "address1", "city", "state", "zip", "country"):
            so.setdefault(field, "")

    return so


def get_so_freight(so_internal_id: str) -> list:
    """
    Return pre-filled freight rows for the quote form based on the SO's line items
    and their Packing Method - Minisoft records.

    Logic:
    - Kit items   → use the kit's own minisoft packing records (consolidated pallet spec)
    - InvtPart items that are components of a kit on this SO → skip (counted in kit packing)
    - Standalone InvtPart items (not a kit component on this SO) → include with their minisoft

    Each row: {qty, description, weight, length, width, height, freight_class, pallet_box}
    pallet_box: 'Pallet' (custrecord5 has a value) | 'Box' (custrecord5 is NULL)
    Qty = abs(SO line qty) — NS stores outbound lines as negative.
    """
    # Step 1: get all SO line items
    lines = _suiteql(f"""
        SELECT tl.item, ABS(tl.quantity) AS quantity, i.itemid, i.itemtype
        FROM transactionline tl
        JOIN item i ON i.id = tl.item
        WHERE tl.transaction = {so_internal_id}
          AND tl.item IS NOT NULL
          AND tl.quantity != 0
          AND tl.itemtype NOT IN ('Discount', 'Subtotal', 'Description', 'EndGroup', 'Group')
    """)

    if not lines:
        return []

    # Step 2: separate kits from InvtPart items
    kit_lines = [l for l in lines if l.get("itemtype") == "Kit"]

    # Collect component item IDs for all kits on this SO so we can skip them
    component_ids: set = set()
    for kit in kit_lines:
        comp_rows = _suiteql(f"SELECT im.item FROM itemmember im WHERE im.parentitem = {kit['item']}")
        component_ids.update(r["item"] for r in comp_rows)

    standalone_lines = [
        l for l in lines
        if l.get("itemtype") == "InvtPart" and l["item"] not in component_ids
    ]

    shipping_lines = kit_lines + standalone_lines
    if not shipping_lines:
        shipping_lines = lines  # fallback: use everything

    # Step 3: for each shipping item, fetch its minisoft packing records
    freight_rows = []
    for line in shipping_lines:
        item_id = line["item"]
        qty = max(1, int(float(line.get("quantity") or 1)))
        item_name = line.get("itemid", "")

        ms_rows = _suiteql(f"""
            SELECT ms.custrecord4, ms.custrecord5,
                   ms.custrecord6, ms.custrecord7, ms.custrecord8, ms.custrecord9
            FROM customrecordpack_details_minisoft ms
            WHERE ms.custrecord2 = {item_id}
              AND ms.isinactive = 'F'
              AND ms.custrecord5 IS NOT NULL
        """)

        if ms_rows:
            for ms in ms_rows:
                # custrecord5 present → Pallet; absent/NULL → Box
                num_boxes = ms.get("custrecord5")
                pallet_box = "Pallet" if num_boxes else "Box"
                freight_rows.append({
                    "qty": qty,
                    "description": "Furniture",
                    "weight": float(ms.get("custrecord6") or 0),
                    "length": float(ms.get("custrecord7") or 0),
                    "width":  float(ms.get("custrecord8") or 0),
                    "height": float(ms.get("custrecord9") or 0),
                    "freight_class": "70",
                    "pallet_box": pallet_box,
                })
        else:
            # No minisoft data — blank row so user can fill in manually
            freight_rows.append({
                "qty": qty,
                "description": "Furniture",
                "weight": 0, "length": 0, "width": 0, "height": 0,
                "freight_class": "70",
                "pallet_box": "",
            })

    return freight_rows


def patch_so(internal_id: str, fields: dict):
    """
    Write arbitrary fields back to a Sales Order via REST Record API PATCH.
    fields: {fieldId: value, ...}
    """
    if not _ns_configured():
        return  # silently skip if NS not configured
    url = f"{_base_url()}/record/v1/salesOrder/{internal_id}"
    resp = requests.patch(url, auth=_auth(), json=fields, timeout=30)
    if not resp.ok:
        raise RuntimeError(f"NS PATCH error {resp.status_code}: {resp.text}")


def write_quote(internal_id: str, quote_number: str, quote_cost: float, carrier: str, broker: str = ""):
    broker_part = f" | Broker: {broker}" if broker else ""
    memo = f"Freight Quote: {quote_number} | Carrier: {carrier}{broker_part} | Amount: ${quote_cost:.2f}"
    patch_so(internal_id, {
        cfg.NS_FIELD_QUOTE_NUMBER: quote_number,
        cfg.NS_FIELD_QUOTE_COST: quote_cost,
        cfg.NS_FIELD_CARRIER: carrier,
        "memo": memo,
    })


def write_bol(internal_id: str, bol_number: str, carrier: str = "", broker: str = "", pro_number: str = ""):
    fields = {cfg.NS_FIELD_BOL_NUMBER: bol_number}
    if pro_number:
        fields["custbody_tracking_number"] = pro_number
    if carrier or broker:
        parts = []
        if broker:
            parts.append(f"Broker: {broker}")
        if carrier:
            parts.append(f"Carrier: {carrier}")
        parts.append(f"BOL#: {bol_number}")
        if pro_number:
            parts.append(f"PRO#: {pro_number}")
        fields["memo"] = " | ".join(parts)
    patch_so(internal_id, fields)


def write_dispatch(internal_id: str, confirmation: str):
    patch_so(internal_id, {
        cfg.NS_FIELD_DISPATCH_CONFIRM: confirmation,
    })
