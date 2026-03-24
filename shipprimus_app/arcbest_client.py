"""ArcBest (ABF Freight) XML client.

This client targets the documented legacy ArcBest XML endpoints under
`https://www.abfs.com/xml`, using the exact parameter vocabulary from the live
ArcBest API docs:

- rates: `aquotexml.asp`
- pickups: `pickupxml.asp`

Responses are normalized into the same general shape the Flask app already
expects from the ShipPrimus flow.
"""

from __future__ import annotations

import datetime as dt
import re
import time
import xml.etree.ElementTree as ET
from typing import Any

import requests

from config import (
    ARCBEST_API_ID,
    ARCBEST_BASE_URL,
    ARCBEST_BOOK_PATH,
    ARCBEST_RATE_PATH,
    ARCBEST_TEST_MODE,
    ARCBEST_USERNAME,
)

ABF_SCAC = "ABFS"

ACCESSORIAL_LABELS = {
    "LGPU": "Liftgate Pickup",
    "LGDL": "Liftgate Delivery",
    "RDP": "Residential Pickup",
    "RDD": "Residential Delivery",
    "INPU": "Inside Pickup",
    "INDL": "Inside Delivery",
    "APPT": "Appointment Delivery",
    "NTFY": "Notification / Call Before Delivery",
}

PRIMUS_TO_ABF_ACC = {
    "LFO": "LGPU",
    "LFD": "LGDL",
    "RSO": "RDP",
    "RSD": "RDD",
    "INO": "INPU",
    "IND": "INDL",
    "APD": "APPT",
    "NTD": "NTFY",
}

RATE_ACCESSORIAL_FLAGS = {
    "LGPU": "Acc_GRD_PU",
    "LGDL": "Acc_GRD_DEL",
    "RDP": "Acc_RPU",
    "RDD": "Acc_RDEL",
    "INPU": "Acc_IPU",
    "INDL": "Acc_IDEL",
    "NTFY": "Acc_ARR",
}

PICKUP_ACCESSORIAL_FLAGS = {
    "LGPU": "Acc_GRD_PU",
    "LGDL": "Acc_GRD_DEL",
    "RDP": "Acc_RPU",
    "RDD": "Acc_RDEL",
    "INPU": "Acc_IPU",
    "INDL": "Acc_IDEL",
    "NTFY": "Acc_ARR",
}

_ERROR_HELP = {
    "4": "Multiple cities found for shipper zip. Pass city name to disambiguate.",
    "23": "Multiple zip codes found for shipper/consignee city.",
    "40": (
        "ABF quote system could not complete the rate automatically. "
        "The account likely has no negotiated tariff for automated quoting. "
        "Contact ABF customer service to activate API pricing."
    ),
    "53": "Shipper or consignee city and state required.",
    "57": "Invalid requester affiliation. Pass SHIPAFF=Y, CONSAFF=Y, or TPBAFF=Y.",
}


def _text(el: ET.Element | None, tag: str, default: str = "") -> str:
    if el is None:
        return default
    child = el.find(tag)
    return (child.text or default).strip() if child is not None and child.text else default


def _all_text(el: ET.Element | None, *tags: str) -> str:
    for tag in tags:
        value = _text(el, tag)
        if value:
            return value
    return ""


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(str(value).replace("$", "").replace(",", "").strip())
    except (TypeError, ValueError):
        return default


def _zip5(value: str) -> str:
    return str(value or "").split("-")[0].strip()


def _phone10(value: Any) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits[:10]


def _phone_ext(value: Any) -> str:
    match = re.search(r"(?i)\b(?:ext\.?|x)\s*(\d+)", str(value or ""))
    return match.group(1)[:5] if match else ""


def _document_url(value: str) -> str:
    path = (value or "").strip()
    if not path:
        return ""
    if path.startswith("http://") or path.startswith("https://"):
        return path
    if path.startswith("/"):
        return f"https://www.abfs.com{path}"
    return f"https://www.abfs.com/{path.lstrip('/')}"


def _normalize_scac(value: Any) -> str:
    return str(value or "").strip().upper()


def _build_location_params(prefix: str, city: str, state: str, zip_code: str) -> dict[str, str]:
    return {
        f"{prefix}City": (city or "").upper(),
        f"{prefix}State": (state or "").upper(),
        f"{prefix}Zip": _zip5(zip_code),
    }


def _map_accessorials(codes: list[str] | None) -> list[str]:
    result: list[str] = []
    for code in codes or []:
        value = PRIMUS_TO_ABF_ACC.get((code or "").strip().upper(), (code or "").strip().upper())
        if value and value not in result:
            result.append(value)
    return result


def _parse_date(value: str) -> str:
    if not value:
        return ""
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return dt.datetime.strptime(value, fmt).strftime("%m/%d/%Y")
        except ValueError:
            continue
    return value


def _unit_type(value: Any) -> str:
    unit = str(value or "").strip().upper()
    return unit if unit else "PLT"


def _build_accessorial_params(
    flags_map: dict[str, str],
    codes: list[str] | None,
) -> dict[str, str]:
    params: dict[str, str] = {}
    for code in _map_accessorials(codes):
        flag = flags_map.get(code)
        if flag:
            params[flag] = "Y"
    return params


class ArcBestClient:
    def __init__(self) -> None:
        self.base_url = ARCBEST_BASE_URL.rstrip("/")
        self.api_id = ARCBEST_API_ID

    def _get(self, endpoint: str, params: dict[str, Any], timeout: int = 45) -> ET.Element:
        if not self.api_id:
            raise RuntimeError("Missing ARCBEST_API_ID in shipprimus_app/.env")

        payload = dict(params)
        payload.setdefault("ID", self.api_id)
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        last_exc: Exception | None = None
        response = None
        for attempt in range(3):
            try:
                response = requests.get(url, params=payload, timeout=timeout)
                last_exc = None
                break
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
                last_exc = exc
                if attempt == 2:
                    break
                time.sleep(1.0 * (attempt + 1))
        if last_exc is not None:
            raise RuntimeError(
                f"ABF transport error from {endpoint} after 3 attempts: {last_exc}"
            ) from last_exc
        if response is None:
            raise RuntimeError(f"ABF transport error from {endpoint}: no response received")
        if not response.ok:
            raise RuntimeError(
                f"ABF HTTP {response.status_code} from {endpoint}: {response.text[:300]}"
            )
        try:
            root = ET.fromstring(response.content)
        except ET.ParseError as exc:
            raise RuntimeError(f"ABF returned non-XML: {response.text[:300]}") from exc
        self._check_errors(root)
        return root

    def _check_errors(self, root: ET.Element) -> None:
        num_errors = root.find("NUMERRORS")
        if num_errors is None:
            return
        try:
            count = int((num_errors.text or "0").strip())
        except ValueError:
            return
        if count == 0:
            return

        messages: list[str] = []
        for err_el in root.findall("ERROR"):
            for code_el in err_el.findall("ERRORCODE"):
                code = (code_el.text or "").strip()
                help_text = _ERROR_HELP.get(code)
                if help_text:
                    messages.append(help_text)
            for msg_el in err_el.findall("ERRORMESSAGE"):
                text = (msg_el.text or "").strip()
                if text:
                    messages.append(text)
        if not messages:
            messages.append("Unknown ABF error")
        unique = list(dict.fromkeys(messages))
        raise RuntimeError(" | ".join(unique))

    def get_rates(
        self,
        origin: dict[str, Any],
        destination: dict[str, Any],
        freight_info: list[dict[str, Any]],
        accessorials: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        ship_date = dt.date.today()
        params: dict[str, Any] = {
            "DL": "2",
            "ShipAff": "Y",
            "ShipCountry": origin.get("country", "US"),
            "ConsCountry": destination.get("country", "US"),
            "ShipMonth": ship_date.strftime("%m"),
            "ShipDay": ship_date.strftime("%d"),
            "ShipYear": ship_date.strftime("%Y"),
            "FrtLWHType": "IN",
        }
        params.update(
            _build_location_params("Ship", origin.get("city", ""), origin.get("state", ""), origin.get("zip", ""))
        )
        params.update(
            _build_location_params(
                "Cons",
                destination.get("city", ""),
                destination.get("state", ""),
                destination.get("zip", ""),
            )
        )
        params.update(_build_accessorial_params(RATE_ACCESSORIAL_FLAGS, accessorials))

        for idx, item in enumerate(freight_info, start=1):
            qty = int(item.get("qty") or 1)
            params[f"UnitNo{idx}"] = qty
            params[f"UnitType{idx}"] = _unit_type(item.get("unit_type"))
            params[f"Wgt{idx}"] = int(round(_safe_float(item.get("weight"), 0.0) * qty))
            params[f"Class{idx}"] = str(item.get("freight_class") or "70")
            params[f"Desc{idx}"] = str(item.get("description") or f"Freight line {idx}")[:35]
            length = _safe_float(item.get("length"), 0.0)
            width = _safe_float(item.get("width"), 0.0)
            height = _safe_float(item.get("height"), 0.0)
            if length:
                params[f"FrtLng{idx}"] = int(round(length))
            if width:
                params[f"FrtWdth{idx}"] = int(round(width))
            if height:
                params[f"FrtHght{idx}"] = int(round(height))
            nmfc_item = str(item.get("nmfc_item") or "").strip()
            nmfc_sub = str(item.get("nmfc_sub") or "").strip()
            if nmfc_item:
                params[f"NMFCItem{idx}"] = nmfc_item
            if nmfc_sub:
                params[f"NMFCSub{idx}"] = nmfc_sub

        root = self._get(ARCBEST_RATE_PATH, params, timeout=60)
        rates = self._parse_rates(root)
        if not rates:
            raise RuntimeError("ABF returned no quote rows.")
        return rates

    def _parse_rates(self, root: ET.Element) -> list[dict[str, Any]]:
        rates: list[dict[str, Any]] = []
        candidates = root.findall(".//RATEQUOTE") + root.findall(".//QUOTE") + root.findall(".//RATE")

        if not candidates:
            total = self._extract_total(root)
            if total is not None:
                candidates = [root]

        for idx, node in enumerate(candidates, start=1):
            total = self._extract_total(node)
            if total is None:
                continue
            carrier_name, scac = self._extract_carrier(node, root)
            quote_number = _all_text(
                node,
                "QUOTEID",
                "QUOTENUMBER",
                "QUOTEID",
                "QUOTE",
                "QUOTEIDNUMBER",
                "QUOTEIDENTIFIER",
            )
            transit = _all_text(
                node,
                "ADVERTISEDTRANSIT",
                "TRANSITDAYS",
                "SERVICECENTERTRANSITDAYS",
                "TRANSIT",
            )
            service = _all_text(node, "SERVICETYPE", "SERVICELEVEL", "PRODUCT") or "LTL"
            charges = self._extract_breakdown(node)
            rates.append(
                {
                    "id": quote_number or f"abf-{idx}",
                    "quoteNumber": quote_number,
                    "name": carrier_name,
                    "carrierName": carrier_name,
                    "SCAC": scac,
                    "serviceLevel": service,
                    "transitDays": transit,
                    "total": total,
                    "rateBreakdown": charges,
                    "provider": "arcbest",
                }
            )
        return rates

    def _extract_carrier(self, node: ET.Element, root: ET.Element) -> tuple[str, str]:
        scac = _normalize_scac(
            _all_text(
                node,
                "SCAC",
                "CARRIERSCAC",
                "CARRIERCODE",
                "PROVIDERSCAC",
            )
        )
        carrier_name = _all_text(
            node,
            "CARRIERNAME",
            "CARRIER",
            "PROVIDERNAME",
            "PROVIDER",
            "CARRIERDISPLAYNAME",
        )

        if not carrier_name:
            # Some responses only expose carrier identity on the envelope/root node.
            carrier_name = _all_text(
                root,
                "CARRIERNAME",
                "CARRIER",
                "PROVIDERNAME",
                "PROVIDER",
                "CARRIERDISPLAYNAME",
            )

        if not scac:
            scac = _normalize_scac(
                _all_text(
                    root,
                    "SCAC",
                    "CARRIERSCAC",
                    "CARRIERCODE",
                    "PROVIDERSCAC",
                )
            )

        if not carrier_name and scac == ABF_SCAC:
            carrier_name = "ABF Freight"
        elif not carrier_name and scac:
            carrier_name = scac
        elif not carrier_name:
            carrier_name = "ArcBest"

        if not scac and carrier_name == "ABF Freight":
            scac = ABF_SCAC

        return carrier_name, scac

    def _extract_total(self, node: ET.Element) -> float | None:
        for tag in (
            "CHARGE",
            "TOTAL",
            "TOTALCHARGE",
            "NETCHARGES",
            "RATE",
            "QUOTEAMOUNT",
            "TOTALRATE",
        ):
            value = _text(node, tag)
            if value:
                return _safe_float(value, 0.0)
        return None

    def _extract_breakdown(self, node: ET.Element) -> list[dict[str, Any]]:
        breakdown: list[dict[str, Any]] = []
        charge_nodes = node.findall(".//CHARGE") + node.findall(".//RATECHARGE")
        for charge in charge_nodes:
            name = _all_text(charge, "DESCRIPTION", "TYPE", "NAME")
            total = _all_text(charge, "AMOUNT", "TOTAL", "CHARGE")
            if name and total:
                breakdown.append({"name": name, "total": _safe_float(total, 0.0)})
        for item in node.findall(".//ITEMIZEDCHARGES/ITEM"):
            amount = item.attrib.get("AMOUNT", "").strip()
            if not amount:
                continue
            name = (
                item.attrib.get("DESCRIPTION", "").strip()
                or item.attrib.get("FOR", "").strip()
                or item.attrib.get("TYPE", "").strip()
            )
            breakdown.append({"name": name or "Charge", "total": _safe_float(amount, 0.0)})
        if breakdown:
            return breakdown

        total = self._extract_total(node)
        if total is not None:
            return [{"name": "Freight Charge", "total": total}]
        return []

    def save_rate(self, rate_id: str, accessorials: list[str] | None = None) -> dict[str, Any]:
        # ArcBest's XML quote flow does not require a separate save call in this app.
        return {
            "data": {
                "results": {
                    "quoteNumber": rate_id,
                    "accessorials": _map_accessorials(accessorials),
                }
            }
        }

    def book(self, payload: dict[str, Any]) -> dict[str, Any]:
        shipper = payload.get("shipper", {})
        consignee = payload.get("consignee", {})
        items = payload.get("lineItems") or []
        pickup_date = _parse_date(payload.get("pickupDate", "")) or dt.date.today().strftime("%m/%d/%Y")
        requester_phone = shipper.get("phone", "")
        ship_phone = shipper.get("phone", "")
        cons_phone = consignee.get("phone", "")
        params: dict[str, Any] = {
            "DL": "2",
            "RequesterType": "1",
            "PayTerms": "P",
            "RequesterName": shipper.get("contact") or shipper.get("name", ""),
            "RequesterPhone": _phone10(requester_phone),
            "RequesterEmail": ARCBEST_USERNAME,
            "RequesterPhoneExt": _phone_ext(requester_phone),
            "ShipContact": shipper.get("contact") or shipper.get("name", ""),
            "ShipName": shipper.get("name", ""),
            "ShipAddress": shipper.get("address", ""),
            "ShipCountry": shipper.get("country", "US"),
            "ShipPhone": _phone10(ship_phone),
            "ShipPhoneExt": _phone_ext(ship_phone),
            "ShipEmail": ARCBEST_USERNAME,
            "ConsContact": consignee.get("contact") or consignee.get("name", ""),
            "ConsName": consignee.get("name", ""),
            "ConsAddress": consignee.get("address", ""),
            "ConsCountry": consignee.get("country", "US"),
            "ConsPhone": _phone10(cons_phone),
            "ConsPhoneExt": _phone_ext(cons_phone),
            "ConsEmail": "",
            "PickupDate": pickup_date,
            "ShipDate": pickup_date,
            "ProAutoAssign": "Y",
            "QuoteID": payload.get("quoteNumber", ""),
            "AT": "16:30",
            "OT": "08:00",
            "CT": "17:00",
            "Instructions": payload.get("specialInstructions") or payload.get("BOLInstructions") or "",
            "Bol": payload.get("BOLNmbr", "") or payload.get("bolNumber", ""),
            "PO1": payload.get("referenceNumber", ""),
            "CRN1": payload.get("referenceNumber", ""),
            "CRN2": payload.get("referenceNumber2", ""),
            "PkupCopyShip": "Y",
            "BolCopyShip": "Y",
            "FileFormat": "A",
        }
        if ARCBEST_TEST_MODE:
            params["Test"] = "Y"
        params.update(
            _build_location_params("Ship", shipper.get("city", ""), shipper.get("state", ""), shipper.get("zipCode", ""))
        )
        params.update(
            _build_location_params(
                "Cons",
                consignee.get("city", ""),
                consignee.get("state", ""),
                consignee.get("zipCode", ""),
            )
        )
        params.update(_build_accessorial_params(PICKUP_ACCESSORIAL_FLAGS, payload.get("accessorialsList")))

        for idx, item in enumerate(items, start=1):
            qty = int(item.get("qty") or 1)
            params[f"HN{idx}"] = qty
            params[f"HT{idx}"] = _unit_type(item.get("unit_type"))
            params[f"WT{idx}"] = int(round(_safe_float(item.get("weight"), 0.0) * qty))
            params[f"CL{idx}"] = str(item.get("freight_class") or "70")
            params[f"Desc{idx}"] = str(item.get("description") or f"Freight line {idx}")[:35]
            params[f"POPiece{idx}"] = qty
            params[f"POWeight{idx}"] = int(round(_safe_float(item.get("weight"), 0.0) * qty))
            nmfc = str(item.get("nmfc_item") or "").strip()
            sub = str(item.get("nmfc_sub") or "").strip()
            if nmfc:
                params[f"NMFC{idx}"] = nmfc
            if sub:
                params[f"SUB{idx}"] = sub

        root = self._get(ARCBEST_BOOK_PATH, params, timeout=60)
        return {"data": {"results": [self._parse_booking(root)]}}

    def _parse_booking(self, root: ET.Element) -> dict[str, Any]:
        confirmation = _all_text(
            root,
            "CONFIRMATIONNUMBER",
            "CONFIRMATION",
            "REQUESTNUMBER",
            "PICKUPNUMBER",
        )
        return {
            "BOLNmbr": _all_text(
                root,
                "PRONUMBER",
                "BOLNUMBER",
                "BOLNUM",
                "PRONUMBER",
                "PRO",
                "CONFIRMATIONNUMBER",
            ),
            "BOLId": _all_text(root, "BOLID", "PICKUPREQUESTNUMBER", "REQUESTNUMBER"),
            "confirmation": confirmation,
            "documents": [
                {"type": "BOL", "url": _document_url(_all_text(root, "DOCUMENT"))},
                {"type": "LBL", "url": _document_url(_all_text(root, "LABELDOCUMENT"))},
                {"type": "PROLBL", "url": _document_url(_all_text(root, "PROLABELDOCUMENT"))},
            ],
        }

    def update_bol(self, bol_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        # ArcBest pickup request is submitted as part of book() in this app flow.
        return {"bol_id": bol_id, "payload": payload}

    def dispatch(self, bol_id: str) -> dict[str, Any]:
        # Treat the booking confirmation as the dispatch confirmation for ABF.
        return {"data": {"results": {"confirmation": bol_id}}}

    def track(self, bol_number: str) -> dict[str, Any]:
        raise RuntimeError("Tracking is not wired for ArcBest in this app yet.")


client = ArcBestClient()
