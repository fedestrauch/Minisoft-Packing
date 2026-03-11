"""ShipPrimus API wrapper — handles auth + token refresh transparently."""
import re
import requests
from config import PRIMUS_BASE_URL, PRIMUS_USERNAME, PRIMUS_PASSWORD


class PrimusClient:
    def __init__(self):
        self.base_url = PRIMUS_BASE_URL.rstrip("/")
        self.token = None

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def login(self):
        resp = requests.post(
            f"{self.base_url}/api/v1/login",
            json={"username": PRIMUS_USERNAME, "password": PRIMUS_PASSWORD},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        d = data.get("data", {})
        self.token = (
            data.get("token")
            or data.get("access_token")
            or d.get("token")
            or d.get("accessToken")
            or d.get("access_token")
        )
        if not self.token:
            raise RuntimeError(f"Login succeeded but no token found in response: {data}")
        return self.token

    def _headers(self):
        if not self.token:
            self.login()
        return {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}

    def _request(self, method, path, *, params=None, json=None, retry=True):
        url = f"{self.base_url}{path}"
        # Rating calls can be slow — use a longer timeout
        timeout = 90 if "rate" in path else 30
        resp = requests.request(method, url, headers=self._headers(), params=params, json=json, timeout=timeout)
        if resp.status_code == 401 and retry:
            self.login()
            return self._request(method, path, params=params, json=json, retry=False)
        if not resp.ok:
            raise RuntimeError(f"{resp.status_code} {resp.reason}: {resp.text[:500]}")
        if resp.status_code == 204 or not resp.content:
            return {}
        return resp.json()

    # ------------------------------------------------------------------
    # Rating
    # ------------------------------------------------------------------

    def get_rates(self, origin: dict, destination: dict, freight_info: list, accessorials: list = None) -> list:
        """
        GET /applet/v1/rate/multiple
        Returns list of carrier rate objects.

        Required params (confirmed from API):
          - originZipcode / destinationZipcode  (not originZip)
          - UOM: 'US' | 'METRIC'
          - freightInfo[*].weightType: 'each' | 'total'
        """
        import json

        # Normalise zip codes: strip +4 extension (e.g. "07114-2216" → "07114")
        def _zip5(z):
            return str(z).split("-")[0].strip()

        # Inject required fields into each freight item
        normalised_freight = []
        for item in freight_info:
            row = dict(item)
            row.setdefault("weightType", "each")
            # ShipPrimus requires "class", not "freight_class"
            if "freight_class" in row:
                row["class"] = row.pop("freight_class")
            normalised_freight.append(row)

        params = {
            "originZipcode": _zip5(origin["zip"]),
            "originCity": origin["city"],
            "originState": origin["state"],
            "originCountry": origin.get("country", "US"),
            "destinationZipcode": _zip5(destination.get("zip", "")),
            "destinationCity": destination.get("city", ""),
            "destinationState": destination.get("state", ""),
            "destinationCountry": destination.get("country", "US"),
            "UOM": "US",
            "freightInfo": json.dumps(normalised_freight),
        }
        if accessorials:
            # API expects accessorialsList[]=LFO|RSD (pipe-delimited, bracket suffix in key)
            params["accessorialsList[]"] = "|".join(
                a if isinstance(a, str) else a.get("code", "") for a in accessorials
            )
        data = self._request("GET", "/applet/v1/rate/multiple", params=params)
        # Response shape: {"data": {"results": {"rates": [...]}}}
        if isinstance(data, list):
            return data
        nested = data.get("data", data)
        if isinstance(nested, dict):
            nested = nested.get("results", nested)
        if isinstance(nested, dict):
            nested = nested.get("rates", nested)
        if isinstance(nested, list):
            return nested
        return []

    def save_rate(self, rate_id: str, accessorials: list = None) -> dict:
        """POST /applet/v1/rate/save — returns quoteNumber + rate details."""
        body = {"rateId": rate_id}
        if accessorials:
            body["accessorialsList"] = [
                a if isinstance(a, str) else a.get("code", "") for a in accessorials
            ]
        return self._request("POST", "/applet/v1/rate/save", json=body)

    # ------------------------------------------------------------------
    # Booking
    # ------------------------------------------------------------------

    def book(self, payload: dict) -> dict:
        """
        POST /applet/v1/book — returns BOL number.

        Required fields (confirmed from API):
          shipper/consignee: name, address, city, state, zipCode, country
          lineItems[*]: qty, description, weight, length, width, height,
                        freight_class, weightType ('each'|'total')
          quoteNumber, pickupDate
        Response: {"data": {"results": [{"BOLNmbr": "...", "BOLId": ..., "documents": [...]}]}}
        """
        def _clean_phone(phone: str) -> str:
            """Strip country code, return 10 digits only (extension handled separately)."""
            if not phone:
                return ""
            base = re.sub(r'(?i)\s*(ext\.?|x)\s*\d+', '', str(phone))
            digits = re.sub(r'\D', '', base)
            if len(digits) == 11 and digits[0] == '1':
                digits = digits[1:]
            return digits[:10]

        def _extract_ext(phone: str) -> str:
            """Return just the numeric extension, e.g. '88030'."""
            m = re.search(r'(?i)\s*(ext\.?|x)\s*(\d+)', str(phone))
            return m.group(2) if m else ""

        # Normalise shipper/consignee: rename keys, clean zip and phone,
        # send street address under all common field names
        def _norm_addr(addr: dict) -> dict:
            a = dict(addr)
            if "companyName" in a and "name" not in a:
                a["name"] = a.pop("companyName")
            elif "addressee" in a and "name" not in a:
                a["name"] = a.pop("addressee")
            if "zip" in a and "zipCode" not in a:
                a["zipCode"] = str(a.pop("zip")).split("-")[0]
            elif "zipCode" in a:
                a["zipCode"] = str(a["zipCode"]).split("-")[0]
            street = a.get("address") or a.get("addr1") or a.get("address1") or ""
            if street:
                a["address"] = street
                a["addr1"] = street
                a["address1"] = street
            if a.get("phone"):
                raw = a["phone"]
                ext = _extract_ext(raw)
                a["phone"] = _clean_phone(raw)
                if ext:
                    a["phoneExtension"] = ext
                    a["phoneExt"] = ext
                    a["extension"] = ext
                    a["contact"] = f"Ext: {ext}"
            return a

        # Normalise lineItems — send description under all known field names,
        # strip internal-only fields (pallet_box) that ShipPrimus doesn't accept
        _ITEM_STRIP = {"pallet_box"}
        items = payload.get("lineItems") or payload.get("freightItems") or payload.get("freightInfo") or []
        norm_items = []
        for item in items:
            row = {k: v for k, v in item.items() if k not in _ITEM_STRIP}
            row.setdefault("weightType", "each")
            # ShipPrimus requires "class", not "freight_class"
            if "freight_class" in row:
                row["class"] = row.pop("freight_class")
            desc = row.get("description") or row.get("shortDescription") or row.get("commodity") or ""
            if desc:
                row["description"] = desc
                row["shortDescription"] = desc
                row["commodity"] = desc
                row["commodityDescription"] = desc
            norm_items.append(row)

        norm_payload = {
            "quoteNumber": payload.get("quoteNumber", ""),
            "pickupDate": payload.get("pickupDate", ""),
            "shipper": _norm_addr(payload.get("shipper", {})),
            "consignee": _norm_addr(payload.get("consignee", {})),
            "lineItems": norm_items,
        }
        # REF 1 = SO# — try all common field names
        ref = payload.get("referenceNumber", "")
        if ref:
            norm_payload["referenceNumber"] = ref
            norm_payload["shipperReferenceNumber"] = ref
            norm_payload["shipper"]["referenceNumber"] = ref
            norm_payload["shipper"]["refNumber"] = ref
        # REF 2 = PO#
        ref2 = payload.get("referenceNumber2", "")
        if ref2:
            norm_payload["referenceNumber2"] = ref2
            norm_payload["poNumber"] = ref2
            norm_payload["purchaseOrderNumber"] = ref2
            norm_payload["shipper"]["referenceNumber2"] = ref2
            norm_payload["shipper"]["poNumber"] = ref2
        if payload.get("specialInstructions"):
            norm_payload["specialInstructions"] = payload["specialInstructions"]
        if payload.get("BOLInstructions"):
            norm_payload["BOLInstructions"] = payload["BOLInstructions"]
        # accessorialsList must be an array of code strings (e.g. ["LFD", "RSD"])
        raw_acc = payload.get("accessorialsList") or payload.get("accessorials") or []
        if raw_acc:
            norm_payload["accessorialsList"] = [
                a if isinstance(a, str) else a.get("code", "") for a in raw_acc
            ]

        import logging as _logging
        _logging.getLogger("shipprimus").debug(
            "PRIMUS book payload specialInstructions=%r accessorials=%s",
            norm_payload.get("specialInstructions"),
            norm_payload.get("accessorials"),
        )
        return self._request("POST", "/applet/v1/book", json=norm_payload)

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    def update_bol(self, bol_id: str, payload: dict) -> None:
        """PUT /applet/v1/book/{BOLId} — update pickup window before dispatch."""
        self._request("PUT", f"/applet/v1/book/{bol_id}", json=payload)

    def dispatch(self, bol_id: str) -> dict:
        """POST /applet/v2/dispatch/{BOLId} — returns confirmation, billTo, quoteNumber."""
        return self._request("POST", f"/applet/v2/dispatch/{bol_id}")

    # ------------------------------------------------------------------
    # Tracking
    # ------------------------------------------------------------------

    def track(self, bol_number: str) -> dict:
        """GET /applet/v1/tracking — returns status timeline."""
        return self._request("GET", "/applet/v1/tracking", params={"bolNumber": bol_number})


# Singleton used by Flask app
client = PrimusClient()
