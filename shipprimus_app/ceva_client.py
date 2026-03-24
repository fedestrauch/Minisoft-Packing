"""CEVA Logistics Matrix API client.

Auth: OAuth2 Client Credentials (RFC 6749)
Rate quote: POST XML (B2BMatrixRateRequest V1.0) → JSON response
Booking:    POST XML (B2BMatrixOrder V1.4)        → JSON response with HAWB + base64 BOL/label

Token is cached per-instance and refreshed on 401 (same pattern as primus_client.py).
Rate response format is not documented by CEVA; parsed defensively and logged in full
on first call so the structure can be inspected in output/app.log.
"""

import base64
import logging
import os
import socket
import time
import uuid
from urllib.parse import urlparse
from datetime import datetime, timedelta
from xml.sax.saxutils import escape as xml_escape

import requests

from config import (
    CEVA_CONSUMER_KEY,
    CEVA_CONSUMER_SECRET,
    CEVA_COST_CENTER,
    CEVA_CUSTOMER_CODE,
    CEVA_ORDER_BU_ID,
    CEVA_ORDER_CUSTOMER_ID,
    CEVA_ORDER_URL,
    CEVA_RATE_BU_ID,
    CEVA_RATE_CUSTOMER_ID,
    CEVA_RATE_URL,
    CEVA_RATE_SCHEMA_VERSION,
    CEVA_TOKEN_URL,
)

log = logging.getLogger("shipprimus")

COMMODITY_CODE = "FURNIT"  # furniture commodity code
_OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")


def _xe(value) -> str:
    """xml.escape for user-supplied string values."""
    return xml_escape(str(value or ""))


def _zip5(value) -> str:
    return str(value or "").split("-")[0].strip()[:5]


def _phone_digits(value) -> str:
    import re
    digits = re.sub(r"\D", "", str(value or ""))
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits[:10]


def _format_request_error(action: str, url: str, exc: Exception) -> RuntimeError:
    host = (urlparse(url).hostname or "").lower()
    if isinstance(exc, requests.exceptions.RequestException):
        detail = str(exc)
        if isinstance(exc, requests.exceptions.ConnectionError) and (
            "nameresolutionerror" in detail.lower()
            or "getaddrinfo failed" in detail.lower()
            or host.endswith(".corp")
        ):
            return RuntimeError(
                f"CEVA {action} failed: could not resolve host '{host}'. "
                "This usually means the endpoint is private/VPN-only or the CEVA rate URL is misconfigured."
            )
        return RuntimeError(f"CEVA {action} failed calling {host or url}: {detail}")
    return RuntimeError(f"CEVA {action} failed: {exc}")


def _assert_dns_access(url: str, label: str) -> None:
    host = (urlparse(url).hostname or "").lower()
    if not host:
        return
    try:
        socket.getaddrinfo(host, 443)
    except socket.gaierror as exc:
        if host.endswith(".corp"):
            log.warning(
                "CEVA %s: host '%s' could not be resolved via DNS (may be VPN-only) — attempting call anyway",
                label, host,
            )
            return
        raise RuntimeError(f"CEVA {label} failed DNS lookup for '{host}': {exc}") from exc


class CevaClient:
    def __init__(self):
        self.token: str | None = None
        self._token_expires_at: float = 0.0

    # ── Auth ────────────────────────────────────────────────────────────

    def _fetch_token(self) -> str:
        """Fetch a new OAuth2 bearer token from the CEVA gateway."""
        if not CEVA_CONSUMER_KEY or not CEVA_CONSUMER_SECRET:
            raise RuntimeError("Missing CEVA_CONSUMER_KEY or CEVA_CONSUMER_SECRET in .env")
        _assert_dns_access(CEVA_TOKEN_URL, "token endpoint")
        try:
            resp = requests.post(
                CEVA_TOKEN_URL,
                data={
                    "grant_type": "client_credentials",
                    "client_id": CEVA_CONSUMER_KEY,
                    "client_secret": CEVA_CONSUMER_SECRET,
                },
                timeout=30,
            )
        except requests.exceptions.RequestException as exc:
            raise _format_request_error("token request", CEVA_TOKEN_URL, exc) from exc
        resp.raise_for_status()
        data = resp.json()
        self.token = data.get("access_token") or data.get("token")
        if not self.token:
            raise RuntimeError(f"CEVA token response missing access_token: {data}")
        expires_in = int(data.get("expires_in", 3600))
        self._token_expires_at = time.time() + expires_in - 60  # 60-second buffer
        return self.token

    def _auth_header(self) -> str:
        if not self.token or time.time() >= self._token_expires_at:
            self._fetch_token()
        return f"Bearer {self.token}"

    def _mx_rate_headers(self) -> dict:
        return {
            "Authorization": self._auth_header(),
            "Content-Type": "application/xml",
            "Accept": "application/json",
            "MX-BusinessUnitId": CEVA_RATE_BU_ID,
            "MX-CustomerId": CEVA_RATE_CUSTOMER_ID,
            "MX-CostCenterCode": CEVA_COST_CENTER,
            "MX-Source": "B2B_API_SYNC",
            "MX-SourceId": str(uuid.uuid4()),
            "MX-SchemaName": "B2B_MatrixRateRequest_V1",
            # Match the rate request XML namespace/version expected by the gateway.
            "MX-SchemaVersion": CEVA_RATE_SCHEMA_VERSION,
            "MX-TradingPartner": "CEVA_USDOM",
        }

    def _mx_order_headers(self) -> dict:
        return {
            "Authorization": self._auth_header(),
            "Content-Type": "application/xml",
            "Accept": "application/json",
            "MX-BusinessUnitId": CEVA_ORDER_BU_ID,
            "MX-CustomerId": CEVA_ORDER_CUSTOMER_ID,
            "MX-CostCenterCode": CEVA_COST_CENTER,
            "MX-Source": "B2B_API_SYNC",
            "MX-SourceId": str(uuid.uuid4()),
            "MX-SchemaName": "B2B_MATRIXORDER_V1",
            "MX-SchemaVersion": "1.4",
            "MX-TradingPartner": "CEVA_USDOM",
        }

    def _post(self, url: str, headers: dict, body: str, retry: bool = True) -> dict:
        _assert_dns_access(url, "endpoint")
        try:
            resp = requests.post(url, headers=headers, data=body.encode("utf-8"), timeout=90)
        except requests.exceptions.RequestException as exc:
            raise _format_request_error("request", url, exc) from exc
        if resp.status_code == 401 and retry:
            self.token = None
            headers["Authorization"] = self._auth_header()
            return self._post(url, headers, body, retry=False)
        if not resp.ok:
            raise RuntimeError(f"CEVA {resp.status_code} {resp.reason}: {resp.text[:500]}")
        return resp.json()

    # ── Rate Quote ──────────────────────────────────────────────────────

    def _build_rate_xml(
        self,
        origin: dict,
        destination: dict,
        freight_items: list,
        accessorials: list,
    ) -> str:
        now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        ship_dt = datetime.utcnow().strftime("%Y-%m-%d") + "T09:00:00"
        doc_id = str(uuid.uuid4().int)[:8]

        total_weight = max(
            1,
            int(sum((i.get("weight") or 0) * (i.get("qty") or 1) for i in freight_items)),
        )

        pkg_xml = ""
        for item in freight_items:
            qty = int(item.get("qty") or 1)
            weight = max(1, int(item.get("weight") or 1))
            length = max(1, int(item.get("length") or 1))
            width = max(1, int(item.get("width") or 1))
            height = max(1, int(item.get("height") or 1))
            desc = _xe(str(item.get("description") or "Furniture")[:30])
            freight_class = str(item.get("freight_class") or "").strip()
            class_xml = ""
            if freight_class:
                class_xml = f"""
    <PackageReference>
      <ReferenceQualifier>FCC</ReferenceQualifier>
      <ReferenceNumber>{_xe(freight_class)}</ReferenceNumber>
    </PackageReference>"""
            pkg_xml += f"""
  <OrderPackage>
    <Quantity>{qty}</Quantity>
    <PackageDescription>{desc}</PackageDescription>
    <GrossWeight>{weight}</GrossWeight>
    <Collapsable>false</Collapsable>
    <PackageReference>
      <ReferenceQualifier>CMD</ReferenceQualifier>
      <ReferenceNumber>{COMMODITY_CODE}</ReferenceNumber>
    </PackageReference>{class_xml}
    <PackageMeasure>
      <MeasurementTypeCode>LEN</MeasurementTypeCode>
      <MeasurementValue>{length}</MeasurementValue>
    </PackageMeasure>
    <PackageMeasure>
      <MeasurementTypeCode>WID</MeasurementTypeCode>
      <MeasurementValue>{width}</MeasurementValue>
    </PackageMeasure>
    <PackageMeasure>
      <MeasurementTypeCode>HGT</MeasurementTypeCode>
      <MeasurementValue>{height}</MeasurementValue>
    </PackageMeasure>
  </OrderPackage>"""

        svc_xml = ""
        for acc in (accessorials or []):
            svc_xml += f"""
  <OrderService>
    <ServiceCode>{_xe(acc)}</ServiceCode>
  </OrderService>"""

        return f"""<?xml version="1.0" encoding="UTF-8"?>
<B2BMatrixRateRequest xmlns="http://www.cevalogistics.com/Matrix/OrderManagement/Order/B2B_MatrixRateRequest_V1_0">
  <DocumentHeader>
    <DocumentID>{doc_id}</DocumentID>
    <DocumentDateTime>{now}</DocumentDateTime>
    <TransactionSet>850</TransactionSet>
    <PurposeCode>00</PurposeCode>
    <PartnerCode>CEVA_USDOM</PartnerCode>
    <WeightUOMTypeCode>LBR</WeightUOMTypeCode>
    <VolumeUOMTypeCode>FTQ</VolumeUOMTypeCode>
  </DocumentHeader>
  <OrderIdNbr>{doc_id}</OrderIdNbr>
  <OrderWeight>{total_weight}</OrderWeight>
  <ShipDateTime>{ship_dt}</ShipDateTime>
  <OMSOrderTypeCode>DO</OMSOrderTypeCode>
  <TransportationModeTypeCode>M</TransportationModeTypeCode>
  <ModeTypeServiceLevelCode>STD</ModeTypeServiceLevelCode>
  <OrderEntity>
    <PartyQualifier>SF</PartyQualifier>
    <EntityAlias>PERS_ADDR</EntityAlias>
    <EntityName>{_xe(origin.get("company", ""))}</EntityName>
    <Address>{_xe(origin.get("address", "") or origin.get("address1", ""))}</Address>
    <City>{_xe(origin.get("city", ""))}</City>
    <State>{_xe(origin.get("state", ""))}</State>
    <PostalCode>{_zip5(origin.get("zip", ""))}</PostalCode>
    <CountryCode>{_xe(origin.get("country", "US"))}</CountryCode>
  </OrderEntity>
  <OrderEntity>
    <PartyQualifier>ST</PartyQualifier>
    <EntityAlias>PERS_ADDR</EntityAlias>
    <EntityName>{_xe(destination.get("addressee", "") or destination.get("name", ""))}</EntityName>
    <Address>{_xe(destination.get("address", "") or destination.get("address1", ""))}</Address>
    <City>{_xe(destination.get("city", ""))}</City>
    <State>{_xe(destination.get("state", ""))}</State>
    <PostalCode>{_zip5(destination.get("zip", ""))}</PostalCode>
    <CountryCode>{_xe(destination.get("country", "US"))}</CountryCode>
  </OrderEntity>
  <OrderReference>
    <ReferenceQualifier>VST</ReferenceQualifier>
    <ReferenceNumber>IDD</ReferenceNumber>
  </OrderReference>{pkg_xml}{svc_xml}
</B2BMatrixRateRequest>"""

    def get_rates(
        self,
        origin: dict,
        destination: dict,
        freight_items: list,
        accessorials: list = None,
    ) -> list:
        self._fetch_token()  # always refresh per CEVA support guidance
        body = self._build_rate_xml(origin, destination, freight_items, accessorials or [])
        headers = self._mx_rate_headers()
        log.debug("CEVA rate request XML (first 400): %s", body[:400])

        try:
            data = self._post(CEVA_RATE_URL, headers, body)
        except RuntimeError as exc:
            msg = str(exc).lower()
            if "could not resolve host" in msg or "nameresolution" in msg or "connection" in msg:
                log.warning("CEVA rate unavailable (host unreachable): %s", exc)
                raise RuntimeError(
                    f"CEVA rate API host is unreachable ({urlparse(CEVA_RATE_URL).hostname}). "
                    "This endpoint requires VPN or direct network access to CEVA's internal gateway. "
                    "Contact CEVA support to request a publicly accessible rate endpoint."
                ) from exc
            raise
        log.debug("CEVA rate response: %s", str(data)[:2000])

        return self._parse_rates(data)

    def _parse_rates(self, data: dict) -> list:
        """Parse CEVA rate response defensively.

        CEVA's rate response format is not documented. This method attempts
        common key names and logs the full response as a warning if it can't
        find a rate, so the actual structure is visible in output/app.log.
        """
        # Top-level keys observed in order response (rate may differ):
        # data → APIResponse.ResponseContext, BDIDocumentResponseContext
        bdi = data.get("BDIDocumentResponseContext") or {}
        api_resp = data.get("APIResponse") or {}
        resp_ctx = api_resp.get("ResponseContext") or {}
        rating_result = data.get("RatingResult") or {}
        rate_ctx = rating_result.get("Rate") or {}
        inserted_activity = rating_result.get("InsertedActivity") or {}

        # Try to find a numeric rate from common key names across levels
        candidate_dicts = [data, bdi, resp_ctx, api_resp, rating_result, rate_ctx]
        rate_keys = [
            "totalCharge", "TotalCharge", "total", "Total",
            "rate", "Rate", "quoteAmount", "QuoteAmount",
            "netCharges", "NetCharges", "totalRate", "TotalRate",
            "TotalAmount", "totalAmount",
        ]
        rate_value = None
        for d in candidate_dicts:
            if not isinstance(d, dict):
                continue
            for k in rate_keys:
                value = d.get(k)
                if isinstance(value, dict):
                    nested_total = value.get("TotalAmount") or value.get("totalAmount")
                    if nested_total is not None:
                        rate_value = nested_total
                        break
                    continue
                if value is not None:
                    rate_value = value
                    break
            if rate_value is not None:
                break

        service_keys = ["serviceLevel", "ServiceLevel", "service", "modeService", "Code", "Description"]
        service = "Standard"
        for d in candidate_dicts:
            if not isinstance(d, dict):
                continue
            for k in service_keys:
                if k in d:
                    service = str(d[k])
                    break
            if service != "Standard":
                break

        transit_keys = ["transitDays", "TransitDays", "transitTime", "transit"]
        transit = ""
        for d in candidate_dicts:
            if not isinstance(d, dict):
                continue
            for k in transit_keys:
                if k in d:
                    transit = str(d[k])
                    break

        if rate_value is None:
            log.warning(
                "CEVA: could not parse rate value from response. "
                "Inspect output/app.log for full structure. Response: %s",
                str(data)[:3000],
            )
            return []

        try:
            total = float(str(rate_value).replace(",", "").replace("$", "").strip())
        except (ValueError, TypeError):
            log.warning("CEVA: rate value %r is not numeric", rate_value)
            return []

        breakdown = []
        charge_results = rate_ctx.get("ChargeResult")
        if isinstance(charge_results, list):
            for entry in charge_results:
                if not isinstance(entry, dict):
                    continue
                charge = entry.get("Charge") or {}
                amount = charge.get("TotalAmount")
                try:
                    charge_total = float(str(amount).replace(",", "").replace("$", "").strip())
                except (ValueError, TypeError):
                    continue
                tariff_charge = charge.get("TariffCharge") or {}
                breakdown.append(
                    {
                        "name": tariff_charge.get("Description") or entry.get("Description") or "Charge",
                        "code": tariff_charge.get("Code") or entry.get("Code") or "",
                        "total": charge_total,
                    }
                )
        if not breakdown:
            breakdown = [{"name": "Freight Charge", "total": total}]

        quote_number = (
            inserted_activity.get("ActivityId")
            or rating_result.get("ActivityId")
            or f"ceva-{service}"
        )

        return [
            {
                "id": f"ceva-{service}",
                "quoteNumber": str(quote_number),
                "name": "CEVA Logistics",
                "carrierName": "CEVA Logistics",
                "SCAC": "CEVA",
                "serviceLevel": service,
                "transitDays": transit,
                "total": total,
                "rateBreakdown": breakdown,
                "provider": "ceva",
                "_service_level": service,
            }
        ]

    def save_rate(self, rate_id: str, accessorials: list = None) -> dict:
        """No server-side save for CEVA. Return rate_id as quote number,
        plus the service level so app.py can store it in session."""
        # rate_id format: "ceva-{service}" (from _parse_rates above)
        parts = str(rate_id).split("-")
        service_level = parts[1] if len(parts) >= 2 else "IDD"
        return {
            "data": {
                "results": {
                    "quoteNumber": rate_id,
                    "serviceLevel": service_level,
                }
            }
        }

    # ── Booking ─────────────────────────────────────────────────────────

    def _build_order_xml(self, payload: dict, service_level: str) -> str:
        shipper = payload.get("shipper", {})
        consignee = payload.get("consignee", {})
        items = payload.get("lineItems", [])
        pickup_date = str(payload.get("pickupDate", "") or "")
        so_number = str(payload.get("referenceNumber", "") or "")
        po_number = str(payload.get("referenceNumber2", "") or "")
        notes = str(payload.get("specialInstructions") or payload.get("BOLInstructions") or "")

        now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        doc_id = str(uuid.uuid4().int)[:8]

        try:
            d = datetime.strptime(pickup_date, "%Y-%m-%d")
            start_dt = d.strftime("%Y-%m-%dT09:00:00")
            end_dt = (d + timedelta(days=7)).strftime("%Y-%m-%dT13:00:00")
            tpd_dt = d.strftime("%Y-%m-%dT17:00:00")
        except ValueError:
            start_dt = end_dt = tpd_dt = now

        total_weight = max(
            1, int(sum((i.get("weight") or 0) * (i.get("qty") or 1) for i in items))
        )
        total_pieces = max(1, sum(int(i.get("qty") or 1) for i in items))

        def _entity(qualifier, addr, contact_qualifier):
            name = _xe(addr.get("name") or addr.get("company") or "")
            address = _xe(addr.get("address") or addr.get("address1") or "")
            city = _xe(addr.get("city") or "")
            state = _xe(addr.get("state") or "")
            postal = _zip5(addr.get("zipCode") or addr.get("zip") or "")
            country = _xe(addr.get("country") or "US")
            contact_name = _xe(addr.get("contact") or addr.get("name") or "")
            phone = _phone_digits(addr.get("phone") or "")
            return f"""  <OrderEntity>
    <PartyQualifier>{qualifier}</PartyQualifier>
    <EntityAlias>PERS_ADDR</EntityAlias>
    <EntityName>{name}</EntityName>
    <Address>{address}</Address>
    <City>{city}</City>
    <State>{state}</State>
    <PostalCode>{postal}</PostalCode>
    <CountryCode>{country}</CountryCode>
    <OrderEntityContact>
      <ContactTypeQualifier>{contact_qualifier}</ContactTypeQualifier>
      <ContactName>{contact_name}</ContactName>
      <OrderEntityContactComm>
        <CommunicationTypeQualifier>BIZTE</CommunicationTypeQualifier>
        <Definition>{phone}</Definition>
      </OrderEntityContactComm>
    </OrderEntityContact>
  </OrderEntity>"""

        refs = ""
        if so_number:
            refs += f"""
  <OrderReference>
    <ReferenceQualifier>ON</ReferenceQualifier>
    <ReferenceNumber>{_xe(so_number)}</ReferenceNumber>
  </OrderReference>"""
        if po_number:
            refs += f"""
  <OrderReference>
    <ReferenceQualifier>PO</ReferenceQualifier>
    <ReferenceNumber>{_xe(po_number)}</ReferenceNumber>
  </OrderReference>"""
        for qual, val in [
            ("BLN", CEVA_CUSTOMER_CODE),
            ("CCD", CEVA_CUSTOMER_CODE),
            ("PRT", "B2B"),
            ("VST", "GND"),
            ("DC", "DOM"),
            ("PT", "LTL"),
            ("OMT", "INNET"),
        ]:
            refs += f"""
  <OrderReference>
    <ReferenceQualifier>{qual}</ReferenceQualifier>
    <ReferenceNumber>{_xe(val)}</ReferenceNumber>
  </OrderReference>"""

        pkg_xml = ""
        for idx, item in enumerate(items, 1):
            qty = int(item.get("qty") or 1)
            weight = max(1, int(item.get("weight") or 1))
            length = max(1, int(item.get("length") or 1))
            width = max(1, int(item.get("width") or 1))
            height = max(1, int(item.get("height") or 1))
            desc = _xe(str(item.get("description") or "Furniture")[:30])
            pkg_xml += f"""
  <OrderPackage ID="PKG_{idx:02d}">
    <Quantity>{qty}</Quantity>
    <PackageDescription>{desc}</PackageDescription>
    <GrossWeight>{weight}</GrossWeight>
    <ContainerIdNumber>Box</ContainerIdNumber>
    <Collapsable>false</Collapsable>
    <PackageReference>
      <ReferenceQualifier>CMD</ReferenceQualifier>
      <ReferenceNumber>{COMMODITY_CODE}</ReferenceNumber>
    </PackageReference>
    <PackageMeasure>
      <MeasurementTypeCode>LEN</MeasurementTypeCode>
      <MeasurementValue>{length}</MeasurementValue>
    </PackageMeasure>
    <PackageMeasure>
      <MeasurementTypeCode>WID</MeasurementTypeCode>
      <MeasurementValue>{width}</MeasurementValue>
    </PackageMeasure>
    <PackageMeasure>
      <MeasurementTypeCode>HGT</MeasurementTypeCode>
      <MeasurementValue>{height}</MeasurementValue>
    </PackageMeasure>
  </OrderPackage>"""

        notes_xml = ""
        if notes:
            notes_xml = f"""
  <OrderShippingInstruction>
    <ShippingInstructionCode>DLI</ShippingInstructionCode>
    <Comments>{_xe(notes[:100])}</Comments>
  </OrderShippingInstruction>"""

        return f"""<?xml version="1.0" encoding="UTF-8"?>
<B2BMatrixOrder xmlns="http://www.cevalogistics.com/Matrix/OrderManagement/Order/B2B_MatrixOrder_V1_4">
  <DocumentHeader>
    <DocumentID>{doc_id}</DocumentID>
    <DocumentDateTime>{now}</DocumentDateTime>
    <TransactionSet>850</TransactionSet>
    <PurposeCode>00</PurposeCode>
    <PartnerCode>CEVA_USDOM</PartnerCode>
    <WeightUOMTypeCode>LBR</WeightUOMTypeCode>
    <VolumeUOMTypeCode>FTQ</VolumeUOMTypeCode>
  </DocumentHeader>
  <OrderWeight>{total_weight}</OrderWeight>
  <OrderPieces>{total_pieces}</OrderPieces>
  <StartDateTime>{start_dt}</StartDateTime>
  <EndDateTime>{end_dt}</EndDateTime>
  <ShipDateTime>{start_dt}</ShipDateTime>
  <ScheduleType>SH</ScheduleType>
  <QuantityQualifier>S</QuantityQualifier>
  <ShipFromAlias>PERS_ADDR</ShipFromAlias>
  <ShipToAlias>PERS_ADDR</ShipToAlias>
  <OMSOrderTypeCode>DO</OMSOrderTypeCode>
  <TransportationModeTypeCode>M</TransportationModeTypeCode>
{_entity("SF", shipper, "SHIPPING_ASSOCIATE")}
{_entity("ST", consignee, "CONSIGNEE")}
  <OrderEntity>
    <PartyQualifier>BT</PartyQualifier>
    <EntityAlias>{_xe(CEVA_CUSTOMER_CODE)}</EntityAlias>
    <EntityName>Int Home Miami</EntityName>
  </OrderEntity>{refs}
  <OrderDateTimeReference>
    <DateTimeQualifier>FPD</DateTimeQualifier>
    <DateTimeValue>{start_dt}</DateTimeValue>
  </OrderDateTimeReference>
  <OrderDateTimeReference>
    <DateTimeQualifier>TPD</DateTimeQualifier>
    <DateTimeValue>{tpd_dt}</DateTimeValue>
  </OrderDateTimeReference>{pkg_xml}
  <OrderService>
    <ServiceCode>{_xe(service_level or "IDD")}</ServiceCode>
  </OrderService>{notes_xml}
</B2BMatrixOrder>"""

    def book(self, payload: dict) -> dict:
        service_level = payload.pop("_ceva_service_level", "IDD")
        body = self._build_order_xml(payload, service_level)
        headers = self._mx_order_headers()
        log.debug("CEVA order XML (first 500): %s", body[:500])

        data = self._post(CEVA_ORDER_URL, headers, body)
        log.debug("CEVA order response: %s", str(data)[:2000])

        return self._parse_booking(data)

    def _parse_booking(self, data: dict) -> dict:
        bdi = data.get("BDIDocumentResponseContext") or {}
        hawb = str(bdi.get("HAWB_NUMBER") or bdi.get("hawb_number") or "")

        documents = []
        for file_entry in (bdi.get("BDIDocumentResponseFile") or []):
            file_type = str(file_entry.get("FILE_DESCRIPTION") or "")
            file_b64 = file_entry.get("FILE") or ""
            if not file_b64:
                continue
            try:
                pdf_bytes = base64.b64decode(file_b64)
                is_bol = "BOL" in file_type.upper()
                fname = f"BOL_{hawb}.pdf" if is_bol else f"label_{hawb}.pdf"
                fpath = os.path.join(_OUTPUT_DIR, fname)
                os.makedirs(_OUTPUT_DIR, exist_ok=True)
                with open(fpath, "wb") as f:
                    f.write(pdf_bytes)
                documents.append({
                    "type": "BOL" if is_bol else "LBL",
                    "url": f"/output/{fname}",
                })
                log.debug("CEVA: saved %s → %s", file_type, fpath)
            except Exception as exc:
                log.warning("CEVA: failed to decode %s file: %s", file_type, exc)

        return {
            "data": {
                "results": [
                    {
                        "BOLNmbr": hawb,
                        "BOLId": hawb,
                        "confirmation": hawb,
                        "documents": documents,
                    }
                ]
            }
        }

    # ── Dispatch / Update (no-ops for CEVA — booking is atomic) ────────

    def update_bol(self, bol_id: str, payload: dict) -> dict:
        return {}

    def dispatch(self, bol_id: str) -> dict:
        return {"data": {"results": {"confirmation": bol_id}}}

    # ── Tracking ────────────────────────────────────────────────────────

    def track(self, bol_number: str) -> dict:
        raise NotImplementedError(
            "CEVA tracking endpoint is not yet documented. "
            "Contact your CEVA representative for the tracking API URL."
        )


# Singleton used by Flask app
client = CevaClient()
