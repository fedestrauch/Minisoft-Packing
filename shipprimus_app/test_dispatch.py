"""
test_dispatch.py
Tests POST /applet/v2/dispatch/{BOLId} with incrementally fuller payloads
so we can discover all required fields from the API error responses.

Usage:
    python test_dispatch.py
"""
import json
import requests
from config import PRIMUS_BASE_URL, PRIMUS_USERNAME, PRIMUS_PASSWORD, SHIPPER

BOL_ID     = "1086823685"   # numeric BOL ID from the book response
BASE_URL   = PRIMUS_BASE_URL.rstrip("/")
DISPATCH_URL = f"{BASE_URL}/applet/v2/dispatch/{BOL_ID}"

# ── Login ──────────────────────────────────────────────────────────────────────
print("Logging in...")
login_resp = requests.post(
    f"{BASE_URL}/api/v1/login",
    json={"username": PRIMUS_USERNAME, "password": PRIMUS_PASSWORD},
    timeout=30,
)
login_resp.raise_for_status()
data = login_resp.json()
d = data.get("data", {})
token = (
    data.get("token") or data.get("access_token")
    or d.get("token") or d.get("accessToken") or d.get("access_token")
)
print(f"Token: {token[:30]}...\n")

headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def try_dispatch(label: str, body: dict):
    print(f"{'='*60}")
    print(f"TEST: {label}")
    print(f"URL:  POST {DISPATCH_URL}")
    print(f"Body: {json.dumps(body, indent=2)}")
    resp = requests.post(DISPATCH_URL, headers=headers, json=body, timeout=30)
    print(f"Status: {resp.status_code}")
    try:
        print(json.dumps(resp.json(), indent=2))
    except Exception:
        print(resp.text)
    print()
    return resp

# ── Test 1: full shipper + consignee + line items (mirrors book payload) ───────
try_dispatch("Full payload mirroring book request", {
    "pickupDate":   "2026-03-13",
    "quoteNumber":  "",          # fill if known
    "shipper": {
        "name":     SHIPPER.get("company", "International Home Miami"),
        "contact":  SHIPPER.get("attn", "Johanna Sifontes"),
        "phone":    SHIPPER.get("phone", "3056206500"),
        "address":  SHIPPER.get("address", "4340 W 104th St Suite 100"),
        "addr1":    SHIPPER.get("address", "4340 W 104th St Suite 100"),
        "city":     SHIPPER.get("city", "Hialeah"),
        "state":    SHIPPER.get("state", "FL"),
        "zipCode":  SHIPPER.get("zip", "33018"),
        "country":  SHIPPER.get("country", "US"),
    },
    "consignee": {
        "name":    "Test Consignee",
        "contact": "Test Contact",
        "phone":   "3055550000",
        "address": "123 Test St",
        "addr1":   "123 Test St",
        "city":    "Miami",
        "state":   "FL",
        "zipCode": "33101",
        "country": "US",
    },
    "lineItems": [
        {
            "qty":          1,
            "description":  "Furniture",
            "weight":       100,
            "length":       48,
            "width":        40,
            "height":       36,
            "freight_class": "70",
            "weightType":   "each",
        }
    ],
})
