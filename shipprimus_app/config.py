import os
from dotenv import load_dotenv

# override=True ensures .env values always win over stale system env vars
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"), override=True)

# Provider routing
FREIGHT_PROVIDER = os.environ.get("FREIGHT_PROVIDER", "primus").strip().lower()
CEVA_ENABLED = os.environ.get("CEVA_ENABLED", "false").strip().lower() in ("1", "true", "yes", "y", "on")

# ShipPrimus
PRIMUS_USERNAME = os.environ.get("PRIMUS_USERNAME", "")
PRIMUS_PASSWORD = os.environ.get("PRIMUS_PASSWORD", "")
PRIMUS_BASE_URL = os.environ.get("PRIMUS_BASE_URL", "https://sandbox-api-applet.shipprimus.com")

# ArcBest / ABF
ARCBEST_API_ID = os.environ.get("ARCBEST_API_ID", "")
ARCBEST_USERNAME = os.environ.get("ARCBEST_USERNAME", "")
ARCBEST_PASSWORD = os.environ.get("ARCBEST_PASSWORD", "")
ARCBEST_BASE_URL = os.environ.get("ARCBEST_BASE_URL", "https://www.abfs.com/xml")
ARCBEST_RATE_PATH = os.environ.get("ARCBEST_RATE_PATH", "aquotexml.asp")
ARCBEST_BOOK_PATH = os.environ.get("ARCBEST_BOOK_PATH", "pickupxml.asp")
ARCBEST_TEST_MODE = os.environ.get("ARCBEST_TEST_MODE", "true").strip().lower() in ("1", "true", "yes", "y", "on")

# NetSuite TBA
NS_ACCOUNT_ID = os.environ.get("NS_ACCOUNT_ID", "")
NS_CONSUMER_KEY = os.environ.get("NS_CONSUMER_KEY", "")
NS_CONSUMER_SECRET = os.environ.get("NS_CONSUMER_SECRET", "")
NS_TOKEN_ID = os.environ.get("NS_TOKEN_ID", "")
NS_TOKEN_SECRET = os.environ.get("NS_TOKEN_SECRET", "")

# NS write-back field IDs
NS_FIELD_QUOTE_NUMBER = os.environ.get("NS_FIELD_QUOTE_NUMBER", "custbody_freight_quote_number")
NS_FIELD_QUOTE_COST = os.environ.get("NS_FIELD_QUOTE_COST", "custbody_freight_quote_cost")
NS_FIELD_BOL_NUMBER = os.environ.get("NS_FIELD_BOL_NUMBER", "custbody_sps_billofladingnumber")
NS_FIELD_CARRIER = os.environ.get("NS_FIELD_CARRIER", "custbody16")
NS_FIELD_DISPATCH_CONFIRM = os.environ.get("NS_FIELD_DISPATCH_CONFIRM", "custbody_dispatch_confirm")

# CEVA Logistics Matrix API (OAuth2 Client Credentials)
CEVA_CONSUMER_KEY = os.environ.get("CEVA_CONSUMER_KEY", "")
CEVA_CONSUMER_SECRET = os.environ.get("CEVA_CONSUMER_SECRET", "")
# SIT (test) endpoints.
# Quick Quote uses CEVA's KUS SIT gateway.
# Booking uses the public cevalogistics.com SIT gateway.
CEVA_TOKEN_URL = os.environ.get("CEVA_TOKEN_URL", "https://apim-gw-sit.cevalogistics.com/token")
CEVA_RATE_URL = os.environ.get("CEVA_RATE_URL", "https://apim-gw-sit-kus.cevalogistics.com/ll/scm/ratequote/1.0.0")
CEVA_ORDER_URL = os.environ.get("CEVA_ORDER_URL", "https://apim-gw-sit.cevalogistics.com/scm/matrixorder/1.0.0/")
CEVA_RATE_SCHEMA_VERSION = os.environ.get("CEVA_RATE_SCHEMA_VERSION", "1.0")
# MX-* headers differ between Rate API and Order API (CEVA uses separate BU/Customer IDs per service)
CEVA_RATE_BU_ID = os.environ.get("CEVA_RATE_BU_ID", "450")
CEVA_RATE_CUSTOMER_ID = os.environ.get("CEVA_RATE_CUSTOMER_ID", "104768")
CEVA_ORDER_BU_ID = os.environ.get("CEVA_ORDER_BU_ID", "414")
CEVA_ORDER_CUSTOMER_ID = os.environ.get("CEVA_ORDER_CUSTOMER_ID", "12645")
CEVA_COST_CENTER = os.environ.get("CEVA_COST_CENTER", "INTE53330ADS")
# Customer code used in XML body (BLN/CCD/BT references — no trailing "DS")
CEVA_CUSTOMER_CODE = os.environ.get("CEVA_CUSTOMER_CODE", "INTE53330A")

# Flask
FLASK_SECRET_KEY = os.environ.get("FLASK_SECRET_KEY", "dev-secret-change-me")

# IHM shipper (hardcoded warehouse)
SHIPPER = {
    "company": "International Home Miami",
    "address": "4340 W 104th St Suite 100",
    "city": "Hialeah",
    "state": "FL",
    "zip": "33018",
    "country": "US",
    "phone": "3056206500",
    "attn": "Johanna Sifontes",
}

REQUIRED_VARS = {
    "primus": ["PRIMUS_USERNAME", "PRIMUS_PASSWORD"],
    "arcbest": ["ARCBEST_API_ID"],
    "ceva": ["CEVA_CONSUMER_KEY", "CEVA_CONSUMER_SECRET"],
}


def validate() -> None:
    required = REQUIRED_VARS.get(FREIGHT_PROVIDER, [])
    missing = [var for var in required if not os.environ.get(var)]
    if missing:
        raise RuntimeError(
            f"Missing required .env variables for provider '{FREIGHT_PROVIDER}': {', '.join(missing)}"
        )
