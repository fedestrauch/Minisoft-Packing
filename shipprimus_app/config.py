import os
from dotenv import load_dotenv

# override=True ensures .env values always win over stale system env vars
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"), override=True)

# ShipPrimus
PRIMUS_USERNAME = os.environ.get("PRIMUS_USERNAME", "")
PRIMUS_PASSWORD = os.environ.get("PRIMUS_PASSWORD", "")
PRIMUS_BASE_URL = os.environ.get("PRIMUS_BASE_URL", "https://sandbox-api-applet.shipprimus.com")

# NetSuite TBA
NS_ACCOUNT_ID = os.environ.get("NS_ACCOUNT_ID", "")
NS_CONSUMER_KEY = os.environ.get("NS_CONSUMER_KEY", "")
NS_CONSUMER_SECRET = os.environ.get("NS_CONSUMER_SECRET", "")
NS_TOKEN_ID = os.environ.get("NS_TOKEN_ID", "")
NS_TOKEN_SECRET = os.environ.get("NS_TOKEN_SECRET", "")

# NS write-back field IDs
NS_FIELD_QUOTE_NUMBER = os.environ.get("NS_FIELD_QUOTE_NUMBER", "custbody_freight_quote_number")
NS_FIELD_QUOTE_COST = os.environ.get("NS_FIELD_QUOTE_COST", "custbody_freight_quote_cost")
NS_FIELD_BOL_NUMBER = os.environ.get("NS_FIELD_BOL_NUMBER", "custbody_tracking_number")
NS_FIELD_CARRIER = os.environ.get("NS_FIELD_CARRIER", "custbody16")
NS_FIELD_DISPATCH_CONFIRM = os.environ.get("NS_FIELD_DISPATCH_CONFIRM", "custbody_dispatch_confirm")

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

REQUIRED_VARS = ["PRIMUS_USERNAME", "PRIMUS_PASSWORD"]


def validate():
    missing = [v for v in REQUIRED_VARS if not os.environ.get(v)]
    if missing:
        raise RuntimeError(f"Missing required .env variables: {', '.join(missing)}")
