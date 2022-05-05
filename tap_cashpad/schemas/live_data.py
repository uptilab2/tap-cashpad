from .models import *

LIVE_DATA = {
    "type": ["null", "object"],
    "additionalProperties": False,
    "properties": {
        "ingestion_date": {
            "type": ["null", "string"]
        },
        "total": TOTAL,
        "sales": SALE,
        "staff": STAFF,
        "payments": {
            "type": ["null", "array"],
            "items": PAYMENT
        },
        "receipts": {
            "type": ["null", "array"],
            "items": RECEIPT
        },
        "cashmanager_snapshots": {
            "type": ["null", "array"],
            "items": CASHMANAGER_SNAPSHOT,
        }
    }
}