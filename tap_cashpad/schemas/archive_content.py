from .models import *

ARCHIVE_CONTENT = {
    "type": ["null", "object"],
    "additionalProperties": False,
    "properties": {
        "ingestion_date": {
            "type": ["null", "string"]
        },
        "is_closed": {
            "type": ["null", "boolean"]
        },
        "sequential_id": {
            "type": ["null", "integer"]
        },
        "date_created": {
            "type": ["null", "string"],
            "format": "date-time"
        },
        "user" : USER,
        "range_begin_date": {
            "type": ["null", "string"],
            "format": "date-time"
        },
        "range_end_date": {
            "type": ["null", "string"],
            "format": "date-time"
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