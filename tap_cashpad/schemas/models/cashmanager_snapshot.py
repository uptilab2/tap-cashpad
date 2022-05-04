from .cashbox import CASHBOX
from .inventory import INVENTORY


CASHMANAGER_SNAPSHOT = {
    "type": ["null", "object"],
    "properties": {
        "date": {
            "type": ["null", "string"],
            "format": "date-time"
        },
        "cashmanager": {
            "type": ["null", "string"]
        },
        "inventory": {
            "type": ["null", "array"],
            "items": INVENTORY,
        },
        "cashbox": {
            "type": ["null", "array"],
            "items": CASHBOX,
        },
    }
} 