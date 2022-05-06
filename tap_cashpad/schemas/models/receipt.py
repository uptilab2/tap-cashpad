from .discount import DISCOUNT
from .owner import OWNER
from .location import LOCATION
from .consumptionmode import CONSUMPTIONMODE
from .tax import TAX
from .payment import PAYMENT
from .item import ITEM
from .customer import CUSTOMER

RECEIPT = {
    "type": ["null", "object"],
    "properties": {
        "id": {
            "type": ["null", "string"]
        },
        "sequential_id": {
            "type": ["null", "integer"]
        },
        "period_id": {
            "type": ["null", "integer"]
        },
        "date_created": {
            "type": ["null", "string"],
            "format": "date-time"
        },
        "date_closed": {
            "type": ["null", "string"],
            "format": "date-time"
        },
        "version": {
            "type": ["null", "integer"],
        },
        "discount": DISCOUNT,
        "cancelled": {
            "type": ["null", "boolean"],
        },
        "cancellation_reason": {
            "type": ["null", "string"],
        },
        "staff": {
            "type": ["null", "boolean"],
        },
        "loyaltycard": {
            "type": ["null", "string"],
        },
        "total_with_taxes": {
            "type": ["null", "number"],
        },
        "notes": {
            "type": ["null", "array"],
            "items": {
                "type": ["null", "string"],
            },
        },
        "nb_seats": {
            "type": ["null", "integer"],
        },
        "owner": OWNER,
        "table": {
            "type": ["null", "integer"],
        }, 
        "location": LOCATION,
        "consumptionmode": CONSUMPTIONMODE,
        "taxes": {
            "type": ["null", "array"],
            "items": TAX,
        },
        "payments": {
            "type": ["null", "array"],
            "items": PAYMENT,
        },
        "items": {
            "type": ["null", "array"],
            "items": ITEM,
        },
        "customer": CUSTOMER,
    }
}