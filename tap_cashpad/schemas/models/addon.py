from .product import PRODUCT
from .tax import TAX

ADDON = {
    "type": ["null", "object"],
    "properties": {
        "id": {
            "type": ["null", "string"]
        },
        "name": {
            "type": ["null", "string"]
        },
        "external_id": {
            "type": ["null", "string"]
        },
        "product": PRODUCT,
        "quantity": {
            "type": ["null", "number"]
        },
        "unit_price": {
            "type": ["null", "integer"]
        },
        "taxes": {
            "type": ["null", "array"],
            "items": TAX,
        },
    }
} 