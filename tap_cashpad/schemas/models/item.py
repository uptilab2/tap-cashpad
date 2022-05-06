from .product import PRODUCT
from .discount import DISCOUNT
from .tax import TAX
from .addon import ADDON
from .menu_value_tax import MENU_VALUE_TAX

ITEM = {
    "type": ["null", "object"],
    "properties": {
        "id": {
            "type": ["null", "string"]
        },
        "type": {
            "type": ["null", "string"]
        },
        "quantity": {
            "type": ["null", "number"]
        },
        "final_price": {
            "type": ["null", "number"]
        },
        "unit_price": {
            "type": ["null", "number"]
        },
        "product": PRODUCT,
        "discount":  DISCOUNT,
        "menu": {
            "type": ["null", "string"]
        },
        "taxes": {
            "type": ["null", "array"],
            "items": TAX,
        },
        "addons": {
            "type": ["null", "array"],
            "items": ADDON,
        },
        "menu_value_incl_taxes": {
            "type": ["null", "number"]
        },
        "menu_value_taxes": {
            "type": ["null", "array"],
            "items": MENU_VALUE_TAX,
        },
    }
}