from .discount import DISCOUNT
from .tax import TAX


PRODUCT_SUMMARY_PRODUCT_SALE = {
    "type": ["null", "object"],
    "properties": {
        "quantity": {
            "type": ["null", "integer"],
        },
        "sales_incl_taxes": {
            "type": ["null", "integer"],
        },
        "sales_excl_taxes": {
            "type": ["null", "integer"],
        },
        "discount": DISCOUNT,
        "taxes":{
            "type": ["null", "array"],
            "items": TAX,
        },
    }
}