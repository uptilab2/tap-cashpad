from .predefined_discount import PREDEFINED_DISCOUNT

DISCOUNT = {
    "type": ["null", "object"],
    "properties": {
        "discount_offered": {
            "type": ["null", "boolean"]
        },
        "discount_amount": {
            "type": ["null", "number"]
        },
        "discount_percentage": {
            "type": ["null", "number"]
        },
        "discount_total": {
            "type": ["null", "number"]
        },
        "predefineddiscount": PREDEFINED_DISCOUNT
    }
}