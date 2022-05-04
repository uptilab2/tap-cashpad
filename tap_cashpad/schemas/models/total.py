from .tax import TAX

TOTAL = {
    "type": ["null", "object"],
    "properties": {
        "nb_receipts": {
            "type": ["null", "integer"]
        },
        "nb_cancelled_receipts": {
            "type": ["null", "integer"]
        },
        "nb_seats": {
            "type": ["null", "integer"]
        },
        "total_with_taxes": {
            "type": ["null", "number"]
        },
        "total_without_taxes": {
            "type": ["null", "number"]
        },
        "taxes": { 
            "type": ["null", "array"],
            "items": TAX
        },
    }
}