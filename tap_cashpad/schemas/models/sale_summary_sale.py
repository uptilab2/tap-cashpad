from .tax import TAX


SALE_SUMMARY_SALE = {
    "type": ["null", "object"],
    "properties": {
        "sales_incl_taxes": {
            "type": ["null", "integer"]
        },
        "sales_excl_taxes": {
            "type": ["null", "integer"]
        },
        "nb_seats": {
            "type": ["null", "integer"]
        },
        "nb_receipts": {
            "type": ["null", "integer"]
        },
        "taxes": {
            "type": ["null", "array"],
            "items": TAX
        }
    }
}