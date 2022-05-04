from .product_summary_product import PRODUCT_SUMMARY_PRODUCT


PRODUCT_SUMMARY_SALE = {
    "type": ["null", "object"],
    "properties": {
        "products": {
            "type": ["null", "array"],
            "items": PRODUCT_SUMMARY_PRODUCT,
        },
        "nb_products": {
            "type": ["null", "integer"]
        },
        "total_incl_taxes": {
            "type": ["null", "integer"]
        },
        "total_excl_taxes": {
            "type": ["null", "integer"]
        },
    }
}