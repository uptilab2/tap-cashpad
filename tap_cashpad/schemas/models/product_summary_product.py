
from .base_model import BASE_MODEL
from .product_summary_product_sale import PRODUCT_SUMMARY_PRODUCT_SALE


PRODUCT_SUMMARY_PRODUCT = {
    "type": ["null", "object"],
    "properties": {
        "product": BASE_MODEL,
        "category": BASE_MODEL,
        "type": {
            "type": ["null", "string"],
        },
        "unit": {
            "type": ["null", "object"],
            "properties": {
                "unit": {
                    "type": ["null", "string"],
                },
                "type": {
                    "type": ["null", "string"],
                }

            }
        },
        "total": PRODUCT_SUMMARY_PRODUCT_SALE,
        "menu": PRODUCT_SUMMARY_PRODUCT_SALE,
        "carte": PRODUCT_SUMMARY_PRODUCT_SALE,
    }
}