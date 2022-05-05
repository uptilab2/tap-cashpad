from .models import *


SALES_SUMMARY = {
    "type": ["null", "object"],
    "additionalProperties": False,
    "properties": {
        "ingestion_date": {
          "type": ["null", "string"]
        },
        "id": { 
            "type": ["null", "string"]
        },
        "sequential_id": {
          "type": ["null", "integer"]
        },
        "date_created": {
          "type": ["null", "string"],
          "format": "date-time"
        },
        "range_begin_date": {
          "type": ["null", "string"],
          "format": "date-time"
        },
        "range_end_date": {
          "type": ["null", "string"],
          "format": "date-time"
        },
        "total_sales": SALE_SUMMARY_SALE,
        "sales": {
            "type": ["null", "array"],
            "items": {
                "type": ["null", "object"],
                "properties": {
                    "location": BASE_MODEL,
                    "consumptionmode": BASE_MODEL,
                    **SALE_SUMMARY_SALE.get('properties')
                }
            }
        }
    }
}