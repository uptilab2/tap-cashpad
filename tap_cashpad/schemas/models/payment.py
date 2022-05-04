from .payment_method import PAYMENT_METHOD
from .user import USER

PAYMENT = {
    "type": ["null", "object"],
    "properties": {
        "user": USER,
        "paymentmethod": PAYMENT_METHOD,
        "amount": {
            "type": ["null", "number"]
        },
        "nb_operations": {
            "type": ["null", "integer"]
        },
        "date": {
            "type": ["null", "string"],
            "format": "date-time"
        },
        "lunch_voucher": {
            "type": ["null", "string"]
        },
    }
}