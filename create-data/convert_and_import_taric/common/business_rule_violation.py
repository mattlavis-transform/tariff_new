from datetime import datetime
import os


class business_rule_violation(object):
    def __init__(self, id, description, operation, transaction_id, message_id, record_code, sub_record_code, pk):
        self.id = id
        self.description = description
        self.operation = operation
        self.transaction_id = transaction_id
        self.message_id = message_id
        self.record_code = record_code
        self.sub_record_code = sub_record_code
        self.pk = pk

        self.message = "Business rule violation "
        self.message += self.id + " " + self.msg
        self.message += " on message " + self.message_id + " in transaction " + self.transaction_id
        self.message += ". Affects primary key " + pk
