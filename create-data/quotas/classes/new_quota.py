import classes.functions as f
import classes.globals as g
import datetime
import sys


class new_quota(object):
    def __init__(self, quota_order_number_id, goods_nomenclature_item_id, duty_rate, measure_type_id="122"):
        self.quota_order_number_id = quota_order_number_id
        self.goods_nomenclature_item_id = goods_nomenclature_item_id
        self.duty_rate = duty_rate
        self.measure_type_id = measure_type_id
