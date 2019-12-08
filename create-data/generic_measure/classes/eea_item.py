
class eea_item(object):
    def __init__(self, geographical_area_id, measure_sid, goods_nomenclature_item_id, validity_start_date,
    validity_end_date, duty_expression_id, duty_amount, monetary_unit_code, measurement_unit_code, measurement_unit_qualifier_code):
        # from parameters
        self.geographical_area_id = geographical_area_id
        self.measure_sid = measure_sid
        self.goods_nomenclature_item_id = goods_nomenclature_item_id
        self.validity_start_date = validity_start_date
        self.validity_end_date = validity_end_date
        self.duty_expression_id = duty_expression_id
        self.duty_amount = duty_amount
        self.monetary_unit_code = monetary_unit_code
        self.measurement_unit_code = measurement_unit_code
        self.measurement_unit_qualifier_code = measurement_unit_qualifier_code
