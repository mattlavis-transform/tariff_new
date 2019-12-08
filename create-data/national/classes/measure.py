import classes.functions as f
import classes.globals as g
import datetime
from datetime import datetime
import sys


class measure(object):
    def __init__(self, measure_sid, measure_type_id, geographical_area_id, goods_nomenclature_item_id, additional_code_type_id,
    additional_code_id, ordernumber, reduction_indicator, validity_start_date, measure_generating_regulation_role,
    measure_generating_regulation_id, validity_end_date, justification_regulation_role, justification_regulation_id,
    stopped_flag, geographical_area_sid, goods_nomenclature_sid, additional_code_sid, export_refund_nomenclature_sid):
        # from parameters
        self.measure_sid = measure_sid
        self.quota_order_number_id = ordernumber
        self.measure_type_id = measure_type_id
        self.validity_start_date = validity_start_date
        self.validity_end_date = validity_end_date
        self.geographical_area_id = geographical_area_id
        self.goods_nomenclature_item_id = goods_nomenclature_item_id
        self.additional_code_type_id = additional_code_type_id
        self.additional_code_id = additional_code_id
        self.reduction_indicator = reduction_indicator
        self.measure_generating_regulation_role = measure_generating_regulation_role
        self.measure_generating_regulation_id = measure_generating_regulation_id
        self.justification_regulation_role = justification_regulation_role
        self.justification_regulation_id = justification_regulation_id
        self.stopped_flag = stopped_flag
        self.geographical_area_sid = geographical_area_sid
        self.goods_nomenclature_sid = goods_nomenclature_sid
        self.additional_code_sid = additional_code_sid
        self.export_refund_nomenclature_sid = export_refund_nomenclature_sid

        # Initialised
        self.measure_component_list = []
        self.measure_condition_list = []
        self.measure_footnote_list = []
        self.measure_excluded_geographical_area_list = []

        self.adjust_dates()

    def adjust_dates(self):
        if g.app.retain is True or g.app.vat is True or g.app.excise is True:
            self.validity_start_date = datetime.strftime(self.validity_start_date, "%Y-%m-%dT%H:%M:%S")
            if self.validity_end_date is not None:
                self.validity_end_date = datetime.strftime(self.validity_end_date, "%Y-%m-%dT%H:%M:%S")

        if not(g.app.retain) and not(g.app.vat_excise):
            self.validity_start_date = g.app.critical_date_plus_one.strftime('%Y-%m-%d')
            self.validity_end_date = ""

    def csv(self):
        s = ""
        s += f.mstr(self.goods_nomenclature_item_id) + ", "
        s += f.mstr(self.measure_type_id) + ", "
        s += f.mstr(self.validity_start_date) + ", "
        s += f.mstr(self.validity_end_date) + "\n"
        return s

    def xml(self):
        app = g.app
        s = app.template_measure

        self.component_content = ""  # Needed
        self.condition_content = ""  # Needed
        self.footnote_content = ""  # Needed
        self.excluded_geographical_area_content = ""  # Needed

        self.update_type = "3"
        s = s.replace("[TRANSACTION_ID]", str(app.transaction_id))
        s = s.replace("[MESSAGE_ID]", str(app.sequence_id))
        s = s.replace("[RECORD_SEQUENCE_NUMBER]", str(app.sequence_id))
        s = s.replace("[UPDATE_TYPE]", str(self.update_type))

        if app.vat_excise is False:
            if app.retain is False:
                # Give the measure a new sid (positive) - except CVD
                # CVD is already used in an EU measure: do not double up

                if self.measure_type_id == "CVD":
                    return ""

                # Transfer over to the new measure types
                to_list = ['350', '351', '352', '353', '354', '355', '356', '357', '358', '359', '360', '361', '362', '363']
                from_list = ['AHC', 'AIL', 'ATT', 'CEX', 'COE', 'COI', 'CVD', 'EQC', 'HOP', 'HSE', 'PHC', 'PRE', 'PRT', 'QRC']
                reg_list = ['I19', ]
                try:
                    pos = from_list.index(self.measure_type_id)
                    self.measure_type_id = to_list[pos]
                    self.measure_generating_regulation_id = "I1900" + self.measure_type_id
                except:
                    print(self.measure_sid)
                    sys.exit()

                # Also move over to the new geographical area types
                try:
                    my_index = g.app.geographical_area_id_from_list.index(self.geographical_area_id)
                    self.geographical_area_id = g.app.geographical_area_id_to_list[my_index]
                except:
                    pass

                try:
                    my_index = g.app.geographical_area_sid_from_list.index(self.geographical_area_sid)
                    self.geographical_area_sid = g.app.geographical_area_sid_to_list[my_index]
                except:
                    pass

                self.measure_sid = app.last_measure_sid

                # And assign it to the dependent objects
                for obj in self.measure_component_list:
                    obj.measure_sid = self.measure_sid

                for obj in self.measure_condition_list:
                    obj.measure_sid = self.measure_sid

                for obj in self.measure_excluded_geographical_area_list:
                    obj.measure_sid = self.measure_sid

                for obj in self.measure_footnote_list:
                    obj.measure_sid = self.measure_sid

        s = s.replace("[MEASURE_SID]", f.mstr(self.measure_sid))
        s = s.replace("[MEASURE_TYPE_ID]", f.mstr(self.measure_type_id))
        s = s.replace("[GEOGRAPHICAL_AREA_ID]", f.mstr(self.geographical_area_id))
        s = s.replace("[GOODS_NOMENCLATURE_ITEM_ID]", f.mstr(self.goods_nomenclature_item_id))
        s = s.replace("[VALIDITY_START_DATE]", f.mstr(self.validity_start_date))
        s = s.replace("[MEASURE_GENERATING_REGULATION_ROLE]", f.mstr(self.measure_generating_regulation_role))
        s = s.replace("[MEASURE_GENERATING_REGULATION_ID]", f.mstr(self.measure_generating_regulation_id))
        s = s.replace("[VALIDITY_END_DATE]", f.mstr(self.validity_end_date))
        s = s.replace("[JUSTIFICATION_REGULATION_ROLE]", f.mstr(self.justification_regulation_role))
        s = s.replace("[JUSTIFICATION_REGULATION_ID]", f.mstr(self.justification_regulation_id))
        s = s.replace("[STOPPED_FLAG]", f.mbool(self.stopped_flag))
        s = s.replace("[GEOGRAPHICAL_AREA_SID]", f.mstr(self.geographical_area_sid))
        s = s.replace("[GOODS_NOMENCLATURE_SID]", f.mstr(self.goods_nomenclature_sid))
        s = s.replace("[ORDERNUMBER]", f.mstr(self.quota_order_number_id))
        s = s.replace("[ADDITIONAL_CODE_TYPE_ID]", f.mstr(self.additional_code_type_id))
        s = s.replace("[ADDITIONAL_CODE_ID]", f.mstr(self.additional_code_id))
        s = s.replace("[ADDITIONAL_CODE_SID]", f.mstr(self.additional_code_sid))
        s = s.replace("[REDUCTION_INDICATOR]", f.mstr(self.reduction_indicator))
        s = s.replace("[EXPORT_REFUND_NOMENCLATURE_SID]", f.mstr(self.export_refund_nomenclature_sid))

        s = s.replace("\t\t\t\t\t\t<oub:validity.end.date></oub:validity.end.date>\n", "")
        s = s.replace("\t\t\t\t\t\t<oub:goods.nomenclature.item.id></oub:goods.nomenclature.item.id>\n", "")
        s = s.replace("\t\t\t\t\t\t<oub:additional.code.type></oub:additional.code.type>\n", "")
        s = s.replace("\t\t\t\t\t\t<oub:additional.code></oub:additional.code>\n", "")
        s = s.replace("\t\t\t\t\t\t<oub:ordernumber></oub:ordernumber>\n", "")
        s = s.replace("\t\t\t\t\t\t<oub:reduction.indicator></oub:reduction.indicator>\n", "")
        s = s.replace("\t\t\t\t\t\t<oub:justification.regulation.role></oub:justification.regulation.role>\n", "")
        s = s.replace("\t\t\t\t\t\t<oub:justification.regulation.id></oub:justification.regulation.id>\n", "")
        s = s.replace("\t\t\t\t\t\t<oub:geographical.area.sid></oub:geographical.area.sid>\n", "")
        s = s.replace("\t\t\t\t\t\t<oub:goods.nomenclature.sid></oub:goods.nomenclature.sid>\n", "")
        s = s.replace("\t\t\t\t\t\t<oub:additional.code.sid></oub:additional.code.sid>\n", "")
        s = s.replace("\t\t\t\t\t\t<oub:export.refund.nomenclature.sid></oub:export.refund.nomenclature.sid>\n", "")

        if app.retain is False:
            app.last_measure_sid += 1

        app.sequence_id += 1

        for obj in self.measure_excluded_geographical_area_list:
            self.excluded_geographical_area_content += obj.xml()

        for obj in self.measure_component_list:
            self.component_content += obj.xml()

        for obj in self.measure_condition_list:
            self.condition_content += obj.xml()

        for obj in self.measure_footnote_list:
            self.footnote_content += obj.xml()

        s = s.replace("[COMPONENTS]\n", self.component_content)
        s = s.replace("[CONDITIONS]\n", self.condition_content)
        s = s.replace("[EXCLUDED_GEOGRAPHICAL_AREAS]\n", self.excluded_geographical_area_content)
        s = s.replace("[FOOTNOTES]\n", self.footnote_content)

        app.transaction_id += 1

        return (s)
