import psycopg2
import sys
import os
import csv
import re
import common.functions as fn
from unidecode import unidecode
import common.objects as o
from common.measure_component import measure_component


class master_commodity(object):
    def __init__(self, goods_nomenclature_item_id, productline_suffix, number_indents, significant_digits, description, leaf):
        self.goods_nomenclature_item_id = fn.mstr(goods_nomenclature_item_id)
        self.productline_suffix = fn.mstr(productline_suffix)
        self.number_indents = int(number_indents)
        self.significant_digits = int(significant_digits)
        self.leaf = int(leaf)
        self.description = fn.mstr(description)

        self.advalorem = ""
        self.specific = ""
        self.minimum = ""
        self.parent_goods_nomenclature_item_id = ""
        self.parent_productline_suffix = ""
        self.measure_components = []
        self.component_xml = ""

        if (self.goods_nomenclature_item_id in o.app.measure_105_list) and self.productline_suffix == "80":
            self.measure_type_id = "105"
            self.advalorem = "0"
            self.specific = ""
            self.minimum = ""
            self.status = "Liberalise"
            o.cnt += 1
        else:
            self.measure_type_id = ""
            if self.productline_suffix == "80" and self.significant_digits != 2:
                self.status = ""
            else:
                self.status = "Barred"

    def print_status(self):
        return (self.status + ((10 - len(self.status)) * " "))

    def print_advalorem(self):
        return (self.advalorem + ((10 - len(self.advalorem)) * " "))

    def print_specific(self):
        return (self.specific + ((20 - len(self.specific)) * " "))

    def print_minimum(self):
        return (self.minimum + ((20 - len(self.minimum)) * " "))

    def print_measure_type_id(self):
        return (self.measure_type_id + ((3 - len(self.measure_type_id)) * " "))

    def get_goods_nomenclature_sid(self):
        self.goods_nomenclature_sid = ""
        for obj in o.app.goods_nomenclature_list_for_sids:
            if obj.goods_nomenclature_item_id == self.goods_nomenclature_item_id:
                self.goods_nomenclature_sid = obj.goods_nomenclature_sid
                break
        if self.goods_nomenclature_sid == "":
            print("fucked")
            sys.exit()

    def xml(self):
        s = o.app.measure_XML
        self.get_goods_nomenclature_sid()
        self.measure_sid = o.app.measure_sid
        s = s.replace("[UPDATE_TYPE]", "3")
        s = s.replace("[MEASURE_TYPE_ID]", self.measure_type_id)
        s = s.replace("[GEOGRAPHICAL_AREA_ID]", "1011")
        s = s.replace("[GOODS_NOMENCLATURE_ITEM_ID]", self.goods_nomenclature_item_id)
        s = s.replace("[ADDITIONAL_CODE_TYPE_ID]", "")
        s = s.replace("[ADDITIONAL_CODE_ID]", "")
        s = s.replace("[ORDERNUMBER]", "")
        s = s.replace("[REDUCTION_INDICATOR]", "")
        s = s.replace("[VALIDITY_START_DATE]", o.app.critical_date_plus_one_string)
        s = s.replace("[MEASURE_GENERATING_REGULATION_ROLE]", "1")
        s = s.replace("[MEASURE_GENERATING_REGULATION_ID]", "M2000010")
        s = s.replace("[VALIDITY_END_DATE]", "")
        s = s.replace("[JUSTIFICATION_REGULATION_ROLE]", "")
        s = s.replace("[JUSTIFICATION_REGULATION_ID]", "")
        s = s.replace("[STOPPED_FLAG]", "0")
        s = s.replace("[GEOGRAPHICAL_AREA_SID]", "400")
        s = s.replace("[GOODS_NOMENCLATURE_SID]", str(self.goods_nomenclature_sid))
        s = s.replace("[ADDITIONAL_CODE_SID]", "")
        s = s.replace("[EXPORT_REFUND_NOMENCLATURE_SID]", "")

        s = s.replace("[TRANSACTION_ID]", str(o.app.transaction_id))
        s = s.replace("[RECORD_SEQUENCE_NUMBER]", str(o.app.message_id))
        s = s.replace("[MESSAGE_ID]", str(o.app.message_id))
        s = s.replace("[MEASURE_SID]", str(self.measure_sid))

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

        self.parse_duties()
        s = s.replace("[COMPONENTS]", self.component_xml)
        o.app.transaction_id += 1
        o.app.message_id += 1
        o.app.measure_sid += 1

        return (s)

    def parse_duties(self):
        self.advalorem_duty_added = False
        self.advalorem = self.advalorem.strip()
        if self.advalorem != "":
            mc = measure_component(self.measure_sid, "01", self.advalorem, "", "", "")
            self.component_xml += mc.xml()
            self.advalorem_duty_added = True

        if self.specific != "":
            duty_clauses = self.specific.split("+")
            for duty_clause in duty_clauses:
                duty_clause = duty_clause.strip()
                duty_clause = duty_clause.replace(" %", "%")
                duty_clause = duty_clause.replace("100 kg", "100kg")
                duty_clause = duty_clause.replace("1000 kg", "1000kg")

                if "%" in duty_clause and "EUR/% vol/hl" not in duty_clause:
                    if self.advalorem != "":
                        print("An error in the data")
                        sys.exit()
                    else:
                        self.advalorem_duty_added = True
                        duty_amount = duty_clause.replace("%", "")
                        mc = measure_component(self.measure_sid, "01", duty_amount, "", "", "")
                        self.component_xml += mc.xml()

                else:
                    if self.advalorem_duty_added is True:
                        duty_expression_id = "04"
                    else:
                        duty_expression_id = "01"

                    mc = self.parse_clause(duty_clause, duty_expression_id)

                    self.component_xml += mc.xml()

        if self.minimum != "":
            duty_clauses = self.minimum.split("+")
            for duty_clause in duty_clauses:
                duty_clause = duty_clause.strip()
                duty_clause = duty_clause.replace(" %", "%")
                duty_clause = duty_clause.replace("100 kg", "100kg")
                duty_clause = duty_clause.replace("1000 kg", "1000kg")

                if "%" in duty_clause and "EUR/% vol/hl" not in duty_clause:
                    self.advalorem_duty_added = True
                    duty_amount = duty_clause.replace("%", "")
                    mc = measure_component(self.measure_sid, "01", duty_amount, "", "", "")
                    self.component_xml += mc.xml()

                else:
                    duty_expression_id = "15"
                    mc = self.parse_clause(duty_clause, duty_expression_id)
                    self.component_xml += mc.xml()

    def parse_clause(self, duty_clause, duty_expression_id):
        if "EUR/100kg std qual" in duty_clause:
            duty_amount = duty_clause.replace("EUR/100kg std qual", "").strip()
            measurement_unit = "DTN"
            measurement_qualifier_unit = "R"
            self.advalorem_duty_added = True

        elif "EUR/100kg" in duty_clause:
            duty_amount = duty_clause.replace("EUR/100kg", "").strip()
            measurement_unit = "DTN"
            measurement_qualifier_unit = ""
            self.advalorem_duty_added = True

        elif "EUR/1000kg" in duty_clause:
            duty_amount = duty_clause.replace("EUR/1000kg", "").strip()
            measurement_unit = "TNE"
            measurement_qualifier_unit = ""
            self.advalorem_duty_added = True

        elif "EUR/hl" in duty_clause:
            duty_amount = duty_clause.replace("EUR/hl", "").strip()
            measurement_unit = "HLT"
            measurement_qualifier_unit = ""
            self.advalorem_duty_added = True

        elif "EUR/% vol/hl" in duty_clause:
            duty_amount = duty_clause.replace("EUR/% vol/hl", "").strip()
            measurement_unit = "ASV"
            measurement_qualifier_unit = "X"
            self.advalorem_duty_added = True

        else:
            print(duty_clause, "no match")
            sys.exit()

        mc = measure_component(self.measure_sid, duty_expression_id, duty_amount, "EUR", measurement_unit, measurement_qualifier_unit)
        return mc
