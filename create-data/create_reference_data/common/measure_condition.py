import psycopg2
import sys
import os
import csv
import re
import common.functions as fn
import common.globals as g
from unidecode import unidecode


class measure_condition(object):
    def __init__(self, measure_sid, update_type, condition_code, component_sequence_number, action_code, certificate_type_code, certificate_code):
        self.measure_sid = fn.mstr(measure_sid)
        self.update_type = fn.mstr(update_type)
        self.condition_code = fn.mstr(condition_code).upper()
        self.component_sequence_number = fn.mstr(component_sequence_number)
        self.action_code = fn.mstr(action_code)
        self.certificate_type_code = fn.mstr(certificate_type_code).upper()
        self.certificate_code = fn.mstr(certificate_code)
        self.xml = ""

    def writeXML(self):
        out = g.app.measure_condition_XML
        out = out.replace("[MEASURE_SID]", self.measure_sid)
        out = out.replace("[UPDATE_TYPE]", self.update_type)
        out = out.replace("[CONDITION_CODE]", self.condition_code)
        out = out.replace("[COMPONENT_SEQUENCE_NUMBER]", self.component_sequence_number)
        out = out.replace("[ACTION_CODE]", self.action_code)
        out = out.replace("[CERTIFICATE_TYPE_CODE]", self.certificate_type_code)
        out = out.replace("[CERTIFICATE_CODE]", self.certificate_code)

        out = out.replace("[MEASURE_CONDITION_SID]", str(g.app.last_measure_condition_sid))
        out = out.replace("[TRANSACTION_ID]", str(g.app.transaction_id))
        out = out.replace("[MESSAGE_ID]", str(g.app.message_id))
        out = out.replace("[RECORD_SEQUENCE_NUMBER]", str(g.app.message_id))

        out = out.replace("\t\t\t\t\t\t<oub:certificate.type.code></oub:certificate.type.code>\n", "")
        out = out.replace("\t\t\t\t\t\t<oub:certificate.code></oub:certificate.code>\n", "")

        self.xml = out

        g.app.transaction_id += 1
        g.app.message_id += 1
        g.app.last_measure_condition_sid += 1
