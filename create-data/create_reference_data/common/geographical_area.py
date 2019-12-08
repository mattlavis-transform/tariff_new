import psycopg2
import sys
import os
import csv
import re
from xml.dom.minidom import Text, Element

import common.functions as fn
import common.globals as g


class geographical_area(object):
    def __init__(self, geographical_area_sid, geographical_area_id, geographical_area_code, description, stype):
        self.geographical_area_sid = fn.mstr(geographical_area_sid)
        self.geographical_area_id = fn.mstr(geographical_area_id)
        self.geographical_area_code = fn.mstr(geographical_area_code)
        self.description = fn.mstr(description)
        self.type = fn.mstr(stype)
        self.needs_change = False
        self.cnt = 0
        self.xml = ""

    def writeXML(self, app):
        if self.type == "update":
            out = app.update_geographical_area_description_XML
        elif self.type == "insert":
            out = app.insert_geographical_area_XML
        else:
            pass

        self.description = fn.cleanse(self.description)
        out = out.replace("[GEOGRAPHICAL_AREA_SID]", self.geographical_area_sid)
        out = out.replace("[GEOGRAPHICAL_AREA_ID]", self.geographical_area_id)
        out = out.replace("[GEOGRAPHICAL_AREA_CODE]", self.geographical_area_code)
        out = out.replace("[DESCRIPTION]", self.description)
        out = out.replace("[VALIDITY_START_DATE]", fn.mdate(app.critical_date_plus_one))
        out = out.replace("[GEOGRAPHICAL_AREA_DESCRIPTION_PERIOD_SID]", str(app.last_geographical_area_description_period_sid))
        out = out.replace("[TRANSACTION_ID]", str(app.transaction_id))
        out = out.replace("[MESSAGE_ID1]", str(app.message_id))
        out = out.replace("[MESSAGE_ID2]", str(app.message_id + 1))
        out = out.replace("[MESSAGE_ID3]", str(app.message_id + 2))
        out = out.replace("[RECORD_SEQUENCE_NUMBER1]", str(app.message_id))
        out = out.replace("[RECORD_SEQUENCE_NUMBER2]", str(app.message_id + 1))
        out = out.replace("[RECORD_SEQUENCE_NUMBER3]", str(app.message_id + 2))

        self.xml = out

        app.last_geographical_area_description_period_sid += 1
        app.transaction_id += 1
        if self.type == "update":
            app.message_id += 2
        else:
            app.message_id += 3
