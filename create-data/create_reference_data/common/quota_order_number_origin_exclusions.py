import psycopg2
import sys
import os
import csv
import re
from xml.dom.minidom import Text, Element

import common.globals as g
import common.functions as fn


class quota_order_number_origin_exclusions(object):
    def __init__(self, geographical_area_id, quota_order_number_origin_sid):
        self.geographical_area_id = fn.mstr(geographical_area_id)
        self.quota_order_number_origin_sid = fn.mstr(quota_order_number_origin_sid)
        self.get_geographical_area_sid()
        self.update_type = "3"
        self.measure_sid_list = []

    def get_measures(self):
        # This does not work - I don't know if it needs to work
        sql = """select distinct measure_sid from measures where validity_start_date >= %s
        and measure_type_id in ('122', '123', '143', '145') and ordernumber = %s order by 1;"""

        params = [g.app.critical_date_plus_one]
        params.append(self.quota_order_number_id)
        cur = g.app.conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        if len(rows) > 0:
            for row in rows:
                measure_sid = row[0]
                self.measure_sid_list.append(measure_sid)

    def get_geographical_area_sid(self):
        sql = """select geographical_area_sid from geographical_areas
        where geographical_area_id = %s order by validity_start_date desc limit 1"""
        params = []
        params.append(self.geographical_area_id)
        cur = g.app.conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        if len(rows) > 0:
            self.geographical_area_sid = rows[0][0]
        else:
            self.geographical_area_sid = -1

    def xml(self):
        out = "<!-- Begin quota origin exclusions for quota " + self.quota_order_number_id + " -->\n"
        out += g.app.quota_order_number_origin_exclusions_XML

        out = out.replace("[QUOTA_ORDER_NUMBER_ORIGIN_SID]", str(self.quota_order_number_origin_sid))
        out = out.replace("[GEOGRAPHICAL_AREA_SID]", str(self.geographical_area_sid))
        out = out.replace("[UPDATE_TYPE]", self.update_type)
        out = out.replace("[TRANSACTION_ID]", str(g.app.transaction_id))
        out = out.replace("[MESSAGE_ID1]", str(g.app.message_id))
        out = out.replace("[RECORD_SEQUENCE_NUMBER1]", str(g.app.message_id))

        out += "<!-- End quota origin exclusions for quota " + self.quota_order_number_id + " -->\n"

        g.app.transaction_id += 1
        g.app.message_id += 1

        return out

    def measure_xml(self):
        template = g.app.measure_excluded_geographical_area_XML
        xml = "<!-- Begin measure excluded geographical area XML for quota " + self.quota_order_number_id + " -->\n"
        for measure_sid in self.measure_sid_list:
            out = template
            out = out.replace("[MEASURE_SID]", str(measure_sid))
            out = out.replace("[QUOTA_ORDER_NUMBER_ORIGIN_SID]", str(self.geographical_area_sid))
            out = out.replace("[GEOGRAPHICAL_AREA_SID]", str(self.geographical_area_sid))
            out = out.replace("[GEOGRAPHICAL_AREA_ID]", str(self.geographical_area_id))
            out = out.replace("[UPDATE_TYPE]", self.update_type)
            out = out.replace("[TRANSACTION_ID]", str(g.app.transaction_id))
            out = out.replace("[MESSAGE_ID]", str(g.app.message_id))
            out = out.replace("[RECORD_SEQUENCE_NUMBER1]", str(g.app.message_id))

            xml += out

            g.app.transaction_id += 1
            g.app.message_id += 1

        xml += "<!-- End measure excluded geographical area XML for quota " + self.quota_order_number_id + " -->\n"
        return xml
