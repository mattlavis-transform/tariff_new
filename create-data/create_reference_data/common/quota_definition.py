import psycopg2
import sys
import os
import csv
import re
from xml.dom.minidom import Text, Element

import common.globals as g
import common.functions as fn


class quota_definition(object):
    def __init__(self):
        self.update_type = "1"

    def xml(self):
        out = "<!-- Begin quota definition for quota " + self.quota_order_number_id + " -->\n"
        out += g.app.quota_definition_XML

        out = out.replace("[QUOTA_DEFINITION_SID]", str(self.quota_definition_sid))
        out = out.replace("[QUOTA_ORDER_NUMBER_ID]", str(self.quota_order_number_id))
        out = out.replace("[VALIDITY_START_DATE]", fn.mdate(self.validity_start_date))
        out = out.replace("[VALIDITY_END_DATE]", fn.mdate(self.validity_end_date))
        out = out.replace("[QUOTA_ORDER_NUMBER_SID]", str(self.quota_order_number_sid))
        out = out.replace("[VOLUME]", str(self.volume))
        out = out.replace("[INITIAL_VOLUME]", str(self.initial_volume))
        out = out.replace("[MEASUREMENT_UNIT_CODE]", str(self.measurement_unit_code))
        out = out.replace("[MAXIMUM_PRECISION]", str(self.maximum_precision))
        out = out.replace("[CRITICAL_STATE]", str(self.critical_state))
        out = out.replace("[CRITICAL_THRESHOLD]", str(self.critical_threshold))
        out = out.replace("[DESCRIPTION]", str(self.description))

        out = out.replace("[UPDATE_TYPE]", self.update_type)
        out = out.replace("[TRANSACTION_ID]", str(g.app.transaction_id))
        out = out.replace("[MESSAGE_ID1]", str(g.app.message_id))
        out = out.replace("[RECORD_SEQUENCE_NUMBER1]", str(g.app.message_id))

        out += "<!-- End quota definition for quota " + self.quota_order_number_id + " -->\n"

        g.app.transaction_id += 1
        g.app.message_id += 1

        return out
