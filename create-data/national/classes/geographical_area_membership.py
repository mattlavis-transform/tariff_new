import classes.functions as f
import classes.globals as g
import datetime
import sys


class geographical_area_membership(object):
    def __init__(self, geographical_area_sid, geographical_area_group_sid, validity_start_date, validity_end_date):
        # from parameters
        self.geographical_area_sid = geographical_area_sid
        self.geographical_area_group_sid = geographical_area_group_sid
        self.validity_start_date = validity_start_date
        self.validity_end_date = validity_end_date

    def xml(self):
        return ""
        app = g.app

        if g.app.retain is False:

            my_index = g.app.geographical_area_sid_from_list.index(self.geographical_area_group_sid)
            self.geographical_area_group_sid = g.app.geographical_area_sid_to_list[my_index]

        s = app.template_geographical_area_membership
        s = s.replace("[TRANSACTION_ID]", str(app.transaction_id))
        s = s.replace("[MESSAGE_ID1]", str(app.sequence_id))
        s = s.replace("[MESSAGE_ID2]", str(app.sequence_id + 1))
        s = s.replace("[MESSAGE_ID3]", str(app.sequence_id + 2))
        s = s.replace("[RECORD_SEQUENCE_NUMBER1]", str(app.sequence_id))
        s = s.replace("[RECORD_SEQUENCE_NUMBER2]", str(app.sequence_id + 1))
        s = s.replace("[RECORD_SEQUENCE_NUMBER3]", str(app.sequence_id + 2))
        s = s.replace("[UPDATE_TYPE]", "3")

        s = s.replace("[GEOGRAPHICAL_AREA_SID]", f.mstr(self.geographical_area_sid))
        s = s.replace("[GEOGRAPHICAL_AREA_GROUP_SID]", f.mstr(self.geographical_area_group_sid))
        s = s.replace("[VALIDITY_START_DATE]", f.mdate(self.validity_start_date))
        s = s.replace("[VALIDITY_END_DATE]", f.mdate(self.validity_end_date))

        s = s.replace("\t\t\t\t\t\t<oub:validity.end.date></oub:validity.end.date>\n", "")
        app.sequence_id += 1
        app.transaction_id += 1
        return (s)
