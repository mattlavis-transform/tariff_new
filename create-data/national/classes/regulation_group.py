import classes.functions as f
import classes.globals as g
import datetime
import sys

class regulation_group(object):
	def __init__(self, regulation_group_id, validity_start_date, validity_end_date, description):
		# from parameters
		self.regulation_group_id = regulation_group_id
		self.validity_start_date = validity_start_date
		self.validity_end_date   = validity_end_date
		self.description         = description


	def xml(self):
		app = g.app
		if (app.retain == False) and (app.vat_excise == False):
			return ""
		else:


			if self.validity_start_date is None:
				self.validity_start_date = "1970-01-01"
			s = app.template_regulation_group
			s = s.replace("[TRANSACTION_ID]",              str(app.transaction_id))
			s = s.replace("[MESSAGE_ID1]",                 str(app.sequence_id))
			s = s.replace("[MESSAGE_ID2]",                 str(app.sequence_id + 1))
			s = s.replace("[RECORD_SEQUENCE_NUMBER1]",     str(app.sequence_id))
			s = s.replace("[RECORD_SEQUENCE_NUMBER2]",     str(app.sequence_id + 1))
			s = s.replace("[UPDATE_TYPE]",                 "3")

			s = s.replace("[REGULATION_GROUP_ID]",          f.mstr(self.regulation_group_id))
			s = s.replace("[VALIDITY_START_DATE]",          f.mdate(self.validity_start_date))
			s = s.replace("[VALIDITY_END_DATE]",            "")
			s = s.replace("[DESCRIPTION]",                  f.mstr(self.description))

			s = s.replace("\t\t\t\t\t\t<oub:validity.end.date></oub:validity.end.date>\n", "")
			s = s.replace("\t\t\t\t\t\t<oub:description></oub:description>\n", "")
			app.sequence_id += 2
			app.transaction_id += 1
			return (s)
