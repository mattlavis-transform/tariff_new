import classes.functions as f
import classes.globals as g
import datetime
import sys

class certificate_type(object):
	def __init__(self, certificate_type_code, validity_start_date, validity_end_date, description):
		# from parameters
		self.certificate_type_code  = certificate_type_code
		self.validity_start_date    = validity_start_date
		self.validity_end_date      = validity_start_date
		self.description            = description

	def xml(self):
		return ""
		app = g.app

		if app.vat_excise == True or app.retain == False:
			return ("")
		else:
			s = app.template_certificate_type
			s = s.replace("[TRANSACTION_ID]",              str(app.transaction_id))
			s = s.replace("[MESSAGE_ID1]",                 str(app.sequence_id))
			s = s.replace("[MESSAGE_ID2]",                 str(app.sequence_id + 1))
			s = s.replace("[RECORD_SEQUENCE_NUMBER1]",     str(app.sequence_id))
			s = s.replace("[RECORD_SEQUENCE_NUMBER2]",     str(app.sequence_id + 1))
			s = s.replace("[UPDATE_TYPE]",                 "3")

			s = s.replace("[CERTIFICATE_TYPE_CODE]",            f.mstr(self.certificate_type_code))
			s = s.replace("[VALIDITY_START_DATE]",         "1970-01-01")
			s = s.replace("[VALIDITY_END_DATE]",           "")
			s = s.replace("[DESCRIPTION]",                 f.mstr(self.description))

			s = s.replace("\t\t\t\t\t\t<oub:validity.end.date></oub:validity.end.date>\n", "")
			s = s.replace("\t\t\t\t\t\t<oub:description></oub:description>\n", "")
			app.sequence_id += 2
			app.transaction_id += 1
			return (s)
