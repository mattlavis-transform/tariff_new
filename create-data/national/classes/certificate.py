import classes.functions as f
import classes.globals as g
import datetime
import sys

class certificate(object):
	def __init__(self, certificate_type_code, certificate_code, validity_start_date, validity_end_date, description, certificate_description_period_sid):
		# from parameters
		self.certificate_type_code              = certificate_type_code
		self.certificate_code                   = certificate_code
		self.validity_start_date                = validity_start_date
		self.validity_end_date                  = validity_end_date
		self.description                        = description
		self.certificate_description_period_sid = certificate_description_period_sid

	def xml(self):
		return ""
		app = g.app

		#if app.retain == False and app.vat_excise == False:
		#	return ""
		
		if app.retain == False:
			self.certificate_description_period_sid = app.last_certificate_description_period_sid

		s = app.template_certificate
		s = s.replace("[TRANSACTION_ID]",           str(app.transaction_id))
		s = s.replace("[MESSAGE_ID1]",              str(app.sequence_id))
		s = s.replace("[MESSAGE_ID2]",              str(app.sequence_id + 1))
		s = s.replace("[MESSAGE_ID3]",              str(app.sequence_id + 2))
		s = s.replace("[RECORD_SEQUENCE_NUMBER1]",  str(app.sequence_id))
		s = s.replace("[RECORD_SEQUENCE_NUMBER2]",  str(app.sequence_id + 1))
		s = s.replace("[RECORD_SEQUENCE_NUMBER3]",  str(app.sequence_id + 2))
		s = s.replace("[UPDATE_TYPE]",              "3")

		s = s.replace("[CERTIFICATE_TYPE_CODE]",                f.mstr(self.certificate_type_code))
		s = s.replace("[CERTIFICATE_CODE]",                     f.mstr(self.certificate_code))
		s = s.replace("[CERTIFICATE_DESCRIPTION_PERIOD_SID]",   f.mstr(self.certificate_description_period_sid))
		s = s.replace("[VALIDITY_START_DATE]",                  f.mdate(self.validity_start_date))
		s = s.replace("[VALIDITY_END_DATE]",                    f.mdate(self.validity_end_date))
		s = s.replace("[DESCRIPTION]",                          f.mcleanse(self.description))

		s = s.replace("\t\t\t\t\t\t<oub:validity.end.date></oub:validity.end.date>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:description></oub:description>\n", "")
		app.sequence_id += 3
		app.transaction_id += 1
		app.last_certificate_description_period_sid += 1
		return (s)
