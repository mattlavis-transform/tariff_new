import classes.functions as f
import classes.globals as g
import datetime
import sys

class footnote_type(object):
	def __init__(self, footnote_type_id, validity_start_date, validity_end_date, application_code, description):
		# from parameters
		self.footnote_type_id  		            = footnote_type_id
		self.validity_start_date                = validity_start_date
		self.validity_end_date                  = validity_end_date
		self.application_code                   = application_code
		self.description                        = description

	def xml(self):
		app = g.app

		# We only want to transfer footnote types across
		# if we are giving ED the UK national P&R, otherwise we need to convert footnotes
		# to an alphabetic format
		#if app.vat_excise == False and app.retain == True:
		if 1 > 2:
			s = app.template_footnote_type
			s = s.replace("[TRANSACTION_ID]",              str(app.transaction_id))
			s = s.replace("[MESSAGE_ID1]",                 str(app.sequence_id))
			s = s.replace("[MESSAGE_ID2]",                 str(app.sequence_id + 1))
			s = s.replace("[RECORD_SEQUENCE_NUMBER1]",     str(app.sequence_id))
			s = s.replace("[RECORD_SEQUENCE_NUMBER2]",     str(app.sequence_id + 1))
			s = s.replace("[UPDATE_TYPE]",                 "3")

			s = s.replace("[FOOTNOTE_TYPE_ID]",            f.mstr(self.footnote_type_id))
			s = s.replace("[VALIDITY_START_DATE]",         "1970-01-01")
			s = s.replace("[VALIDITY_END_DATE]",           "")
			s = s.replace("[APPLICATION_CODE]",            f.mstr(self.application_code))
			s = s.replace("[DESCRIPTION]",                 f.mstr(self.description))

			s = s.replace("\t\t\t\t\t\t<oub:validity.end.date></oub:validity.end.date>\n", "")
			s = s.replace("\t\t\t\t\t\t<oub:description></oub:description>\n", "")
			app.sequence_id += 2
			app.transaction_id += 1
			return (s)
		else:
			return ("")
