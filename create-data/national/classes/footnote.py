import classes.functions as f
import classes.globals as g
import datetime
import sys

class footnote(object):
	def __init__(self, footnote_type_id, footnote_id, validity_start_date, validity_end_date, description):
		# from parameters
		self.footnote_type_id  		            = footnote_type_id
		self.footnote_id                        = footnote_id
		self.validity_start_date                = validity_start_date
		self.validity_end_date                  = validity_end_date
		self.description                        = description

	def xml(self):
		app = g.app

		s = app.template_footnote
		s = s.replace("[TRANSACTION_ID]",              str(app.transaction_id))
		s = s.replace("[MESSAGE_ID1]",                 str(app.sequence_id))
		s = s.replace("[MESSAGE_ID2]",                 str(app.sequence_id + 1))
		s = s.replace("[MESSAGE_ID3]",                 str(app.sequence_id + 2))
		s = s.replace("[RECORD_SEQUENCE_NUMBER1]",     str(app.sequence_id))
		s = s.replace("[RECORD_SEQUENCE_NUMBER2]",     str(app.sequence_id + 1))
		s = s.replace("[RECORD_SEQUENCE_NUMBER3]",     str(app.sequence_id + 2))
		s = s.replace("[UPDATE_TYPE]",                 "3")


		app.last_footnote_description_period_sid += 1
		self.footnote_description_period_sid = app.last_footnote_description_period_sid
		if self.footnote_type_id == '04':
			self.footnote_type_id = 'FM'
			self.footnote_id = '99' + self.footnote_id
			self.validity_start_date = g.app.critical_date_plus_one.strftime('%Y-%m-%d')

		s = s.replace("[FOOTNOTE_TYPE_ID]",                 f.mstr(self.footnote_type_id))
		s = s.replace("[FOOTNOTE_ID]",                      f.mstr(self.footnote_id))
		s = s.replace("[VALIDITY_START_DATE]",              f.mdate(self.validity_start_date))
		s = s.replace("[VALIDITY_END_DATE]",                f.mdate(self.validity_end_date))
		s = s.replace("[FOOTNOTE_DESCRIPTION_PERIOD_SID]",  f.mstr(self.footnote_description_period_sid))
		s = s.replace("[DESCRIPTION]",                      f.mcleanse(self.description))

		s = s.replace("\t\t\t\t\t\t<oub:validity.end.date></oub:validity.end.date>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:description></oub:description>\n", "")
		app.sequence_id += 3
		app.transaction_id += 1
		return (s)
