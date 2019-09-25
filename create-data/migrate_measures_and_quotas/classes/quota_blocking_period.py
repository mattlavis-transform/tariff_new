import classes.functions as f
import classes.globals as g
import datetime
import sys

class quota_blocking_period(object):
	def __init__(self, quota_blocking_period_sid, quota_definition_sid, blocking_start_date, blocking_end_date, blocking_period_type, description):

		# from parameters
		self.quota_blocking_period_sid  = quota_blocking_period_sid
		self.quota_definition_sid       = quota_definition_sid
		self.blocking_start_date        = blocking_start_date
		self.blocking_end_date			= blocking_end_date
		self.blocking_period_type       = blocking_period_type
		self.description                = description

	def xml(self):
		app = g.app
		s = app.template_quota_blocking_period
		s = s.replace("[TRANSACTION_ID]",         			str(app.transaction_id))
		s = s.replace("[MESSAGE_ID]",             			str(app.sequence_id))
		s = s.replace("[RECORD_SEQUENCE_NUMBER]", 			str(app.sequence_id))
		s = s.replace("[UPDATE_TYPE]", "2")

		s = s.replace("[QUOTA_BLOCKING_PERIOD_SID]",    f.mstr(self.quota_blocking_period_sid))
		s = s.replace("[QUOTA_DEFINITION_SID]",  		f.mstr(self.quota_definition_sid))
		s = s.replace("[BLOCKING_START_DATE]",          f.mstr(self.blocking_start_date))
		s = s.replace("[BLOCKING_END_DATE]",            f.mstr(self.blocking_end_date))
		s = s.replace("[BLOCKING_PERIOD_TYPE]",         f.mstr(self.blocking_period_type))
		s = s.replace("[DESCRIPTION]",					f.mstr(self.description))

		s = s.replace("\t\t\t\t\t\t<oub:description></oub:description>\n", "")

		app.sequence_id += 1
		return (s)

