import classes.functions as f
import classes.globals as g
import datetime
import sys

class quota_suspension_period(object):
	def __init__(self, quota_suspension_period_sid, quota_definition_sid, suspension_start_date, suspension_end_date, description):

		# from parameters
		self.quota_suspension_period_sid    = quota_suspension_period_sid
		self.quota_definition_sid           = quota_definition_sid
		self.suspension_start_date			= suspension_start_date
		self.suspension_end_date			= suspension_end_date
		self.description					= description

	def xml(self):
		app = g.app
		s = app.template_quota_suspension_period
		s = s.replace("[TRANSACTION_ID]",         			str(app.transaction_id))
		s = s.replace("[MESSAGE_ID]",             			str(app.sequence_id))
		s = s.replace("[RECORD_SEQUENCE_NUMBER]", 			str(app.sequence_id))
		s = s.replace("[UPDATE_TYPE]", "2")

		s = s.replace("[QUOTA_SUSPENSION_PERIOD_SID]",	f.mstr(self.quota_suspension_period_sid))
		s = s.replace("[QUOTA_DEFINITION_SID]",  		f.mstr(self.quota_definition_sid))
		s = s.replace("[SUSPENSION_START_DATE]",        f.mstr(self.suspension_start_date))
		s = s.replace("[SUSPENSION_END_DATE]",          f.mstr(self.suspension_end_date))
		s = s.replace("[DESCRIPTION]",					f.mstr(self.description))

		s = s.replace("\t\t\t\t\t\t<oub:description></oub:description>\n", "")

		app.sequence_id += 1
		return (s)

