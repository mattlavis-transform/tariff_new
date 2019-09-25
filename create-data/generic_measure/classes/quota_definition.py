import classes.functions as f
import classes.globals as g
import datetime
import sys

class quota_definition(object):
	def __init__(self, quota_order_number_id, validity_start_date, validity_end_date, initial_volume,
		measurement_unit_code, maximum_precision, critical_state, critical_threshold, monetary_unit_code,
		measurement_unit_qualifier_code, blocking_period_start, blocking_period_end):

		# from parameters
		self.quota_order_number_id  			= quota_order_number_id
		self.validity_start_date    			= validity_start_date
		self.validity_end_date      			= validity_end_date
		self.volume      						= initial_volume
		self.initial_volume      				= initial_volume
		self.measurement_unit_code  			= measurement_unit_code
		self.maximum_precision      			= maximum_precision
		self.critical_state      				= critical_state
		self.critical_threshold      			= critical_threshold
		self.monetary_unit_code     			= monetary_unit_code
		self.measurement_unit_qualifier_code	= measurement_unit_qualifier_code
		self.blocking_period_start      		= blocking_period_start
		self.blocking_period_end		      	= blocking_period_end

		self.description = ""
		self.quota_order_number_sid = 0

		# Initialised
		self.quota_blocking_period_list	= []

	def xml(self):
		s = g.app.template_quota_definition

		self.quota_definition_sid = g.app.last_quota_definition_sid
		self.description += " from " + self.validity_start_date + " to " + self.validity_end_date

		s = s.replace("[TRANSACTION_ID]",         			str(g.app.transaction_id))
		s = s.replace("[MESSAGE_ID]",             			str(g.app.message_id))
		s = s.replace("[RECORD_SEQUENCE_NUMBER]", 			str(g.app.message_id))
		s = s.replace("[UPDATE_TYPE]",						"3")

		s = s.replace("[QUOTA_DEFINITION_SID]",             f.mstr(self.quota_definition_sid))
		s = s.replace("[QUOTA_ORDER_NUMBER_ID]",            f.mstr(self.quota_order_number_id))
		s = s.replace("[VALIDITY_START_DATE]",              self.validity_start_date)
		s = s.replace("[VALIDITY_END_DATE]",                self.validity_end_date)
		s = s.replace("[QUOTA_ORDER_NUMBER_SID]",           f.mstr(self.quota_order_number_sid))
		s = s.replace("[VOLUME]",                           f.mstr(self.initial_volume))
		s = s.replace("[INITIAL_VOLUME]",                   f.mstr(self.initial_volume))
		s = s.replace("[MONETARY_UNIT_CODE]",               f.mstr(self.monetary_unit_code))
		s = s.replace("[MEASUREMENT_UNIT_CODE]",            f.mstr(self.measurement_unit_code))
		s = s.replace("[MEASUREMENT_UNIT_QUALIFIER_CODE]",  f.mstr(self.measurement_unit_qualifier_code))
		s = s.replace("[MAXIMUM_PRECISION]",                f.mstr(self.maximum_precision))
		s = s.replace("[CRITICAL_STATUS]",                  f.mstr(self.critical_state))
		s = s.replace("[CRITICAL_THRESHOLD]",               f.mstr(self.critical_threshold))
		s = s.replace("[DESCRIPTION]",                      f.mstr(self.description))

		s = s.replace("\t\t\t\t\t\t<oub:validity.end.date></oub:validity.end.date>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:monetary.unit.code></oub:monetary.unit.code>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:measurement.unit.code></oub:measurement.unit.code>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:measurement.unit.qualifier.code></oub:measurement.unit.qualifier.code>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:description></oub:description>\n", "")

		g.app.last_quota_definition_sid +=1
		g.app.message_id += 1
		g.app.transaction_id += 1

		return (s)
