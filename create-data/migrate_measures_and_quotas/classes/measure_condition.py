import classes.functions as f
import classes.globals as g
import datetime
import sys

class measure_condition(object):
	def __init__(self, measure_condition_sid, measure_sid, condition_code, component_sequence_number,
		condition_duty_amount, condition_monetary_unit_code, condition_measurement_unit_code,
		condition_measurement_unit_qualifier_code, action_code, certificate_type_code,
		certificate_code):
		
		# from parameters
		self.measure_condition_sid	    		        = measure_condition_sid
		self.measure_condition_sid_original				= measure_condition_sid
		self.measure_sid  			    		        = measure_sid
		self.condition_code  			                = condition_code
		self.component_sequence_number  				= component_sequence_number
		self.condition_duty_amount  			        = condition_duty_amount
		self.condition_monetary_unit_code  			    = condition_monetary_unit_code
		self.condition_measurement_unit_code            = condition_measurement_unit_code
		self.condition_measurement_unit_qualifier_code  = condition_measurement_unit_qualifier_code
		self.action_code    							= action_code
		self.certificate_type_code   					= certificate_type_code
		self.certificate_code    						= certificate_code
		self.action										= ""
		app = g.app
		if app.remove_SIVs:
			if self.condition_code == "V":
				app.measures_with_sivs_list.append (self.measure_sid)

	def xml(self):
		if self.condition_code == "V" and self.action == "restart":
			return ""
		app = g.app
		s = app.template_measure_condition
		s = s.replace("[TRANSACTION_ID]",                   str(app.transaction_id))
		s = s.replace("[MESSAGE_ID]",                       str(app.sequence_id))
		s = s.replace("[RECORD_SEQUENCE_NUMBER]",           str(app.sequence_id))

		if self.action == "delete":
			s = s.replace("[UPDATE_TYPE]",                      "2")
		else:
			if app.remove_SIVs:
				if self.condition_code == "V":
					return ("")

			s = s.replace("[UPDATE_TYPE]",                      "3")

		s = s.replace("[MEASURE_CONDITION_SID]",            f.mstr(self.measure_condition_sid))
		s = s.replace("[MEASURE_SID]",                      f.mstr(self.measure_sid))
		s = s.replace("[CONDITION_CODE]",                   f.mstr(self.condition_code))
		s = s.replace("[COMPONENT_SEQUENCE_NUMBER]",        f.mstr(self.component_sequence_number))
		s = s.replace("[DUTY_AMOUNT]",                      f.mstr(self.condition_duty_amount))
		s = s.replace("[MONETARY_UNIT_CODE]",               f.mstr(self.condition_monetary_unit_code))
		s = s.replace("[MEASUREMENT_UNIT_CODE]",            f.mstr(self.condition_measurement_unit_code))
		s = s.replace("[MEASUREMENT_UNIT_QUALIFIER_CODE]",  f.mstr(self.condition_measurement_unit_qualifier_code))
		s = s.replace("[ACTION_CODE]",                      f.mstr(self.action_code))
		s = s.replace("[CERTIFICATE_TYPE_CODE]",            f.mstr(self.certificate_type_code))
		s = s.replace("[CERTIFICATE_CODE]",                 f.mstr(self.certificate_code))

		s = s.replace("\t\t\t\t\t\t<oub:condition.duty.amount></oub:condition.duty.amount>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:condition.monetary.unit.code></oub:condition.monetary.unit.code>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:condition.measurement.unit.code></oub:condition.measurement.unit.code>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:condition.measurement.unit.qualifier.code></oub:condition.measurement.unit.qualifier.code>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:action.code></oub:action.code>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:certificate.type.code></oub:certificate.type.code>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:certificate.code></oub:certificate.code>\n", "")
		app.sequence_id += 1
		return (s)
