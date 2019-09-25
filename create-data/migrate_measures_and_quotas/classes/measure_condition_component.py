import classes.functions as f
import classes.globals as g
import datetime
import sys

class measure_condition_component(object):
	def __init__(self, measure_sid, measure_condition_sid, duty_expression_id, duty_amount, monetary_unit_code,
		measurement_unit_code, measurement_unit_qualifier_code, condition_code):
		# from parameters
		self.measure_sid	    		        = measure_sid
		self.measure_condition_sid	    		= measure_condition_sid
		self.duty_expression_id  			    = duty_expression_id
		self.duty_amount  			    		= duty_amount
		self.monetary_unit_code  			    = monetary_unit_code
		self.measurement_unit_code  			= measurement_unit_code
		self.measurement_unit_qualifier_code    = measurement_unit_qualifier_code
		self.condition_code						= condition_code
		self.action								= ""
		self.measure_condition_sid_original		= measure_condition_sid
		self.omit = False

	def xml(self):
		if self.condition_code == "V" and self.action == "restart":
			return ""
		app = g.app
		
		if app.remove_Meursing == True:
			if self.action == "restart":
				if self.duty_expression_id in app.meursing_list:
					return ""

		if app.remove_SIVs: # This presumes that removing the safeguard part of the duty is what we will do (only)
			if self.action == "restart":
				if self.condition_code == "V":
					"""
					if self.duty_expression_id == "01":
						self.duty_amount						= "0.00"
						#self.monetary_unit_code					= ""
						#self.measurement_unit_code				= ""
						#self.measurement_unit_qualifier_code	= ""
					else:
						return ("")
					"""
					self.duty_amount						= "0.00"

		s = app.template_measure_condition_component
		s = s.replace("[TRANSACTION_ID]",                   str(app.transaction_id))
		s = s.replace("[MESSAGE_ID]",                       str(app.sequence_id))
		s = s.replace("[RECORD_SEQUENCE_NUMBER]",           str(app.sequence_id))

		if self.action == "delete":
			s = s.replace("[UPDATE_TYPE]",                      "2")
		else:
			s = s.replace("[UPDATE_TYPE]",                      "3")

		s = s.replace("[MEASURE_CONDITION_SID]",            f.mstr(self.measure_condition_sid))
		s = s.replace("[DUTY_EXPRESSION_ID]",               f.mstr(self.duty_expression_id))
		s = s.replace("[DUTY_AMOUNT]",                      f.mstr(self.duty_amount))
		s = s.replace("[MONETARY_UNIT_CODE]",               f.mstr(self.monetary_unit_code))
		s = s.replace("[MEASUREMENT_UNIT_CODE]",            f.mstr(self.measurement_unit_code))
		s = s.replace("[MEASUREMENT_UNIT_QUALIFIER_CODE]",  f.mstr(self.measurement_unit_qualifier_code))

		s = s.replace("\t\t\t\t\t\t<oub:duty.amount></oub:duty.amount>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:monetary.unit.code></oub:monetary.unit.code>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:measurement.unit.code></oub:measurement.unit.code>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:measurement.unit.qualifier.code></oub:measurement.unit.qualifier.code>\n", "")
		app.sequence_id += 1
		return (s)
