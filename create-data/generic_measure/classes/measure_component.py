import classes.functions as f
import classes.globals as g
import datetime
import sys

class measure_component(object):
	def __init__(self, measure_sid, duty_string, component_type, duty_expression_id):
		# from parameters
		self.measure_sid		= measure_sid
		self.duty_string		= duty_string
		self.component_type		= component_type
		self.duty_expression_id	= duty_expression_id

		self.parse()

		"""
		self.duty_amount  			    		= duty_amount
		self.monetary_unit_code  			    = monetary_unit_code
		self.measurement_unit_code  			= measurement_unit_code
		self.measurement_unit_qualifier_code    = measurement_unit_qualifier_code
		"""

	def parse(self):
		self.duty_string = self.duty_string.replace("MAX", "")
		self.duty_string = self.duty_string.replace("%", "")
		self.duty_string = self.duty_string.strip()
		
		if "EUR" in self.duty_string:
			self.monetary_unit_code = "EUR"
			pos = self.duty_string.find("EUR")
			self.duty_amount = self.duty_string[0:pos]
			remainder = self.duty_string[pos:]
			remainder = remainder.replace("EUR", "").strip()
			if remainder in ("HLT", "/ hl"):
				self.measurement_unit_code  			= "HLT"
				self.measurement_unit_qualifier_code    = ""
			elif remainder in ("/ 100 kg", "/ 100 KG"):
				self.measurement_unit_code  			= "DTN"
				self.measurement_unit_qualifier_code    = ""
			elif remainder in ("/ 100 kg / net drained wt"):
				self.measurement_unit_code  			= "DTN"
				self.measurement_unit_qualifier_code    = "E"
			elif remainder in ("ASV X"):
				self.measurement_unit_code  			= "ASV"
				self.measurement_unit_qualifier_code    = "X"
			else:
				print (self.duty_string)
				sys.exit()

		else:
			self.duty_amount  			    		= self.duty_string
			self.monetary_unit_code  			    = ""
			self.measurement_unit_code  			= ""
			self.measurement_unit_qualifier_code    = ""




	def xml(self):
		# Get duty amounts for special cases
		s = g.app.template_measure_component
		s = s.replace("[TRANSACTION_ID]",                   str(g.app.transaction_id))
		s = s.replace("[MESSAGE_ID]",                       str(g.app.message_id))
		s = s.replace("[RECORD_SEQUENCE_NUMBER]",           str(g.app.message_id))
		s = s.replace("[UPDATE_TYPE]",                      "3")
		s = s.replace("[MEASURE_SID]",                      f.mstr(self.measure_sid))
		s = s.replace("[DUTY_EXPRESSION_ID]",               f.mstr(self.duty_expression_id))
		s = s.replace("[DUTY_AMOUNT]",                      f.mstr(self.duty_amount))
		s = s.replace("[MONETARY_UNIT_CODE]",               f.mstr(self.monetary_unit_code))
		s = s.replace("[MEASUREMENT_UNIT_CODE]",            f.mstr(self.measurement_unit_code))
		s = s.replace("[MEASUREMENT_UNIT_QUALIFIER_CODE]",  f.mstr(self.measurement_unit_qualifier_code))

		s = s.replace("\t\t\t\t\t\t<oub:duty.amount></oub:duty.amount>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:monetary.unit.code></oub:monetary.unit.code>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:measurement.unit.code></oub:measurement.unit.code>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:measurement.unit.qualifier.code></oub:measurement.unit.qualifier.code>\n", "")
		g.app.message_id += 1
		return (s)
