import functions

class duty(object):
	def __init__(self, commodity_code = "", additional_code_type_id = "", additional_code_id = "", measure_type_id = "", duty_expression_id = "", duty_amount = 0, monetary_unit_code = "", measurement_unit_code = "", measurement_unit_qualifier_code = "", measure_sid = 0, quota_order_number_id = ""):
		self.commodity_code                  	= functions.mstr(commodity_code)
		self.additional_code_type_id         	= functions.mstr(additional_code_type_id)
		self.additional_code_id              	= functions.mstr(additional_code_id)
		self.measure_type_id                 	= functions.mstr(measure_type_id)
		self.measure_type_description        	= ""
		self.duty_expression_id              	= functions.mstr(duty_expression_id)
		self.duty_amount                     	= duty_amount
		self.monetary_unit_code              	= functions.mstr(monetary_unit_code)
		self.measurement_unit_code           	= functions.mstr(measurement_unit_code)
		self.measurement_unit_qualifier_code 	= functions.mstr(measurement_unit_qualifier_code)
		self.measure_sid					 	= measure_sid
		self.quota_order_number_id				= quota_order_number_id
		
		self.getDutyString()

	def getDutyString(self):
		# This bit of code removes the Meursing data from the duty expressions
		# This will be artifically re-inserted from an external data source,
		# i.e. not from data that is stored in the database (check meursing_products.csv in /source folder)
		# Confirmed 24/01/19 by Daren Timson-Hunt and David Owen that this will not be needed on the Tariff
		# but should be used in the reference documents in order to back up the legislation
		
		if self.duty_expression_id in ('12', '13', '14', '21', '23', '25', '27', '29'):
			self.duty_string = ""
			return

		self.duty_string = ""

		if self.duty_expression_id == "01":
			if self.monetary_unit_code == "":
				self.duty_string += "{0:1.1f}".format(self.duty_amount) + "%"
			else:
				self.duty_string += "{0:1.2f}".format(self.duty_amount) + " " + self.monetary_unit_code
				if self.measurement_unit_code != "":
					self.duty_string += " / " + self.getMeasurementUnit(self.measurement_unit_code)
					if self.measurement_unit_qualifier_code != "":
						self.duty_string += " / " + self.getQualifier()

		elif self.duty_expression_id in ("04", "19", "20"):
			if self.monetary_unit_code == "":
				self.duty_string += "+ {0:1.1f}".format(self.duty_amount) + "%"
			else:
				self.duty_string += "+ {0:1.2f}".format(self.duty_amount) + " " + self.monetary_unit_code
				if self.measurement_unit_code != "":
					self.duty_string += " / " + self.getMeasurementUnit(self.measurement_unit_code)
					if self.measurement_unit_qualifier_code != "":
						self.duty_string += " / " + self.getQualifier()

		elif self.duty_expression_id == "12":
			self.duty_string += " + AC"

		elif self.duty_expression_id == "15":
			if self.monetary_unit_code == "":
				self.duty_string += "MIN {0:1.1f}".format(self.duty_amount) + "%"
			else:
				self.duty_string += "MIN {0:1.2f}".format(self.duty_amount) + " " + self.monetary_unit_code
				if self.measurement_unit_code != "":
					self.duty_string += " / " + self.getMeasurementUnit(self.measurement_unit_code)
					if self.measurement_unit_qualifier_code != "":
						self.duty_string += " / " + self.getQualifier()

		elif self.duty_expression_id == "17": #MAX
			if self.monetary_unit_code == "":
				self.duty_string += "MAX {0:1.1f}".format(self.duty_amount) + "%"
			else:
				self.duty_string += "MAX {0:1.2f}".format(self.duty_amount) + " " + self.monetary_unit_code
				if self.measurement_unit_code != "":
					self.duty_string += " / " + self.getMeasurementUnit(self.measurement_unit_code)
					if self.measurement_unit_qualifier_code != "":
						self.duty_string += " / " + self.getQualifier()

		elif self.duty_expression_id == "21":
			self.duty_string += " + SD"

		elif self.duty_expression_id == "27":
			self.duty_string += " + FD"

		#print (self.commodity_code, self.duty_string)

	def getMeasurementUnit(self, s):
		if s == "ASV":
			return "% vol" # 3302101000
		if s == "NAR":
			return "item"
		elif s == "CCT":
			return "ct/l"
		elif s == "CEN":
			return "100 p/st"
		elif s == "CTM":
			return "c/k"
		elif s == "DTN":
			return "100 kg"
		elif s == "GFI":
			return "gi F/S"
		elif s == "GRM":
			return "g"
		elif s == "HLT":
			return "hl" # 2209009100
		elif s == "HMT":
			return "100 m" # 3706909900
		elif s == "KGM":
			return "kg"
		elif s == "KLT":
			return "1,000 l"
		elif s == "KMA":
			return "kg met.am."
		elif s == "KNI":
			return "kg N"
		elif s == "KNS":
			return "kg H2O2"
		elif s == "KPH":
			return "kg KOH"
		elif s == "KPO":
			return "kg K2O"
		elif s == "KPP":
			return "kg P2O5"
		elif s == "KSD":
			return "kg 90 % sdt"
		elif s == "KSH":
			return "kg NaOH"
		elif s == "KUR":
			return "kg U"
		elif s == "LPA":
			return "l alc. 100%"
		elif s == "LTR":
			return "l"
		elif s == "MIL":
			return "1,000 items"
		elif s == "MTK":
			return "m2"
		elif s == "MTQ":
			return "m3"
		elif s == "MTR":
			return "m"
		elif s == "MWH":
			return "1,000 kWh"
		elif s == "NCL":
			return "ce/el"
		elif s == "NPR":
			return "pa"
		elif s == "TJO":
			return "TJ"
		elif s == "TNE":
			return "tonne" # 1005900020
			# return "1000 kg" # 1005900020
		else:
			return s

	def getQualifier(self):
		sQualDesc = ""
		s = self.measurement_unit_qualifier_code
		if s == "A":
			sQualDesc = "tot alc" # Total alcohol
		elif s == "C":
			sQualDesc = "1 000" # Total alcohol
		elif s == "E":
			sQualDesc = "net drained wt" # net of drained weight
		elif s == "G":
			sQualDesc = "gross" # Gross
		elif s == "M":
			sQualDesc = "net dry" # net of dry matter
		elif s == "P":
			sQualDesc = "lactic matter" # of lactic matter
		elif s == "R":
			sQualDesc = "std qual" # of the standard quality
		elif s == "S":
			sQualDesc = " raw sugar"
		elif s == "T":
			sQualDesc = "dry lactic matter" # of dry lactic matter
		elif s == "X":
			sQualDesc = " hl" # Hectolitre
		elif s == "Z":
			sQualDesc = "% sacchar." # per 1% by weight of sucrose
		return sQualDesc
