import functions as f

#app = f.app

class measure(object):
	def __init__(self):
		self.measure_sid                = 0
		self.goods_nomenclature_item_id = ""
		self.measure_type_id            = ""
		self.validity_start_date        = ""
		self.validity_end_date          = ""
		self.ordernumber		        = ""
		self.is_zero					= True
		self.duty_list = []
		self.category = ""
	

	def combine_duties(self):
		self.combined_duty      = ""

		self.measure_list         = []
		self.measure_type_list    = []
		self.additional_code_list = []

		for d in self.duty_list:
			self.measure_type_list.append(d.measure_type_id)
			self.measure_list.append(d.measure_sid)
			self.additional_code_list.append(d.additional_code_id)

		measure_type_list_unique    = set(self.measure_type_list)
		measure_list_unique         = set(self.measure_list)
		additional_code_list_unique = set(self.additional_code_list)

		self.measure_count      = len(measure_list_unique)
		self.measure_type_count = len(measure_type_list_unique)
		self.additional_code_count = len(additional_code_list_unique)
		
		if self.measure_count == 1 and self.measure_type_count == 1 and self.additional_code_count == 1:
			for d in self.duty_list:
				self.combined_duty += d.duty_string + " "
		else:
			if self.measure_type_count > 1:
				#self.combined_duty = "More than one measure type"
				if "105" in measure_type_list_unique:
					for d in self.duty_list:
						if d.measure_type_id == "105":
							self.combined_duty += d.duty_string + " "
			elif self.additional_code_count > 1:
				#self.combined_duty = "More than one additional code"
				if "500" in additional_code_list_unique:
					for d in self.duty_list:
						if d.additional_code_id == "500":
							self.combined_duty += d.duty_string + " "
				if "550" in additional_code_list_unique:
					for d in self.duty_list:
						if d.additional_code_id == "550":
							self.combined_duty += d.duty_string + " "
	
		self.combined_duty = self.combined_duty.replace("  ", " ")
		self.combined_duty = self.combined_duty.strip()

	def get_category(self):
		self.category = "Unspecified"
		for c in f.app.categories:
			l = len(c.code)
			if self.goods_nomenclature_item_id[0:l] == c.code:
				#print ("found", c.description)
				self.category = c.description
				break
			
	def goods_nomenclature_formatted(self):
		s = self.goods_nomenclature_item_id
		s = s[0:4] + " " + s[4:6] + " " + s[6:8] + " " + s[8:10]
		return s