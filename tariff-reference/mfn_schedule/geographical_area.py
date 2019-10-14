import functions as f

class geographical_area(object):
	def __init__(self):
		self.name					= ""
		self.content				= None
		self.country_codes			= []
		self.excluded_country_codes	= []
		self.measures				= []
		self.duty_list				= []
		self.weighting				= 0
		
		
	def country_codes_to_sql(self):
		s = ""
		for item in self.country_codes:
			s += "'" + item + "', "
		s = s.strip()
		s = s.strip(",")
		return s


	def excluded_country_codes_to_sql(self):
		s = ""
		for item in self.excluded_country_codes:
			s += "'" + item + "', "
		s = s.strip()
		s = s.strip(",")
		return s