import re
import sys
from datetime import datetime

class quota_order_number(object):
	def __init__(self, ordernumber):
		self.ordernumber = ordernumber
		self.commodity_list = []
		self.section_text = ""
		self.country = ""
		
	def format_country(self):
		if self.country == "ERGA OMNES":
			return ("Erga Omnes")
		else:
			return (self.country)
