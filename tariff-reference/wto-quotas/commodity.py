import re
import sys
from datetime import datetime

class commodity(object):
	def __init__(self, section_text, ordernumber, goods_nomenclature_item_id, geographical_area_id, country):

		self.section_text						= section_text
		self.ordernumber						= ordernumber
		self.goods_nomenclature_item_id			= goods_nomenclature_item_id
		self.geographical_area_id				= geographical_area_id
		self.country							= country
		self.measure_list						= []
		self.combined_duty						= ""
		
		self.format_commodity_code()

	def format_commodity_code(self):
		s = self.goods_nomenclature_item_id
		self.commodity_code_formatted = s[0:4] + " " + s[4:6] + " " + s[6:8] + " " + s[8:10]

	def combine_duty_strings(self):
		for m in self.measure_list:
			if self.combined_duty != "":
				self.combined_duty += " + "
			self.combined_duty += m.duty_string
			self.combined_duty = self.combined_duty.replace("  ", " ")
			self.combined_duty = self.combined_duty.replace("  ", " ")