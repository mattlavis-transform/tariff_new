import classes.functions as f
import classes.globals as g
import datetime
import sys

from classes.eea_measure import eea_measure
from classes.eea_measure_component import eea_measure_component

class goods_nomenclature(object):
	def __init__(self, goods_nomenclature_item_id, productline_suffix, validity_start_date, validity_end_date):
		# from parameters
		self.goods_nomenclature_item_id	= goods_nomenclature_item_id
		self.productline_suffix			= productline_suffix
		self.validity_start_date		= validity_start_date
		self.validity_end_date			= validity_end_date

		self.parent_goods_nomenclature_item_id = ""
		self.parent_productline_suffix = ""

		self.eea_measure = None
		self.eea_iceland_measure = None
		self.iceland_measure = None
		self.norway_measure = None
		self.prevailing_measure = None
		

		self.active_measure_count = 0
		self.conflict_list = []


	def get_conflicts(self, my_index, my_commodity, direction):
		if self.goods_nomenclature_item_id == "1806201000":
			a = 1

		# Search UP the tree for parentage
		if direction != "down":
			for loop2 in range(my_index - 1, -1, -1):
				prior_commodity = g.app.commodity_list[loop2]
				if prior_commodity.goods_nomenclature_item_id == my_commodity.parent_goods_nomenclature_item_id \
					and prior_commodity.productline_suffix == my_commodity.parent_productline_suffix:
					if prior_commodity.prevailing_measure != None:
						self.active_measure_count += 1
						self.conflict_list.append (prior_commodity)
					self.get_conflicts(loop2, prior_commodity, "up")
					if prior_commodity.significant_digits == 4:
						break


		# Now search DOWN the tree for children
		if direction != "up":
			for loop2 in range(my_index + 1, len(g.app.commodity_list)):
				next_commodity = g.app.commodity_list[loop2]
				if next_commodity.parent_goods_nomenclature_item_id == my_commodity.goods_nomenclature_item_id \
					and next_commodity.parent_productline_suffix == my_commodity.productline_suffix:

					if next_commodity.prevailing_measure != None:
						self.active_measure_count += 1
						self.conflict_list.append (next_commodity)
					self.get_conflicts(loop2, next_commodity, "down")
					
					if next_commodity.significant_digits == 4 or loop2 == len(g.app.commodity_list):
						break

		a = 1


	def resolve_conflicts(self):
		#return
		#print ("Resolving conflicts for", self.goods_nomenclature_item_id)
		#print ("Including, myself, the active measure count of", self.goods_nomenclature_item_id, "is", str(self.active_measure_count))
		my_conflict_count = len(self.conflict_list)

		for loop1 in range(my_conflict_count - 1, -1, -1):
			conflicting_item = self.conflict_list[loop1]

			if conflicting_item.prevailing_measure.value >= self.prevailing_measure.value:
				if conflicting_item.number_indents > self.number_indents:
					conflicting_item.prevailing_measure = None
					conflicting_item.active_measure_count -= 1
					count = len(conflicting_item.conflict_list)
					for i in range(0, count):
						item2 = conflicting_item.conflict_list[i]
						if item2.goods_nomenclature_item_id == self.goods_nomenclature_item_id:
							conflicting_item.conflict_list.pop(i)
							break

					self.conflict_list.pop(loop1)
					self.active_measure_count -= 1


		pass


	def get_lower_iceland(self):
		zero_count = 0
		if self.eea_measure == None:
			self.eea_value = 999999
			zero_count += 1
		else:
			self.eea_value = self.eea_measure.value

		if self.eea_iceland_measure == None:
			self.eea_iceland_value = 999999
			zero_count += 1
		else:
			self.eea_iceland_value = self.eea_iceland_measure.value

		if self.iceland_measure == None:
			self.iceland_value = 999999
			zero_count += 1
		else:
			self.iceland_value = self.iceland_measure.value


		if zero_count == 3:
			self.prevailing_measure = None
		else:
			self.active_measure_count = 1
			if self.eea_value < self.eea_iceland_value:
				if self.eea_value < self.iceland_value:
					self.prevailing_measure = self.eea_measure
				else:
					self.prevailing_measure = self.iceland_measure
			else:
				if self.eea_iceland_value < self.iceland_value:
					self.prevailing_measure = self.eea_iceland_measure
				else:
					self.prevailing_measure = self.iceland_measure

		if self.goods_nomenclature_item_id == "0304750000":
			a = 1
			self.prevailing_measure = None


	def get_lower_norway(self):
		zero_count = 0
		if self.eea_measure == None:
			self.eea_value = 999999
			zero_count += 1
		else:
			self.eea_value = self.eea_measure.value

		if self.norway_measure == None:
			self.norway_value = 999999
			zero_count += 1
		else:
			self.norway_value = self.norway_measure.value

		if zero_count == 2:
			self.prevailing_measure = None
		else:
			self.active_measure_count = 1
			if self.eea_value < self.norway_value:
				self.prevailing_measure = self.eea_measure
			else:
				self.prevailing_measure = self.norway_measure

		
		if self.goods_nomenclature_item_id == "1605100000":
			self.prevailing_measure = None
		elif self.goods_nomenclature_item_id in ("1605100091", "1605100095", "1605100097") and self.productline_suffix == "80":
			m = eea_measure("NO", -1, self.goods_nomenclature_item_id, f.mdate(g.app.critical_date_plus_one), None)
			mc = eea_measure_component(-1, "01", 2.4, None, None, None)
			m.measure_component_list.append (mc)
			m.combine_duties()
			self.prevailing_measure = m

