import classes.functions as f
import classes.globals as g
import datetime
import sys

class goods_nomenclature(object):
	def __init__(self, goods_nomenclature_item_id, productline_suffix, validity_start_date, validity_end_date):
		# from parameters
		self.goods_nomenclature_item_id	= goods_nomenclature_item_id
		self.productline_suffix			= productline_suffix
		self.validity_start_date		= validity_start_date
		self.validity_end_date			= validity_end_date

