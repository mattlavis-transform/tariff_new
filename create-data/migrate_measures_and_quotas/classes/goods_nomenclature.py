import classes.functions as f
import classes.globals as g
import datetime
import sys

class goods_nomenclature(object):
	def __init__(self, goods_nomenclature_item_id):
		self.goods_nomenclature_item_id = goods_nomenclature_item_id
		self.measure_list = []
	