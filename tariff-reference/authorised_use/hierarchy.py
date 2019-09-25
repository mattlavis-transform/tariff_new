import urllib.request, json, sys
import functions as f
import glob as g

class hierarchy(object):
	def __init__(self, goods_nomenclature_item_id, producline_suffix, number_indents, description):
		self.goods_nomenclature_item_id = goods_nomenclature_item_id
		self.producline_suffix		    = producline_suffix
		self.number_indents		        = number_indents
		self.description		        = description


class subheading_hierarchy(object):
	def __init__(self, subheading, hierarchy_list):
		self.subheading = subheading
		self.hierarchy_list  = hierarchy_list
