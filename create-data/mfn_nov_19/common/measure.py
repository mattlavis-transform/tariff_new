import psycopg2
import sys
import os
import csv
import re
import common.functions as fn
from unidecode import unidecode
import common.objects as o


class measure(object):
	def __init__(self, goods_nomenclature_item_id, advalorem, specific, minimum):
		self.goods_nomenclature_item_id	= fn.mstr(goods_nomenclature_item_id)
		self.advalorem   				= fn.mstr(advalorem)
		self.specific   				= fn.mstr(specific)
		self.minimum   					= fn.mstr(minimum)
		
		self.description   				= ""
		self.number_indents   			= -1
		self.chapter		   			= -1

	def get_indents_and_description(self):
		for obj in o.app.goods_nomenclature_list:
			if self.goods_nomenclature_item_id == obj.goods_nomenclature_item_id:
				if obj.productline_suffix == "80":
					self.number_indents = int(obj.number_indents)
					self.description	= obj.description
					self.chapter		= int(self.goods_nomenclature_item_id[:2])
					obj.status			= "Applied"
					if obj.measure_type_id == "":
						obj.measure_type_id = "103"
					obj.advalorem		= self.advalorem
					obj.specific		= self.specific
					obj.minimum		= self.minimum
					break


	def writeXML(self, app):
		if self.type == "update":
			out = app.update_measure_type_description_XML
		else:
			out = app.insert_measure_type_XML
		
		self.description = fn.cleanse(self.description)
		out = out.replace("{MEASURE_TYPE_ID}",						self.measure_type_id)
		out = out.replace("{DESCRIPTION}",							self.description)
		out = out.replace("{VALIDITY_START_DATE}",					self.validity_start_date)
		out = out.replace("{TRADE_MOVEMENT_CODE}",					self.trade_movement_code)
		out = out.replace("{PRIORITY_CODE}",						self.priority_code)
		out = out.replace("{MEASURE_COMPONENT_APPLICABLE_CODE}",	self.measure_component_applicable_code)
		out = out.replace("{ORIGIN_DEST_CODE}",						self.origin_dest_code)
		out = out.replace("{ORDER_NUMBER_CAPTURE_CODE}",			self.order_number_capture_code)
		out = out.replace("{MEASURE_EXPLOSION_LEVEL}",				self.measure_explosion_level)
		out = out.replace("{MEASURE_TYPE_SERIES_ID}",				self.measure_type_series_id)
		out = out.replace("{TRANSACTION_ID}", 						str(app.transaction_id))
		out = out.replace("{MESSAGE_ID1}", 							str(app.message_id))
		out = out.replace("{MESSAGE_ID2}", 							str(app.message_id + 1))
		out = out.replace("{MESSAGE_ID3}",							str(app.message_id + 2))
		out = out.replace("{RECORD_SEQUENCE_NUMBER1}",				str(app.message_id))
		out = out.replace("{RECORD_SEQUENCE_NUMBER2}",				str(app.message_id + 1))
		out = out.replace("{RECORD_SEQUENCE_NUMBER3}",				str(app.message_id + 2))

		self.xml = out

		app.transaction_id += 1
		app.message_id += 1