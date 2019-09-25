import psycopg2
import sys
import os
import csv
import re
import common.functions as fn
from unidecode import unidecode

class measure_type(object):
	def __init__(self, measure_type_id, description, validity_start_date, trade_movement_code, priority_code, measure_component_applicable_code, origin_dest_code, order_number_capture_code, measure_explosion_level, measure_type_series_id, stype):
		self.measure_type_id    				= fn.mstr(measure_type_id)
		self.description        				= fn.mstr(description)
		self.validity_start_date   				= fn.mdate(validity_start_date)
		self.end_date			   				= fn.mdate(validity_start_date)
		self.trade_movement_code   				= fn.mstr(trade_movement_code)
		self.priority_code   					= fn.mstr(priority_code)
		self.measure_component_applicable_code	= fn.mstr(measure_component_applicable_code)
		self.origin_dest_code   				= fn.mstr(origin_dest_code)
		self.order_number_capture_code   		= fn.mstr(order_number_capture_code)
		self.measure_explosion_level   			= fn.mstr(measure_explosion_level)
		self.measure_type_series_id   			= fn.mstr(measure_type_series_id)
		self.type		        				= stype
		self.cnt = 0
		self.xml = ""
		self.same = False

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
		if self.type == "update":
			app.message_id += 1
		else:
			app.message_id += 2
