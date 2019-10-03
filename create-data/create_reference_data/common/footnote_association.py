import psycopg2
import sys
import os
import csv
import re
from xml.dom.minidom import Text, Element

import common.objects as o
import common.functions as fn

class footnote_association(object):
	def __init__(self, footnote_type_id, footnote_id, measure_type_id, geographical_area_id, goods_nomenclature_item_id):
		self.footnote_type_id		    = fn.mstr(footnote_type_id)
		self.footnote_id		        = fn.mstr(footnote_id)
		self.measure_type_id		    = fn.mstr(measure_type_id)
		self.geographical_area_id		= fn.mstr(geographical_area_id)
		self.goods_nomenclature_item_id	= fn.mstr(goods_nomenclature_item_id)
		self.get_measure_sid()
		
		self.cnt = 0
		self.xml = ""

	def get_measure_sid(self):
		sql = """
		select measure_sid from measures where validity_start_date = '2019-11-01'
		and measure_type_id = '""" + self.measure_type_id + """' and geographical_area_id = '""" + self.geographical_area_id + """'
		and goods_nomenclature_item_id = '""" + self.goods_nomenclature_item_id + """'
		"""
		cur = o.app.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		if len(rows) > 0:
			self.measure_sid = rows[0][0]
		else:
			self.measure_sid = -1


	def writeXML(self, app):
		if self.measure_sid != -1:
			out = app.footnote_association_measure_XML

			out = out.replace("{MEASURE_SID}", str(self.measure_sid))
			out = out.replace("{FOOTNOTE_TYPE_ID}", self.footnote_type_id)
			out = out.replace("{FOOTNOTE_ID}", str(self.footnote_id))
			out = out.replace("{MESSAGE_ID1}", str(app.message_id))
			out = out.replace("{RECORD_SEQUENCE_NUMBER1}", str(app.message_id))
			out = out.replace("{TRANSACTION_ID}", str(app.transaction_id))
			self.xml = out

			app.transaction_id 				+= 1
			app.message_id 					+= 1
		else:
			self.xml = ""
