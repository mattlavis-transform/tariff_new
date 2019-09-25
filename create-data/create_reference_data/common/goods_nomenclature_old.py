import psycopg2
import sys
import os
import csv
import re
import common.functions as fn
import common.objects as o
from unidecode import unidecode

class goods_nomenclature(object):
	def __init__(self, goods_nomenclature_item_id, productline_suffix, description, get_goods_nomenclature_sid, goods_nomenclature_description_period_sid, datestring, stype, app):
		self.goods_nomenclature_item_id = fn.mstr(goods_nomenclature_item_id)
		self.productline_suffix			= fn.mstr(productline_suffix)
		self.description		     	= fn.mstr(description)
		self.datestring		 			= fn.mstr(datestring)
		self.type		             	= stype
		self.cnt = 0
		self.xml = ""

		self.get_goods_nomenclature_sid(app)
		self.increment_description_period_sid(app)
		
		self.check_still_exists()

	def check_still_exists(self):
		sql = """SELECT validity_end_date FROM goods_nomenclatures WHERE producline_suffix = '""" + self.productline_suffix + """' AND
		goods_nomenclature_item_id = '""" + self.goods_nomenclature_item_id + """' ORDER BY validity_end_date DESC;"""
		#print (sql)
		cur = o.app.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		#print ("Here")
		for row in rows:
			self.validity_end_date = row[0]
			if self.validity_end_date != None:
				#print (row[0])
				self.still_exists = False
			else:
				self.still_exists = True
		
		#sys.exit()

	def get_goods_nomenclature_sid(self, app):
		self.goods_nomenclature_sid = -1
		cur = app.conn.cursor()
		cur.execute("SELECT goods_nomenclature_sid FROM goods_nomenclatures WHERE producline_suffix = %s AND goods_nomenclature_item_id = %s ORDER BY validity_start_date DESC LIMIT 1", (self.productline_suffix, self.goods_nomenclature_item_id))
		rows = cur.fetchall()
		self.goods_nomenclature_sid = rows[0][0]

	def increment_description_period_sid(self, app):
		#app.last_goods_nomenclature_sid += 1
		app.last_goods_nomenclature_description_period_sid += 1
		#self.goods_nomenclature_sid = app.last_goods_nomenclature_sid
		self.goods_nomenclature_description_period_sid = app.last_goods_nomenclature_description_period_sid
		

	def writeXML(self, app):
		if self.type == "update":
			out = app.update_goods_nomenclature_description_XML
		else:
			out = app.insert_goods_nomenclature_description_XML
		
		self.description = fn.cleanse(self.description)
		out = out.replace("{GOODS_NOMENCLATURE_ITEM_ID}", self.goods_nomenclature_item_id)
		out = out.replace("{PRODUCTLINE_SUFFIX}", self.productline_suffix)
		out = out.replace("{DESCRIPTION}", self.description)
		out = out.replace("{GOODS_NOMENCLATURE_DESCRIPTION_PERIOD_SID}", str(self.goods_nomenclature_description_period_sid))
		out = out.replace("{GOODS_NOMENCLATURE_SID}", str(self.goods_nomenclature_sid))
		out = out.replace("{VALIDITY_START_DATE}", "2019-03-30")
		out = out.replace("{LANGUAGE_ID}", "EN")
		out = out.replace("{TRANSACTION_ID}", str(app.transaction_id))
		out = out.replace("{MESSAGE_ID1}", str(app.message_id))
		out = out.replace("{MESSAGE_ID2}", str(app.message_id + 1))
		out = out.replace("{MESSAGE_ID3}", str(app.message_id + 2))
		out = out.replace("{RECORD_SEQUENCE_NUMBER1}", str(app.message_id))
		out = out.replace("{RECORD_SEQUENCE_NUMBER2}", str(app.message_id + 1))
		out = out.replace("{RECORD_SEQUENCE_NUMBER3}", str(app.message_id + 2))

		self.xml = out

		app.transaction_id += 1
		if self.type == "update":
			app.message_id += 1
		else:
			app.message_id += 2
