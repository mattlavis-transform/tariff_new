import psycopg2
import sys
import os
import csv
import re
from xml.dom.minidom import Text, Element

import common.objects as o
import common.functions as fn

class quota_order_number_origin(object):
	def __init__(self, quota_order_number_id, geographical_area_id, validity_start_date):
		self.quota_order_number_id		= fn.mstr(quota_order_number_id)
		self.geographical_area_id		= fn.mstr(geographical_area_id)
		self.validity_start_date		= fn.mdate(validity_start_date)
		self.validity_end_date			= ""
		self.update_type				= "3"

		self.cnt = 0
		self.xml = ""

		self.get_geographical_area_sid()
		self.get_quota_order_number_sid()
		o.app.last_quota_order_number_origin_sid	+= 1
		self.quota_order_number_origin_sid = o.app.last_quota_order_number_origin_sid
		

	def get_geographical_area_sid(self):
		sql = """
		select geographical_area_sid
		from geographical_areas
		where geographical_area_id = '""" + self.geographical_area_id + """'
		order by validity_start_date desc limit 1
		"""
		cur = o.app.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		if len(rows) > 0:
			self.geographical_area_sid = rows[0][0]
		else:
			self.geographical_area_sid = -1


	def get_quota_order_number_sid(self):
		sql = """
		select quota_order_number_sid from quota_order_numbers
		where quota_order_number_id = '""" + self.quota_order_number_id + """'
		order by validity_start_date desc limit 1
		"""
		cur = o.app.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		if len(rows) > 0:
			self.quota_order_number_sid = rows[0][0]
		else:
			self.quota_order_number_sid = -1


	def writeXML(self, app):
		out = app.quota_order_number_origin_XML

		out = out.replace("{QUOTA_ORDER_NUMBER_ORIGIN_SID}",	str(self.quota_order_number_origin_sid))
		out = out.replace("{QUOTA_ORDER_NUMBER_SID}",			str(self.quota_order_number_sid))
		out = out.replace("{VALIDITY_START_DATE}",				self.validity_start_date)
		out = out.replace("{VALIDITY_END_DATE}",				self.validity_end_date)
		out = out.replace("{GEOGRAPHICAL_AREA_SID}",			str(self.geographical_area_sid))
		out = out.replace("{GEOGRAPHICAL_AREA_ID}",				self.geographical_area_id)

		out = out.replace("{UPDATE_TYPE}",						self.update_type)
		out = out.replace("{TRANSACTION_ID}",					str(app.transaction_id))
		out = out.replace("{MESSAGE_ID1}",						str(app.message_id))
		out = out.replace("{RECORD_SEQUENCE_NUMBER1}",			str(app.message_id))

		out = out.replace("\t\t\t\t<oub:validity.end.date></oub:validity.end.date>\n", "")
		self.xml = out

		app.transaction_id 				+= 1
		app.message_id 					+= 1
