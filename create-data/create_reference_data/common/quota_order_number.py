import psycopg2
import sys
import os
import csv
import re
from datetime import datetime
from xml.dom.minidom import Text, Element

import common.objects as o
import common.functions as fn

class quota_order_number(object):
	def __init__(self):
		self.quota_order_number_sid		= -1
		self.quota_order_number_id		= ""
		self.validity_start_date		= ""
		self.validity_end_date			= ""
		self.update_type				= ""


	def get_data_from_id(self):
		sql = """
		select quota_order_number_sid, validity_start_date, validity_end_date
		from quota_order_numbers
		where quota_order_number_id = '""" + self.quota_order_number_id + """'
		order by validity_start_date desc limit 1
		"""
		cur = o.app.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		if len(rows) > 0:
			self.quota_order_number_sid	= rows[0][0]
			self.validity_start_date	= rows[0][1]
			self.validity_end_date		= rows[0][2]
		else:
			sys.exit()


	def get_next_sid(self):
		self.quota_order_number_sid = o.app.last_quota_order_number_sid
		o.app.last_quota_order_number_sid += 1


	def terminate(self, validity_end_date):
		self.validity_end_date	= validity_end_date
		self.update_type		= "1"


	def start(self, validity_start_date):
		self.validity_start_date	= validity_start_date
		self.update_type			= "3"


	def date_to_string(self, v):
		if v is None:
			return ""
		elif (isinstance(v, str)):
			return v
		else:
			return datetime.strftime(v, '%Y-%m-%d')


	def xml(self):
		out = o.app.quota_order_number_XML

		out = out.replace("{QUOTA_ORDER_NUMBER_ID}",			str(self.quota_order_number_id))
		out = out.replace("{QUOTA_ORDER_NUMBER_SID}",			str(self.quota_order_number_sid))
		out = out.replace("{VALIDITY_START_DATE}",				self.date_to_string(self.validity_start_date))
		out = out.replace("{VALIDITY_END_DATE}",				self.date_to_string(self.validity_end_date))

		out = out.replace("{UPDATE_TYPE}",						self.update_type)
		out = out.replace("{TRANSACTION_ID}",					str(o.app.transaction_id))
		out = out.replace("{MESSAGE_ID1}",						str(o.app.message_id))
		out = out.replace("{RECORD_SEQUENCE_NUMBER1}",			str(o.app.message_id))

		out = out.replace("\t\t\t\t\t<oub:validity.end.date></oub:validity.end.date>\n", "")
		self.xml = out

		o.app.transaction_id 				+= 1
		o.app.message_id 					+= 1
		return out