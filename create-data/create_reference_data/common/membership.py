import psycopg2
import sys
import os
import csv
import re
from xml.dom.minidom import Text, Element

import common.functions as fn

class membership(object):
	def __init__(self, PARENT_SID, PARENT_ID, CHILD_SID, CHILD_ID, START_DATE, END_DATE, UPDATE_TYPE):
		self.PARENT_SID		= fn.mstr(PARENT_SID)
		self.PARENT_ID		= fn.mstr(PARENT_ID)
		self.CHILD_SID		= fn.mstr(CHILD_SID)
		self.CHILD_ID		= fn.mstr(CHILD_ID)
		self.START_DATE		= fn.mdate(START_DATE)
		self.END_DATE		= fn.mdate(END_DATE)
		self.UPDATE_TYPE	= fn.mstr(UPDATE_TYPE)

		self.cnt = 0
		self.xml = ""


	def writeXML(self, app):
		out = app.membership_XML

		out = out.replace("{GEOGRAPHICAL_AREA_SID}", self.CHILD_SID)
		out = out.replace("{GEOGRAPHICAL_AREA_GROUP_SID}", self.PARENT_SID)
		out = out.replace("{UPDATE_TYPE}", self.UPDATE_TYPE)
		out = out.replace("{VALIDITY_START_DATE}", self.START_DATE)
		out = out.replace("{VALIDITY_END_DATE}", self.END_DATE)
		out = out.replace("{TRANSACTION_ID}", str(app.transaction_id))
		out = out.replace("{MESSAGE_ID1}", str(app.message_id))
		out = out.replace("{MESSAGE_ID2}", str(app.message_id + 1))
		out = out.replace("{MESSAGE_ID3}", str(app.message_id + 2))
		out = out.replace("{RECORD_SEQUENCE_NUMBER1}", str(app.message_id))
		out = out.replace("{RECORD_SEQUENCE_NUMBER2}", str(app.message_id + 1))
		out = out.replace("{RECORD_SEQUENCE_NUMBER3}", str(app.message_id + 2))

		out = out.replace("\t\t\t\t<oub:validity.end.date></oub:validity.end.date>\n", "")
		self.xml = out

		app.transaction_id 				+= 1
		app.message_id 					+= 1
