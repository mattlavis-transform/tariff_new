import psycopg2
import sys
import os
import csv
import re
import common.functions as fn
from unidecode import unidecode

class footnote_type(object):
	def __init__(self, footnote_type_id, description, application_code, validity_start_date, stype):
		self.footnote_type_id   	= fn.mstr(footnote_type_id).upper()
		self.description        	= fn.mstr(description)
		self.application_code   	= fn.mstr(application_code)
		self.validity_start_date	= fn.mdate(validity_start_date)
		self.type		        	= stype
		self.cnt = 0
		self.xml = ""
		self.same = False

	def writeXML(self, app):
		if self.type == "update":
			out = app.update_footnote_type_XML
		else:
			out = app.insert_footnote_type_XML
		
		self.description = fn.cleanse(self.description)

		out = out.replace("{FOOTNOTE_TYPE_ID}",			self.footnote_type_id)
		out = out.replace("{DESCRIPTION}",				self.description)
		out = out.replace("{VALIDITY_START_DATE}",		self.validity_start_date)
		out = out.replace("{LANGUAGE_ID}",				"EN")
		out = out.replace("{APPLICATION_CODE}",			self.application_code)
		out = out.replace("{TRANSACTION_ID}",			str(app.transaction_id))
		out = out.replace("{MESSAGE_ID1}",				str(app.message_id))
		out = out.replace("{MESSAGE_ID2}",				str(app.message_id + 1))
		out = out.replace("{MESSAGE_ID3}",				str(app.message_id + 2))
		out = out.replace("{RECORD_SEQUENCE_NUMBER1}",	str(app.message_id))
		out = out.replace("{RECORD_SEQUENCE_NUMBER2}",	str(app.message_id + 1))
		out = out.replace("{RECORD_SEQUENCE_NUMBER3}",	str(app.message_id + 2))

		self.xml = out

		app.transaction_id += 1
		if self.type == "update":
			app.message_id += 2
		else:
			app.message_id += 2
