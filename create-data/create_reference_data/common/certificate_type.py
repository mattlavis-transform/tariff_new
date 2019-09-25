import psycopg2
import sys
import os
import csv
import re
import common.functions as fn
from unidecode import unidecode

class certificate_type(object):
	def __init__(self, certificate_type_code, description, validity_start_date, stype):
		self.certificate_type_code  = fn.mstr(certificate_type_code).upper()
		self.description		    = fn.mstr(description)
		self.validity_start_date    = fn.mdate(validity_start_date)
		self.type		            = stype
		self.cnt = 0
		self.xml = ""
		self.same = False

	def writeXML(self, app):
		if self.type == "update":
			out = app.update_certificate_type_description_XML
		else:
			out = app.insert_certificate_type_XML
		
		self.description = fn.cleanse(self.description)
		out = out.replace("{CERTIFICATE_TYPE_CODE}",	self.certificate_type_code)
		out = out.replace("{DESCRIPTION}",				self.description)
		out = out.replace("{VALIDITY_START_DATE}",		self.validity_start_date)
		out = out.replace("{LANGUAGE_ID}",				"EN")
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
			app.message_id += 1
		else:
			app.message_id += 2
