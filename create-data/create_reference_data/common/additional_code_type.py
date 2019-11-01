import psycopg2
import sys
import os
import csv
import re
import common.functions as fn
from unidecode import unidecode

class additional_code_type(object):
	def __init__(self, additional_code_type_id, description, application_code, stype):
		self.additional_code_type_id = fn.mstr(additional_code_type_id).upper()
		self.description		     = fn.mstr(description)
		self.application_code		 = fn.mstr(application_code)
		self.type		             = stype
		self.cnt = 0
		self.xml = ""
		self.same = False

	def writeXML(self, app):
		if self.type == "update":
			out = app.update_additional_code_description_XML
		else:
			out = app.insert_additional_code_description_XML
		
		self.description = fn.cleanse(self.description)
		out = out.replace("[ADDITIONAL_CODE_TYPE_ID]", self.additional_code_type_id)
		out = out.replace("[DESCRIPTION]", self.description)
		out = out.replace("[VALIDITY_START_DATE]", fn.mdate(app.critical_date_plus_one))
		out = out.replace("[LANGUAGE_ID]", "EN")
		out = out.replace("[APPLICATION_CODE]", self.application_code)
		out = out.replace("[TRANSACTION_ID]", str(app.transaction_id))
		out = out.replace("[MESSAGE_ID1]", str(app.message_id))
		out = out.replace("[MESSAGE_ID2]", str(app.message_id + 1))
		out = out.replace("[MESSAGE_ID3]", str(app.message_id + 2))
		out = out.replace("[RECORD_SEQUENCE_NUMBER1]", str(app.message_id))
		out = out.replace("[RECORD_SEQUENCE_NUMBER2]", str(app.message_id + 1))
		out = out.replace("[RECORD_SEQUENCE_NUMBER3]", str(app.message_id + 2))

		self.xml = out

		app.transaction_id += 1
		if self.type == "update":
			app.message_id += 1
		else:
			app.message_id += 2
