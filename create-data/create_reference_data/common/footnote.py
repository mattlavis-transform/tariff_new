import psycopg2
import sys
import os
import csv
import re
import common.functions as fn
from unidecode import unidecode

class footnote(object):
	def __init__(self, footnote_type_id, footnote_id, description, type):
		self.footnote_type_id	= fn.mstr(footnote_type_id).upper()
		self.footnote_id		= fn.mstr(footnote_id)
		self.description		= fn.mstr(description)
		self.type				= fn.mstr(type)
		self.cnt = 0
		self.xml = ""

		self.validate()

	def validate(self):
		# Validate the footnote type ID
		if len(self.footnote_type_id) > 3:
			print ("The", self.type, "on footnote", self.footnote_type_id, self.footnote_id, " has failed validation, as the footnote type ID is longer than 3 characters in length")
			sys.exit()
		if not(self.footnote_type_id.isalpha()):
			print ("The", self.type, "on footnote", self.footnote_type_id, self.footnote_id, " has failed validation, as the footnote type ID contains non-alphabetical characters")
			sys.exit()
		# Validate the footnote ID
		if len(self.footnote_id) != 5 and len(self.footnote_id) != 3:
			print ("The", self.type, "on footnote", self.footnote_type_id, self.footnote_id, " has failed validation, as the footnote ID is neither 3 nor 5 characters in length")
			sys.exit()
		if not(self.footnote_id.isdigit()):
			print ("The", self.type, "on footnote", self.footnote_type_id, self.footnote_id, " has failed validation, as the footnote ID contains non-numeric characters")
			sys.exit()


	def writeXML(self, app):
		if self.type == "update":
			out = app.update_footnote_description_XML
		else:
			out = app.insert_footnote_XML
		
		self.description = fn.cleanse(self.description)
		out = out.replace("{FOOTNOTE_TYPE_ID}", self.footnote_type_id)
		out = out.replace("{FOOTNOTE_ID}", self.footnote_id)
		out = out.replace("{DESCRIPTION}", self.description)
		out = out.replace("{VALIDITY_START_DATE}", "2019-11-01")
		out = out.replace("{FOOTNOTE_DESCRIPTION_PERIOD_SID}", str(app.base_footnote_description_period_sid))
		out = out.replace("{TRANSACTION_ID}", str(app.transaction_id))
		out = out.replace("{MESSAGE_ID1}", str(app.message_id))
		out = out.replace("{MESSAGE_ID2}", str(app.message_id + 1))
		out = out.replace("{MESSAGE_ID3}", str(app.message_id + 2))
		out = out.replace("{RECORD_SEQUENCE_NUMBER1}", str(app.message_id))
		out = out.replace("{RECORD_SEQUENCE_NUMBER2}", str(app.message_id + 1))
		out = out.replace("{RECORD_SEQUENCE_NUMBER3}", str(app.message_id + 2))

		self.xml = out

		app.base_footnote_description_period_sid	+= 1
		app.transaction_id 							+= 1
		if self.type == "update":
			app.message_id 					+= 2
		else:
			app.message_id 					+= 3

