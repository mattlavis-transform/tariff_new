import psycopg2
import sys
import os
import csv
import re
import common.functions as fn
from unidecode import unidecode

class base_regulation(object):
	def __init__(self, base_regulation_id, validity_start_date, regulation_group_id, legislation_id, url, information_text, typestring):
		self.base_regulation_id	= fn.mstr(base_regulation_id).upper()
		self.validity_start_date		= fn.mdate(validity_start_date)
		self.regulation_group_id	    = fn.mstr(regulation_group_id)
		self.legislation_id			    = fn.mstr(legislation_id)
		self.url					    = fn.mstr(url)
		self.information_text		    = fn.mstr(information_text)
		self.information_text			= fn.cleanse(self.information_text)
		self.type		    			= typestring

		self.cnt = 0
		self.xml = ""
		self.concatenate()

	def concatenate(self):
		self.information_text_concatenated = ""
		self.information_text_concatenated += self.legislation_id + "|"
		self.information_text_concatenated += self.url + "|"
		self.information_text_concatenated += self.information_text

	def writeXML(self, app):
		if self.base_regulation_id != "":
			if self.type == "update":
				out = app.update_base_regulation_XML
			else:
				out = app.insert_base_regulation_XML

			out = out.replace("{BASE_REGULATION_ROLE}",     "1")
			out = out.replace("{BASE_REGULATION_ID}",       self.base_regulation_id)
			out = out.replace("{PUBLISHED_DATE}",           self.validity_start_date)
			out = out.replace("{OFFICIALJOURNAL_NUMBER}",   "1")
			out = out.replace("{OFFICIALJOURNAL_PAGE}",     "1")
			out = out.replace("{VALIDITY_START_DATE}",      self.validity_start_date)
			out = out.replace("{VALIDITY_END_DATE}",        "")
			out = out.replace("{EFFECTIVE_END_DATE}",       "")
			out = out.replace("{COMMUNITY_CODE}",           "1")
			out = out.replace("{REGULATION_GROUP_ID}",      self.regulation_group_id)
			out = out.replace("{REPLACEMENT_INDICATOR}",    "0")
			out = out.replace("{STOPPED_FLAG}",             "0")
			out = out.replace("{INFORMATION_TEXT}",         self.information_text_concatenated)
			out = out.replace("{APPROVED_FLAG}",            "1")
			out = out.replace("{TRANSACTION_ID}",			str(app.transaction_id))
			out = out.replace("{MESSAGE_ID}",				str(app.message_id))
			out = out.replace("{RECORD_SEQUENCE_NUMBER}",	str(app.message_id))

			out = out.replace("\t\t\t\t\t\t<oub:published.date></oub:published.date>\n", "")
			out = out.replace("\t\t\t\t\t\t<oub:officialjournal.number></oub:officialjournal.number>\n", "")
			out = out.replace("\t\t\t\t\t\t<oub:officialjournal.page></oub:officialjournal.page>\n", "")
			out = out.replace("\t\t\t\t\t\t<oub:validity.end.date></oub:validity.end.date>\n", "")
			out = out.replace("\t\t\t\t\t\t<oub:effective.end.date></oub:effective.end.date>\n", "")
			out = out.replace("\t\t\t\t\t\t<oub:information.text></oub:information.text>\n", "")

			self.xml = out

			app.transaction_id 				+= 1
			app.message_id 					+= 1
		else:
			self.xml = ""
