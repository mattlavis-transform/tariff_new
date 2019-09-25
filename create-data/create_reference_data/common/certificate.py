import psycopg2
import sys
import os
import csv
import re
import common.functions as fn
import common.objects as o
from unidecode import unidecode

class certificate(object):
	def __init__(self, certificate_type_code, certificate_code, description_old, description, validity_start_date, type):
		self.certificate_type_code				= fn.mstr(certificate_type_code).upper()
		self.certificate_code					= fn.mstr(certificate_code)
		self.description_old	    			= fn.mstr(description_old)
		self.description		    			= fn.mstr(description)
		self.validity_start_date				= fn.mdate(validity_start_date)
		self.type				    			= fn.mstr(type)
		self.needs_change	        			= False
		self.cnt = 0
		self.xml = ""

		if self.description_old != self.description:
			#self.cleanse()
			#self.validate()
			self.same = False
		else:
			self.same = True

		self.get_next_certificate_description_period_sid()

	def get_next_certificate_description_period_sid(self):
		self.certificate_description_period_sid = o.app.last_certificate_description_period_sid

	def writeXML(self, app):
		if not(self.same):
			if self.type == "update":
				out = app.update_certificate_description_XML
			else:
				out = app.insert_certificate_XML
			
			if self.certificate_description_period_sid == "":
				self.certificate_description_period_sid = str(app.base_certificate_description_period_sid)
			
			self.description = fn.cleanse(self.description)
			out = out.replace("[CERTIFICATE_TYPE_CODE]",				self.certificate_type_code)
			out = out.replace("[CERTIFICATE_CODE]",						self.certificate_code)
			out = out.replace("[DESCRIPTION]",							self.description)
			out = out.replace("[VALIDITY_START_DATE]",					self.validity_start_date)
			out = out.replace("[VALIDITY_END_DATE]",					"")
			out = out.replace("[CERTIFICATE_DESCRIPTION_PERIOD_SID]",	str(self.certificate_description_period_sid))
			out = out.replace("[TRANSACTION_ID]",						str(app.transaction_id))
			out = out.replace("[MESSAGE_ID1]",							str(app.message_id))
			out = out.replace("[MESSAGE_ID2]",							str(app.message_id + 1))
			out = out.replace("[MESSAGE_ID3]",							str(app.message_id + 2))
			out = out.replace("[RECORD_SEQUENCE_NUMBER1]",				str(app.message_id))
			out = out.replace("[RECORD_SEQUENCE_NUMBER2]",				str(app.message_id + 1))
			out = out.replace("[RECORD_SEQUENCE_NUMBER3]",				str(app.message_id + 2))

			out = out.replace("\t\t\t\t\t\t<oub:validity.end.date></oub:validity.end.date>\n", "")
			out = out.replace("\t\t\t\t\t\t<oub:description></oub:description>\n", "")

			self.xml = out

			app.base_certificate_description_period_sid	+= 1
			app.transaction_id 				+= 1
			if self.type == "update":
				app.message_id 					+= 2
			else:
				app.message_id 					+= 3
			o.app.last_certificate_description_period_sid += 1
	
	def resolve(self, app):
		s = self.description
		if "reg. " in s.lower():
			app.cnt += 1
		s = re.sub(r", as last amended by Reg. [^.][1,1000].", r".", s)
		s = re.sub(r"issued in accordance with Reg. [^.][1,1000].", r"", s)
		s = s.strip()
		s = s.strip(",")
		self.new_description = s
