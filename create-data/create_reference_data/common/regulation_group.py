import common.functions as f
import common.objects as o
import datetime
import sys

class regulation_group(object):
	def __init__(self, regulation_group_id, description, validity_start_date, stype):
		# from parameters
		self.regulation_group_id 	= regulation_group_id
		self.validity_start_date 	= validity_start_date
		self.description         	= description
		self.type					= stype
		self.xml = ""

	def writeXML(self):
		app = o.app
		if self.type == "update":
			s = app.update_regulation_group_XML
		else:
			s = app.insert_regulation_group_XML

		s = s.replace("[TRANSACTION_ID]",              str(app.transaction_id))
		s = s.replace("[MESSAGE_ID1]",                 str(app.message_id))
		s = s.replace("[MESSAGE_ID2]",                 str(app.message_id + 1))
		s = s.replace("[RECORD_SEQUENCE_NUMBER1]",     str(app.message_id))
		s = s.replace("[RECORD_SEQUENCE_NUMBER2]",     str(app.message_id + 1))

		s = s.replace("[REGULATION_GROUP_ID]",          f.mstr(self.regulation_group_id))
		s = s.replace("[VALIDITY_START_DATE]",          f.mdate(self.validity_start_date))
		s = s.replace("[VALIDITY_END_DATE]",            "")
		s = s.replace("[DESCRIPTION]",                  f.mstr(self.description))

		s = s.replace("\t\t\t\t\t\t<oub:validity.end.date></oub:validity.end.date>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:description></oub:description>\n", "")

		self.xml = s

		app.message_id += 2
		app.transaction_id += 1
		return (s)
