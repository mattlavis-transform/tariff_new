import classes.functions as f
import classes.globals as g
import datetime
import sys

from classes.quota_order_number_origin import quota_order_number_origin
from classes.quota_order_number_origin_exclusion import quota_order_number_origin_exclusion

class quota_order_number(object):
	def __init__(self, quota_order_number_id, regulation_id, measure_type_id, origin_string, origin_exclusion_string, validity_start_date, subject, status):
		self.quota_order_number_id      = quota_order_number_id
		self.regulation_id              = regulation_id
		self.measure_type_id    	    = measure_type_id
		self.origin_string              = origin_string
		self.origin_exclusion_string    = origin_exclusion_string
		self.validity_start_date    	= validity_start_date
		self.subject		            = subject
		self.status		            	= status

		if status == "Y" or status == "New":
			status = "New"
		else:
			status = "Existing"

		self.trim_measure_type()

		self.check_existence()
		self.get_origins()

		self.quota_definition_list		= []
		self.measure_list				= []

		self.assign_definitions()
		self.assign_measures()

	def trim_measure_type(self):
		space_pos = self.measure_type_id.find(" ")
		#print ("SP", str(space_pos))
		self.measure_type_id_trimmed = self.measure_type_id[0:space_pos]


	def assign_definitions(self):
		for obj in g.app.quota_definition_list:
			if obj.quota_order_number_id == self.quota_order_number_id:
				self.quota_definition_list.append (obj)

	def assign_measures(self):
		for obj in g.app.measure_list:
			#print ("finding")
			if obj.quota_order_number_id == self.quota_order_number_id:
				self.measure_list.append (obj)

	def get_origins(self):
		self.origins				= self.origin_string.split(",")
		self.origin_exclusion_string = self.origin_exclusion_string.strip()
		if (len(self.origin_exclusion_string) > 0):
			self.origin_exclusions		= self.origin_exclusion_string.split(",")
		else:
			self.origin_exclusions = []
		self.origin_list 			= []
		self.origin_exclusion_list	= []
		self.origin_with_exclusions = ""

		# Get the origins and put them into an array
		# Check if any of these are a geographical area code (type 1)
		# If so, then this is suitable for exclusions (which are attached to the origin)
		for geographical_area_id in self.origins:
			obj_quota_order_number_origin = quota_order_number_origin(self.quota_order_number_sid, geographical_area_id, self.validity_start_date)
			if obj_quota_order_number_origin.geographical_code == "1":
				for geographical_area_id2 in self.origin_exclusions:
					obj_quota_order_number_origin_exclusion = quota_order_number_origin_exclusion(obj_quota_order_number_origin.quota_order_number_origin_sid, geographical_area_id2)
					obj_quota_order_number_origin.exclusion_list.append (obj_quota_order_number_origin_exclusion)



			self.origin_list.append (obj_quota_order_number_origin)





	def check_existence(self):
		sql = "SELECT quota_order_number_sid FROM quota_order_numbers WHERE quota_order_number_id = '" + self.quota_order_number_id + "' ORDER BY validity_start_date DESC LIMIT 1"
		cur = g.app.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		if len(rows) > 0:
			self.exists = True
			#print ("Exists")
			self.quota_order_number_sid = rows[0][0]
		else:
			#print ("Does not exist")
			# Get a new SID
			self.exists = False
			g.app.last_quota_order_number_sid += 1
			self.quota_order_number_sid = g.app.last_quota_order_number_sid

	def xml(self):
		#if self.status == "Existing":
		#	return ("")
		s = ""
		if self.exists == False:
			s = g.app.template_quota_order_number
			s = s.replace("[TRANSACTION_ID]",			str(g.app.transaction_id))
			s = s.replace("[MESSAGE_ID]",				str(g.app.message_id))
			s = s.replace("[RECORD_SEQUENCE_NUMBER]",	str(g.app.message_id))
			s = s.replace("[UPDATE_TYPE]",				"3")
			s = s.replace("[QUOTA_ORDER_NUMBER_SID]",	str(self.quota_order_number_sid))
			s = s.replace("[QUOTA_ORDER_NUMBER_ID]",	self.quota_order_number_id)
			s = s.replace("[VALIDITY_START_DATE]",		self.validity_start_date)
			s = s.replace("[VALIDITY_END_DATE]",		"")

			s = s.replace("\t\t\t\t\t\t<oub:validity.end.date></oub:validity.end.date>\n", "")
			g.app.message_id +=1
			g.app.transaction_id +=1


			for obj in self.origin_list:
				g.app.last_quota_order_number_origin_sid += 1
				obj.quota_order_number_origin_sid = g.app.last_quota_order_number_origin_sid
				obj.quota_order_number_sid = self.quota_order_number_sid
				obj.description = self.subject
				s += obj.xml()

		for obj in self.quota_definition_list:
			obj.quota_order_number_sid = self.quota_order_number_sid
			obj.description = self.subject
			s += obj.xml()

		return (s)

	def measure_xml(self):
		# Loop through all order numbers origins (geographies), then each definition, then each commodity code
		s = ""
		i = 1
		for o in self.origin_list:
			for d in self.quota_definition_list:
				for m in self.measure_list:
					m.measure_sid = g.app.last_measure_sid
					g.app.last_measure_sid += 1
					m.quota_order_number_id = self.quota_order_number_id
					m.geographical_area_id = o.geographical_area_id
					m.geographical_area_sid = o.geographical_area_sid
					m.measure_generating_regulation_id = self.regulation_id
					m.measure_type_id = self.measure_type_id_trimmed
					m.validity_start_date = d.validity_start_date
					m.validity_end_date = d.validity_end_date
					"""
					if len(o.exclusion_list) > 0:
						for x in o.exclusion_list:
							print (x)
						sys.exit()
					"""
					m.measure_excluded_geographical_area_list = o.exclusion_list
					s += m.xml()
					g.app.transaction_id += 1
					i += 1

		return (s)