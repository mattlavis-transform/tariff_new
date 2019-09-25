import classes.functions as f
import classes.globals as g
import datetime
import sys

class quota_order_number_origin(object):
	def __init__(self, quota_order_number_sid, geographical_area_id, validity_start_date):
		self.quota_order_number_sid 		= quota_order_number_sid
		self.geographical_area_id   		= geographical_area_id
		self.validity_start_date    		= validity_start_date
		self.quota_order_number_origin_sid	= -1 # Temp -- g.app.last_quota_order_number_origin_sid
		self.exclusion_list = []

		self.get_geography()

	def get_geography(self):
		sql = """SELECT geographical_area_sid, geographical_code FROM geographical_areas WHERE
		geographical_area_id = '""" + self.geographical_area_id + """' ORDER BY validity_start_date DESC LIMIT 1"""
		cur = g.app.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		if len(rows) > 0:
			self.geographical_area_sid	= rows[0][0]
			self.geographical_code		= rows[0][1] # 1 is a group and can therefore have exclusions
		else:
			print ("Missing geography", self.geographical_area_id, sql)
			sys.exit()

	def xml(self):
		s = ""
		s = g.app.template_quota_order_number_origin
		s = s.replace("[TRANSACTION_ID]",			        str(g.app.transaction_id))
		s = s.replace("[MESSAGE_ID]",				        str(g.app.message_id))
		s = s.replace("[RECORD_SEQUENCE_NUMBER]",	        str(g.app.message_id))
		s = s.replace("[UPDATE_TYPE]",				        "3")
		s = s.replace("[QUOTA_ORDER_NUMBER_ORIGIN_SID]",	str(self.quota_order_number_origin_sid))
		s = s.replace("[QUOTA_ORDER_NUMBER_SID]",	        str(self.quota_order_number_sid))
		s = s.replace("[GEOGRAPHICAL_AREA_ID]",	            str(self.geographical_area_id))
		s = s.replace("[VALIDITY_START_DATE]",		        self.validity_start_date)
		s = s.replace("[GEOGRAPHICAL_AREA_SID]",	        str(self.geographical_area_sid))
		s = s.replace("[VALIDITY_END_DATE]",		        "")

		s = s.replace("\t\t\t\t\t\t<oub:validity.end.date></oub:validity.end.date>\n", "")
		
		exclusion_xml = ""
		if len(self.exclusion_list) > 0:
			for obj in self.exclusion_list:
				obj.quota_order_number_origin_sid = self.quota_order_number_origin_sid
				exclusion_xml += obj.xml()

		g.app.message_id +=1

		s = s.replace("[EXCLUSIONS]\n", exclusion_xml)
		
		
		g.app.transaction_id +=1
		#g.app.last_quota_order_number_origin_sid += 1

		return (s)

