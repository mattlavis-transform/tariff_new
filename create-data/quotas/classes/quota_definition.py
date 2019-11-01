import classes.functions as f
import classes.globals as g
import datetime
import sys

class quota_definition(object):
	def __init__(self, quota_order_number_id, quota_order_number_sid, measure_type, quota_method, validity_start_date, validity_end_date, length, initial_volume,
					measurement_unit_code, maximum_precision, critical_state, critical_threshold, monetary_unit_code,
					measurement_unit_qualifier_code, blocking_period_start, blocking_period_end, origin_identifier):

		# from parameters
		self.quota_order_number_id  			= quota_order_number_id
		self.quota_order_number_sid  			= quota_order_number_sid
		self.measure_type  						= measure_type
		self.quota_method  						= quota_method
		self.validity_start_date    			= validity_start_date
		self.validity_end_date      			= validity_end_date
		self.length      						= length
		self.volume      						= initial_volume
		self.initial_volume      				= initial_volume
		self.measurement_unit_code  			= measurement_unit_code
		self.maximum_precision      			= maximum_precision
		self.critical_state      				= critical_state
		self.critical_threshold      			= critical_threshold
		self.monetary_unit_code     			= monetary_unit_code
		self.measurement_unit_qualifier_code	= measurement_unit_qualifier_code
		self.blocking_period_start      		= blocking_period_start
		self.blocking_period_end		      	= blocking_period_end
		self.origin_identifier 					= origin_identifier
		self.quota_definition_sid				= g.app.last_quota_definition_sid
		g.app.last_quota_definition_sid +=1

		self.description = ""

		# Initialised
		self.quota_blocking_period_list	= []

	def xml(self):
		if self.quota_order_number_id[0:3] == "094":
			return ""

		if self.initial_volume == 0:
			return ""
			
		self.quota_association_content = ""
		s = g.app.template_quota_definition

		self.description = ""
		for obj in g.app.quota_descriptions:
			if obj[0] == self.quota_order_number_id:
				self.description = obj[2]
				break

		if self.description == "":
			print ("Missing quota description on quota", self.quota_order_number_id)
			sys.exit()

		self.description += " - from " + str(self.validity_start_date)  + " to " + str(self.validity_end_date)

		s = s.replace("[TRANSACTION_ID]",         			str(g.app.transaction_id))
		s = s.replace("[MESSAGE_ID]",             			str(g.app.message_id))
		s = s.replace("[RECORD_SEQUENCE_NUMBER]", 			str(g.app.message_id))
		s = s.replace("[UPDATE_TYPE]",						"3")

		s = s.replace("[QUOTA_DEFINITION_SID]",             f.mstr(self.quota_definition_sid))
		s = s.replace("[QUOTA_ORDER_NUMBER_ID]",            f.mstr(self.quota_order_number_id))
		s = s.replace("[VALIDITY_START_DATE]",              str(self.validity_start_date))
		s = s.replace("[VALIDITY_END_DATE]",                str(self.validity_end_date))
		s = s.replace("[QUOTA_ORDER_NUMBER_SID]",           f.mstr(self.quota_order_number_sid))
		s = s.replace("[VOLUME]",                           f.mstr(self.initial_volume))
		s = s.replace("[INITIAL_VOLUME]",                   f.mstr(self.initial_volume))
		s = s.replace("[MONETARY_UNIT_CODE]",               f.mstr(self.monetary_unit_code))
		s = s.replace("[MEASUREMENT_UNIT_CODE]",            f.mstr(self.measurement_unit_code))
		s = s.replace("[MEASUREMENT_UNIT_QUALIFIER_CODE]",  f.mstr(self.measurement_unit_qualifier_code))
		s = s.replace("[MAXIMUM_PRECISION]",                f.mstr(self.maximum_precision))
		s = s.replace("[CRITICAL_STATUS]",                  f.mstr(self.critical_state))
		s = s.replace("[CRITICAL_THRESHOLD]",               f.mstr(self.critical_threshold))
		s = s.replace("[DESCRIPTION]",                      f.mstr(self.description))

		s = s.replace("\t\t\t\t\t\t<oub:validity.end.date></oub:validity.end.date>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:monetary.unit.code></oub:monetary.unit.code>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:measurement.unit.code></oub:measurement.unit.code>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:measurement.unit.qualifier.code></oub:measurement.unit.qualifier.code>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:description></oub:description>\n", "")

		g.app.completed_quota_definition_list.append(self)

		g.app.message_id += 1

		"""
		for item in g.app.quota_associations_list:
			if item.main_quota_order_number_id == self.quota_order_number_id:
				item.main_quota_definition_sid = self.quota_definition_sid
				self.quota_association_content += item.xml
		"""
		g.app.transaction_id += 1

		return (s)

	def txt(self):
		s = ""
		s += str(self.quota_definition_sid) + ","
		s += self.quota_order_number_id + ","
		s += self.validity_start_date + ","
		s += self.validity_end_date

		return (s)

	def quota_association_xml(self):
		association_xml = ""
		for item in g.app.quota_associations_list:
			if item.main_quota_order_number_id == self.quota_order_number_id:
				s = g.app.template_quota_association
				sub_quota_order_number_id = item.sub_quota_order_number_id


				# Get the associated sub quota definition ID
				for my_def in g.app.completed_quota_definition_list:
					if my_def.quota_order_number_id == sub_quota_order_number_id:
						if my_def.validity_start_date == self.validity_start_date:
							if my_def.validity_end_date == self.validity_end_date:
								item.sub_quota_definition_sid = my_def.quota_definition_sid
								break


				s = s.replace("[TRANSACTION_ID]",         		str(g.app.transaction_id))
				s = s.replace("[MESSAGE_ID]",             		str(g.app.message_id))
				s = s.replace("[RECORD_SEQUENCE_NUMBER]", 		str(g.app.message_id))
				s = s.replace("[UPDATE_TYPE]",					"3")

				s = s.replace("[MAIN_QUOTA_DEFINITION_SID]",    str(self.quota_definition_sid))
				s = s.replace("[SUB_QUOTA_DEFINITION_SID]",     str(item.sub_quota_definition_sid))
				s = s.replace("[RELATION_TYPE]",                item.relation_type)
				s = s.replace("[COEFFICIENT]",                  item.coefficient)

				g.app.message_id += 1

				association_xml += s + "\n"

		return (association_xml)
