import classes.functions as f
import classes.globals as g
import datetime
import sys

class quota_definition(object):
	def __init__(self, quota_definition_sid, quota_order_number_id, validity_start_date, validity_end_date, quota_order_number_sid, volume,
		initial_volume, measurement_unit_code, maximum_precision, critical_state, critical_threshold, monetary_unit_code, 
		measurement_unit_qualifier_code, description, action):

		# from parameters
		self.quota_definition_sid				= quota_definition_sid
		self.quota_order_number_id  			= quota_order_number_id
		self.validity_start_date    			= validity_start_date
		self.validity_end_date      			= validity_end_date
		self.quota_order_number_sid  			= quota_order_number_sid
		self.volume      						= volume
		self.initial_volume      				= initial_volume
		self.measurement_unit_code  			= measurement_unit_code
		self.maximum_precision      			= maximum_precision
		self.critical_state      				= critical_state
		self.critical_threshold      			= critical_threshold
		self.monetary_unit_code     			= monetary_unit_code
		self.measurement_unit_qualifier_code	= measurement_unit_qualifier_code
		self.description      					= description
		self.action		      					= action
		self.update_type						= ""
		self.omit								= False
		self.quota_definition_sid_new			= 0

		# Initialised
		self.quota_association_list			= []
		self.quota_suspension_period_list	= []

		# worked out
		self.mappedin1819           = False

		self.get_period_type()
		self.cleanse_description()

		app = g.app
		if self.action == "truncate":
			self.validity_end_date = app.critical_date
			self.update_type = "1"
			if self.description == None:
				self.description = ""
			if len(self.description) > 440:
				self.description = self.description[:440] + " ... definition truncated by DIT TAP script"
			else:
				self.description = self.description + " ... definition truncated by DIT TAP script"

		if self.action == "insert":
			self.update_type = "3"
			self.quota_definition_sid_new			= app.last_quota_definition_sid
			app.quota_definition_sid_mapping_list.append([self.quota_definition_sid, self.quota_definition_sid_new])
			app.last_quota_definition_sid += 1

			# Get the quota description from the imported CSV list
			description_found = False
			for qon in app.quota_description_list:
				if qon.quota_order_number_id == self.quota_order_number_id:
					self.description = f.cleanse(qon.description)
					#print (qon.description)
					description_found = True
					break
			if description_found == False:
				print (self.quota_order_number_id, "No description match")
			else:
				#print (self.quota_order_number_id, "Description match")
				pass

	def cleanse_description(self):
		if self.description != None:
			self.description = self.description.replace("<br>", "; ")
			self.description = self.description.replace(">", "&gt;")
			self.description = self.description.replace("<", "&lt;")

	def xml(self):
		app = g.app
		s = app.template_quota_definition

		if self.action == "insert":
			self.update_type = "3"
			self.quota_definition_sid_old		= self.quota_definition_sid
			self.quota_definition_sid			= self.quota_definition_sid_new
			pass

		# Add in any related suspensions to the transaction
		self.suspension_period_content = ""
		if self.action in ("delete", "truncate"):
			for obj in self.quota_suspension_period_list:
				self.suspension_period_content += obj.xml()

		# Add in any related associations to the transaction
		self.association_content = ""
		if self.action in ("delete", "truncate"):
			for obj in self.quota_association_list:
				self.association_content += obj.xml()

		if self.action == "delete":
			self.update_type = "2"
		elif self.action in ("update", "truncate"):
			self.update_type = "1"


		if self.volume is None:
			print (self.quota_definition_sid_old)
			sys.exit()
			self.volume = 0
		if self.initial_volume is None:
			self.initial_volume = 0
		
		s = s.replace("[TRANSACTION_ID]",         			str(app.transaction_id))
		s = s.replace("[MESSAGE_ID]",             			str(app.sequence_id))
		s = s.replace("[RECORD_SEQUENCE_NUMBER]", 			str(app.sequence_id))
		s = s.replace("[UPDATE_TYPE]",						self.update_type)

		s = s.replace("[QUOTA_DEFINITION_SID]",             f.mstr(self.quota_definition_sid))
		s = s.replace("[QUOTA_ORDER_NUMBER_ID]",            f.mstr(self.quota_order_number_id))
		s = s.replace("[VALIDITY_START_DATE]",              f.mdate(self.validity_start_date))
		s = s.replace("[VALIDITY_END_DATE]",                f.mdate(self.validity_end_date))
		s = s.replace("[QUOTA_ORDER_NUMBER_SID]",           f.mstr(self.quota_order_number_sid))
		#s = s.replace("[VOLUME]",                           f.mstr(int(self.volume)))
		#s = s.replace("[INITIAL_VOLUME]",                   f.mstr(int(self.initial_volume)))
		s = s.replace("[VOLUME]",                           f.mstr(self.volume))
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

		app.sequence_id += 1
		app.transaction_id += 1


		s = s.replace("[SUSPENSION_PERIODS]\n",	self.suspension_period_content)
		s = s.replace("[ASSOCIATIONS]\n",		self.association_content)
		return (s)

	def get_period_type(self):
		self.validity_start_year	= self.validity_start_date.year
		self.validity_start_month	= self.validity_start_date.month
		self.validity_start_day		= self.validity_start_date.day

		self.validity_end_year		= self.validity_end_date.year
		self.validity_end_month		= self.validity_end_date.month
		self.validity_end_day		= self.validity_end_date.day

		# Get period length
		self.period_length			= (self.validity_end_date - self.validity_start_date).days + 1
		if self.period_length in (365, 366):
			self.period_type = "Annual"
		elif self.period_length in (28, 29, 30, 31):
			self.period_type = "Monthly"
		elif self.period_length in (88, 89, 90, 91, 92, 93):
			self.period_type = "Quarterly"
		elif self.period_length in (181, 182, 183):
			self.period_type = "Half-yearly"
		elif self.validity_start_day == 1 and self.validity_end_day in (28, 29, 30, 31):
			self.period_type = "Multiple of monthly"
		else:
			self.period_type = "Custom"
			pass

	def get_balance(self):
		app = g.app
		self.initial_volume_uk = 0
		for b in app.quota_balance_list:
			if b.quota_order_number_id == self.quota_order_number_id:
				self.initial_volume_uk = b.balance
				break

