import classes.functions as f
import classes.globals as g
from datetime import datetime
import sys

from classes.measure_component import measure_component
#from classes.measure_excluded_geographical_area import measure_excluded_geographical_area

class measure(object):
	def __init__(self, geographical_area_id, goods_nomenclature_item_id, duty_amount, start_date_override, end_date_override):
		# from parameters
		self.geographical_area_id						= geographical_area_id
		self.goods_nomenclature_item_id 				= goods_nomenclature_item_id
		self.duty_amount		        				= duty_amount
		#self.exclusion_string		        			= exclusion_string
		self.measure_excluded_geographical_area_list	= []
		self.validity_end_date							= ""
		self.start_date_override = start_date_override
		self.end_date_override = end_date_override

		# Get GEO SID
		for geo in g.app.geographical_area_list:
			if geo.geographical_area_id == self.geographical_area_id:
				self.geographical_area_sid = geo.geographical_area_sid
				break

		# Get the goods nomenclature SID and its end date; this will override anything else
		sql = """SELECT goods_nomenclature_sid, validity_end_date FROM goods_nomenclatures WHERE producline_suffix = '80'
		AND goods_nomenclature_item_id = %s ORDER BY validity_start_date DESC LIMIT 1"""
		params = [
			self.goods_nomenclature_item_id
		]

		cur = g.app.conn.cursor()
		cur.execute(sql, params)
		rows = cur.fetchall()

		if len(rows) > 0:
			self.goods_nomenclature_sid = rows[0][0]
			if rows[0][1] == None:
				self.validity_end_date = ""
			else:
				self.validity_end_date = rows[0][1]
				self.validity_end_date = datetime.strftime(self.validity_end_date, "%Y-%m-%d")
				#self.validity_end_date = f.mdate(self.validity_end_date)
		else:
			print ("Error - incorrect goods nomenclature item ID - ", self.goods_nomenclature_item_id)
			self.goods_nomenclature_sid = -1


		# Initialised
		self.justification_regulation_id		= ""
		self.justification_regulation_role		= ""
		self.measure_generating_regulation_role	= 1
		self.stopped_flag						= "0"
		self.additional_code_type_id			= ""
		self.additional_code_id					= ""
		self.additional_code_sid				= ""
		self.reduction_indicator				= ""
		self.export_refund_nomenclature_sid		= ""

		self.measure_component_list = []
		self.measure_sid = g.app.last_measure_sid # This is temporary
		g.app.last_measure_sid += 1

		self.monetary_unit_code					= ""
		self.measurement_unit_code				= ""
		self.measurement_unit_qualifier_code	= ""
		obj = measure_component(self.measure_sid, self.duty_amount, self.monetary_unit_code, self.measurement_unit_code, self.measurement_unit_qualifier_code)
		self.measure_component_list.append(obj)

		#print ("In measure init", g.app.last_measure_sid)
		#sys.exit()

	def transfer_sid(self):
		pass

	def xml(self):
		if self.goods_nomenclature_sid == -1:
			return ""

		s = g.app.template_measure
		s = s.replace("[TRANSACTION_ID]",         			str(g.app.transaction_id))
		s = s.replace("[MESSAGE_ID]",             			str(g.app.message_id))
		s = s.replace("[RECORD_SEQUENCE_NUMBER]", 			str(g.app.message_id))

		for obj in self.measure_component_list:
			obj.measure_sid = self.measure_sid
			obj.update_type = "3"
			obj.measure_sid = self.measure_sid

		for obj in self.measure_excluded_geographical_area_list:
			obj.measure_sid = self.measure_sid
			obj.update_type = "3"

		"""
		for obj in self.measure_condition_list:
			obj.action = "restart"
			obj.update_type = "3"
			obj.measure_sid = self.measure_sid
			app.last_measure_condition_sid += 1
			obj.measure_condition_sid = app.last_measure_condition_sid
			my_condition = [obj.measure_condition_sid_original, obj.measure_condition_sid]
			list_conditions.append (my_condition)

		for obj in self.measure_condition_component_list:
			obj.action = "restart"
			obj.update_type = "3"
			obj.measure_sid = self.measure_sid

			for cond in list_conditions:
				if obj.measure_condition_sid == cond[0]:
					obj.measure_condition_sid = cond[1]
					break

		for obj in self.measure_footnote_list:
			obj.action = "restart"
			obj.update_type = "3"
			obj.measure_sid = self.measure_sid

		for obj in self.measure_partial_temporary_stop_list:
			obj.action = "restart"
			obj.update_type = "3"
			obj.measure_sid = self.measure_sid
		"""

		self.measure_type_id = "142"

		# Bring in the date overrides, if one is specified
		if self.start_date_override == "":
			self.validity_start_date = f.mdate(g.app.critical_date_plus_one)
		else:
			self.validity_start_date = self.start_date_override

		# Bring in the end date overrides, but only if this has not already been 
		# overriden by the end date to the commodity code
		if self.validity_end_date == "":
			if self.end_date_override == "":
				self.validity_end_date = ""
			else:
				self.validity_end_date = self.end_date_override

			
		my_area = int(self.geographical_area_id)
		if my_area == 2020:
			self.measure_generating_regulation_id = "U1900010"
		elif my_area == 2027:
			self.measure_generating_regulation_id = "U1900011"
		elif my_area == 2005:
			self.measure_generating_regulation_id = "U1900012"
		elif my_area == 1032:
			self.measure_generating_regulation_id = "U1900013"
		else:
			print (self.geographical_area_id)
			sys.exit()


		self.justification_regulation_id = ""
		self.justification_regulation_role = ""

		if self.validity_end_date != "":
			self.justification_regulation_id = self.measure_generating_regulation_id
			self.justification_regulation_role = self.measure_generating_regulation_role
			
		self.measure_generating_regulation_role = "1"
		self.stopped_flag = "0"
		self.quota_order_number_id = ""
		self.additional_code_type_id = ""
		self.additional_code_sid = ""
		self.reduction_indicator = ""
		self.export_refund_nomenclature_sid = ""

		s = s.replace("[UPDATE_TYPE]",                        "3")
		s = s.replace("[MEASURE_SID]",                        f.mstr(self.measure_sid))
		s = s.replace("[MEASURE_TYPE_ID]",                    f.mstr(self.measure_type_id))
		s = s.replace("[GEOGRAPHICAL_AREA_ID]",               f.mstr(self.geographical_area_id))
		s = s.replace("[GOODS_NOMENCLATURE_ITEM_ID]",         f.mstr(self.goods_nomenclature_item_id))
		s = s.replace("[VALIDITY_START_DATE]",                self.validity_start_date)
		s = s.replace("[MEASURE_GENERATING_REGULATION_ROLE]", f.mstr(self.measure_generating_regulation_role))
		s = s.replace("[MEASURE_GENERATING_REGULATION_ID]",   f.mstr(self.measure_generating_regulation_id))
		s = s.replace("[VALIDITY_END_DATE]",                  self.validity_end_date)
		s = s.replace("[JUSTIFICATION_REGULATION_ROLE]",      f.mstr(self.justification_regulation_role))
		s = s.replace("[JUSTIFICATION_REGULATION_ID]",        f.mstr(self.justification_regulation_id))
		s = s.replace("[STOPPED_FLAG]",                       self.stopped_flag)
		s = s.replace("[GEOGRAPHICAL_AREA_SID]",              f.mstr(self.geographical_area_sid))
		s = s.replace("[GOODS_NOMENCLATURE_SID]",             f.mstr(self.goods_nomenclature_sid))
		s = s.replace("[ORDERNUMBER]",                        f.mstr(self.quota_order_number_id))
		s = s.replace("[ADDITIONAL_CODE_TYPE_ID]",            f.mstr(self.additional_code_type_id))
		s = s.replace("[ADDITIONAL_CODE_ID]",                 f.mstr(self.additional_code_id))
		s = s.replace("[ADDITIONAL_CODE_SID]",                f.mstr(self.additional_code_sid))
		s = s.replace("[REDUCTION_INDICATOR]",                f.mstr(self.reduction_indicator))
		s = s.replace("[EXPORT_REFUND_NOMENCLATURE_SID]",     f.mstr(self.export_refund_nomenclature_sid))

		s = s.replace("\t\t\t\t\t\t<oub:validity.end.date></oub:validity.end.date>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:goods.nomenclature.item.id></oub:goods.nomenclature.item.id>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:additional.code.type></oub:additional.code.type>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:additional.code></oub:additional.code>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:ordernumber></oub:ordernumber>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:reduction.indicator></oub:reduction.indicator>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:justification.regulation.role></oub:justification.regulation.role>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:justification.regulation.id></oub:justification.regulation.id>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:geographical.area.sid></oub:geographical.area.sid>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:goods.nomenclature.sid></oub:goods.nomenclature.sid>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:additional.code.sid></oub:additional.code.sid>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:export.refund.nomenclature.sid></oub:export.refund.nomenclature.sid>\n", "")

		g.app.message_id += 1
		
		self.component_content = ""
		self.condition_content = ""
		self.condition_component_content = ""
		self.exclusion_content = ""
		self.footnote_content = ""
		self.pts_content = ""

		for obj in self.measure_component_list:
			self.component_content += obj.xml()

		for obj in self.measure_excluded_geographical_area_list:
			#writeprint ("Printing exclusion")
			obj.measure_sid = self.measure_sid
			self.exclusion_content += obj.xml()

		"""
		for obj in self.measure_condition_list:
			obj.action = self.action
			self.condition_content += obj.xml()

		for obj in self.measure_condition_component_list:
			obj.action = self.action
			self.condition_component_content += obj.xml()

		for obj in self.measure_footnote_list:
			obj.action = self.action
			self.footnote_content += obj.xml()

		for obj in self.measure_partial_temporary_stop_list:
			obj.action = self.action
			self.pts_content += obj.xml()
			#print (self.pts_content)
		"""
		s = s.replace("[COMPONENTS]\n", 			self.component_content)
		s = s.replace("[CONDITIONS]\n", 			self.condition_content)
		s = s.replace("[CONDITION_COMPONENTS]\n",	self.condition_component_content)
		s = s.replace("[EXCLUDED]\n", 				self.exclusion_content)
		s = s.replace("[FOOTNOTES]\n", 				self.footnote_content)
		s = s.replace("[PTS]\n",	 				self.pts_content)

		g.app.transaction_id += 1
		return (s)