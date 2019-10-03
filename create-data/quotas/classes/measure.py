import classes.functions as f
import classes.globals as g
import datetime
import sys

from classes.measure_component import measure_component
from classes.measure_condition import measure_condition
from classes.footnote_association_measure import footnote_association_measure


class measure(object):
	def __init__(self, goods_nomenclature_item_id, quota_order_number_id, origin_identifier, duty_amount,
			monetary_unit_code, measurement_unit_code, measurement_unit_qualifier_code, measure_type_id, 
			start_date_override = "", end_date_override = "", measure_sid = -1):
		# from parameters
		self.goods_nomenclature_item_id 		= goods_nomenclature_item_id
		self.quota_order_number_id    			= quota_order_number_id
		self.origin_identifier    				= origin_identifier
		self.duty_amount		        		= duty_amount
		self.monetary_unit_code      			= monetary_unit_code
		self.measurement_unit_code      		= measurement_unit_code
		self.measurement_unit_qualifier_code	= measurement_unit_qualifier_code
		self.measure_type_id					= measure_type_id
		self.start_date_override				= start_date_override
		self.end_date_override					= end_date_override
		self.goods_nomenclature_sid     		= 0
		self.duty_list							= []

		l = len(self.goods_nomenclature_item_id)
		if (l < 10):
			self.goods_nomenclature_item_id += ("0" * (10 - l))

		# Get the goods nomenclature SID
		sql = """SELECT goods_nomenclature_sid FROM goods_nomenclatures
		WHERE producline_suffix = '80'
		AND goods_nomenclature_item_id = '""" + self.goods_nomenclature_item_id + """'
		AND (validity_end_date is null)
		ORDER BY validity_start_date DESC LIMIT 1"""
		#print (sql)
		cur = g.app.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		if len(rows) > 0:
			self.goods_nomenclature_sid = rows[0][0]
		else:
			#print ("Error - incorrect goods nomenclature item ID -", self.goods_nomenclature_item_id)
			self.goods_nomenclature_sid = -1

		
		# Initialised
		self.justification_regulation_id		= "" # self.measure_generating_regulation_id
		self.justification_regulation_role		= "1"
		self.measure_generating_regulation_role	= 1
		self.stopped_flag						= "0"
		self.additional_code_type_id			= ""
		self.additional_code_id					= ""
		self.additional_code_sid				= ""
		self.reduction_indicator				= ""
		self.export_refund_nomenclature_sid		= ""

		self.measure_component_list = []
		self.measure_sid = measure_sid


	def duty_string(self):
		if self.monetary_unit_code == "":
			return str(self.duty_amount) + "%"
		else:
			out = "€"
			out += format(self.duty_amount, "^0.3f")
			if self.measurement_unit_code != "":
				out += " per " + self.fmt_mu()
			
			if self.measurement_unit_qualifier_code != "":
				out += " (" + self.fmt_muq() + ")"
			
			return (out)


	def fmt_muq(self):
		if self.measurement_unit_qualifier_code == "E":
			return ("net of drained weight")
		else:
			return ("blah blah blah")
	

	def fmt_mu(self):
		if self.measurement_unit_code == "TNE":
			return ("1000kg")
		elif self.measurement_unit_code == "DTN":
			return ("100kg")
		elif self.measurement_unit_code == "DAP":
			return ("Decatonne, corrected according to polarisation")
		elif self.measurement_unit_code == "HLT":
			return ("hl")
		else:
			return self.measurement_unit_code


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

		if self.measure_excluded_geographical_area_list != None:
			for obj in self.measure_excluded_geographical_area_list:
				obj.measure_sid = self.measure_sid
				obj.update_type = "3"

		"""
		if self.start_date_override != "":
			self.validity_start_date = self.start_date_override
		if self.end_date_override != "":
			self.validity_end_date = self.end_date_override
		"""

		s = s.replace("[UPDATE_TYPE]",                        	"3")
		s = s.replace("[MEASURE_SID]",                        	f.mstr(self.measure_sid))
		s = s.replace("[MEASURE_TYPE_ID]",                    	f.mstr(self.measure_type_id))
		s = s.replace("[GEOGRAPHICAL_AREA_ID]",               	f.mstr(self.geographical_area_id))
		s = s.replace("[GOODS_NOMENCLATURE_ITEM_ID]",         	f.mstr(self.goods_nomenclature_item_id))
		s = s.replace("[VALIDITY_START_DATE]",                	self.validity_start_date)
		s = s.replace("[MEASURE_GENERATING_REGULATION_ROLE]",	f.mstr(self.measure_generating_regulation_role))
		s = s.replace("[MEASURE_GENERATING_REGULATION_ID]",		f.mstr(self.measure_generating_regulation_id))
		if self.validity_end_date == None:
			print (self.goods_nomenclature_item_id, self.quota_order_number_id)
		s = s.replace("[VALIDITY_END_DATE]",					f.mstr(self.validity_end_date))
		s = s.replace("[JUSTIFICATION_REGULATION_ROLE]",      	f.mstr(self.justification_regulation_role))
		s = s.replace("[JUSTIFICATION_REGULATION_ID]",        	f.mstr(self.justification_regulation_id))
		s = s.replace("[STOPPED_FLAG]",                       	self.stopped_flag)
		s = s.replace("[GEOGRAPHICAL_AREA_SID]",              	f.mstr(self.geographical_area_sid))
		s = s.replace("[GOODS_NOMENCLATURE_SID]",            	f.mstr(self.goods_nomenclature_sid))
		s = s.replace("[ORDERNUMBER]",                        	f.mstr(self.quota_order_number_id))
		s = s.replace("[ADDITIONAL_CODE_TYPE_ID]",            	f.mstr(self.additional_code_type_id))
		s = s.replace("[ADDITIONAL_CODE_ID]",                 	f.mstr(self.additional_code_id))
		s = s.replace("[ADDITIONAL_CODE_SID]",                	f.mstr(self.additional_code_sid))
		s = s.replace("[REDUCTION_INDICATOR]",                	f.mstr(self.reduction_indicator))
		s = s.replace("[EXPORT_REFUND_NOMENCLATURE_SID]",     	f.mstr(self.export_refund_nomenclature_sid))

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
		#print (g.app.last_measure_sid)
		
		self.component_content = ""
		self.condition_content = ""
		self.condition_component_content = ""
		self.exclusion_content = ""
		self.footnote_content = ""
		self.pts_content = ""

		for obj in self.measure_component_list:
			if self.quota_order_number_id == "091370":
				print ("Adding components for 091370")
			self.component_content += obj.xml()

		for obj in self.measure_excluded_geographical_area_list:
			obj.measure_sid = self.measure_sid
			self.exclusion_content += obj.measure_xml()

		if self.quota_order_number_id[0:3] == "094":
			# Add the standard conditions
			self.conditions = []
			my_condition = measure_condition(self.measure_sid, "C", 1, "27", "L", "001")
			self.conditions.append (my_condition)

			my_condition = measure_condition(self.measure_sid, "C", 2, "07", None, None)
			self.conditions.append (my_condition)

			my_condition = measure_condition(self.measure_sid, "Q", 1, "27", "Y", "100")
			self.conditions.append (my_condition)

			my_condition = measure_condition(self.measure_sid, "Q", 2, "07", None, None)
			self.conditions.append (my_condition)

			for c in self.conditions:
				self.condition_content += c.xml()


			# Add the standard footnote
			self.footnotes = []
			fn = footnote_association_measure(self.measure_sid, "CD", "356")
			self.footnotes.append (fn)

			for fn in self.footnotes:
				self.footnote_content += fn.xml()

		
		s = s.replace("[COMPONENTS]\n", 			self.component_content)
		s = s.replace("[CONDITIONS]\n", 			self.condition_content)
		s = s.replace("[CONDITION_COMPONENTS]\n",	self.condition_component_content)
		s = s.replace("[EXCLUDED]\n", 				self.exclusion_content)
		s = s.replace("[FOOTNOTES]\n", 				self.footnote_content)
		s = s.replace("[PTS]\n",	 				self.pts_content)

		return (s)