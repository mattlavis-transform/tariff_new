import classes.functions as f
import classes.globals as g

class eea_measure(object):
	def __init__(self, geographical_area_id, measure_sid, goods_nomenclature_item_id, validity_start_date,
    validity_end_date):
		# from parameters
		self.geographical_area_id               = geographical_area_id
		self.measure_sid                        = measure_sid
		self.goods_nomenclature_item_id	        = str(goods_nomenclature_item_id)
		self.validity_start_date	            = validity_start_date
		self.validity_end_date	                = validity_end_date
		self.mark_for_measure_trimming			= False
		self.value								= 0

		self.measure_component_list = []

		# Common fields needed by XML
		self.justification_regulation_role = ""
		self.justification_regulation_id = ""
		self.stopped_flag = 0
		self.quota_order_number_id = ""
		self.additional_code_type_id = ""
		self.additional_code_id = ""
		self.additional_code_sid = ""
		self.reduction_indicator = ""
		self.export_refund_nomenclature_sid 	= ""
		self.update_type 						= "3"
		self.measure_type_id					= "142"
		self.validity_start_date				= f.mdate(g.app.critical_date_plus_one)
		self.validity_end_date					= ""
		self.measure_generating_regulation_role = "1"
		self.justification_regulation_role		= ""
		self.justification_regulation_id		= ""

		if g.app.output_profile == "norway":
			self.geographical_area_id2  = "NO"
			self.measure_generating_regulation_id = "P1900140"
			self.geographical_area_sid = "252"
		else:
			self.geographical_area_id2  = "IS"
			self.measure_generating_regulation_id = "P1900130"
			self.geographical_area_sid = "53"


	def combine_duties(self):
		self.combined_duty = ""
		#print (self.goods_nomenclature_item_id, self.geographical_area_id)
		for mc in self.measure_component_list:
			self.combined_duty += mc.duty_string + " "

		self.combined_duty = self.combined_duty.replace("  ", " ")
		self.combined_duty = self.combined_duty.strip()


	def lookup_sid(self):
		for gn in g.app.goods_nomenclature_sid_lookup:
			if gn[1] == self.goods_nomenclature_item_id:
				if gn[2] == "80":
					self.goods_nomenclature_sid = gn[0]
					break

	def xml(self):
		app = g.app
		s = app.template_measure

		self.lookup_sid()
		self.measure_sid = g.app.last_measure_sid

		s = s.replace("[MEASURE_SID]",         					str(g.app.last_measure_sid))

		s = s.replace("[TRANSACTION_ID]",         				str(app.transaction_id))
		s = s.replace("[MESSAGE_ID]",             				str(app.sequence_id))
		s = s.replace("[RECORD_SEQUENCE_NUMBER]", 				str(app.sequence_id))
		s = s.replace("[UPDATE_TYPE]", 							str(self.update_type))
		s = s.replace("[MEASURE_TYPE_ID]", 						str(self.measure_type_id))
		s = s.replace("[VALIDITY_START_DATE]", 					str(self.validity_start_date))
		s = s.replace("[VALIDITY_END_DATE]", 					str(self.validity_end_date))
		s = s.replace("[MEASURE_GENERATING_REGULATION_ROLE]",	str(self.measure_generating_regulation_role))
		s = s.replace("[MEASURE_GENERATING_REGULATION_ID]",		str(self.measure_generating_regulation_id))
		s = s.replace("[GOODS_NOMENCLATURE_ITEM_ID]",			str(self.goods_nomenclature_item_id))
		s = s.replace("[GOODS_NOMENCLATURE_SID]",				str(self.goods_nomenclature_sid))
		s = s.replace("[GEOGRAPHICAL_AREA_ID]",					str(self.geographical_area_id2))
		s = s.replace("[JUSTIFICATION_REGULATION_ROLE]",      f.mstr(self.justification_regulation_role))
		s = s.replace("[JUSTIFICATION_REGULATION_ID]",        f.mstr(self.justification_regulation_id))
		s = s.replace("[STOPPED_FLAG]",                       f.mstr(self.stopped_flag))
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

		self.component_content = ""
		for mc in self.measure_component_list:
			mc.measure_sid = self.measure_sid
			self.component_content += mc.xml()
		self.exclusion_content = ""

		s = s.replace("[COMPONENTS]\n", 			self.component_content)
		s = s.replace("[EXCLUDED]\n", 				self.exclusion_content)

		app.sequence_id += 1
		app.transaction_id += 1
		g.app.last_measure_sid += 1

		return (s)