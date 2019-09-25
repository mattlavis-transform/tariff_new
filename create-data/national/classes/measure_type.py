import classes.functions as f
import classes.globals as g
import datetime
import sys

class measure_type(object):
	def __init__(self, measure_type_id, validity_start_date, validity_end_date, trade_movement_code,
			priority_code, measure_component_applicable_code, origin_dest_code, order_number_capture_code,
			measure_explosion_level, measure_type_series_id, description):
		# from parameters
		self.measure_type_id  		            = measure_type_id
		self.validity_start_date                = validity_start_date
		self.validity_end_date                  = validity_end_date
		self.trade_movement_code                = trade_movement_code
		self.priority_code                      = priority_code
		self.measure_component_applicable_code  = measure_component_applicable_code
		self.origin_dest_code                   = origin_dest_code
		self.order_number_capture_code          = order_number_capture_code
		self.measure_explosion_level            = measure_explosion_level
		self.measure_type_series_id             = measure_type_series_id
		self.description                        = description

		self.cleanse()

	def cleanse(self):
		if self.measure_explosion_level > 10:
			self.measure_explosion_level = 10

	def xml(self):
		app = g.app
		if (app.retain == False) and (app.vat_excise == False):
			return ""
		else:
			s = app.template_measure_type
			s = s.replace("[TRANSACTION_ID]",              str(app.transaction_id))
			s = s.replace("[MESSAGE_ID1]",                 str(app.sequence_id))
			s = s.replace("[MESSAGE_ID2]",                 str(app.sequence_id + 1))
			s = s.replace("[RECORD_SEQUENCE_NUMBER1]",     str(app.sequence_id))
			s = s.replace("[RECORD_SEQUENCE_NUMBER2]",     str(app.sequence_id + 1))
			s = s.replace("[UPDATE_TYPE]",                 "3")

			s = s.replace("[MEASURE_TYPE_ID]",            			f.mstr(self.measure_type_id))
			s = s.replace("[VALIDITY_START_DATE]",         			"1971-12-31")
			s = s.replace("[VALIDITY_END_DATE]",            		"")
			s = s.replace("[TRADE_MOVEMENT_CODE]",                  f.mstr(self.trade_movement_code))
			s = s.replace("[PRIORITY_CODE]",                        f.mstr(self.priority_code))
			s = s.replace("[MEASURE_COMPONENT_APPLICABLE_CODE]",    f.mstr(self.measure_component_applicable_code))
			s = s.replace("[ORIGIN_DEST_CODE]",                     f.mstr(self.origin_dest_code))
			s = s.replace("[ORDER_NUMBER_CAPTURE_CODE]",            f.mstr(self.order_number_capture_code))
			s = s.replace("[MEASURE_EXPLOSION_LEVEL]",              f.mstr(self.measure_explosion_level))
			s = s.replace("[MEASURE_TYPE_SERIES_ID]",               f.mstr(self.measure_type_series_id))
			s = s.replace("[DESCRIPTION]",                          f.mstr(self.description))

			s = s.replace("\t\t\t\t\t\t<oub:validity.end.date></oub:validity.end.date>\n", "")
			s = s.replace("\t\t\t\t\t\t<oub:description></oub:description>\n", "")
			app.sequence_id += 2
			app.transaction_id += 1
			return (s)
