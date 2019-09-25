import classes.functions as f
import classes.globals as g
import datetime
import sys

class measure_excluded_geographical_area(object):
	def __init__(self, geographical_area_id_group, goods_nomenclature_item_id, excluded_geographical_area):
		
		# from parameters
		self.geographical_area_id_group	= geographical_area_id_group
		self.goods_nomenclature_item_id	= goods_nomenclature_item_id
		self.excluded_geographical_area	= excluded_geographical_area
		self.geographical_area_sid		= -1
		self.measure_sid				= -1

		# From a parent geo ID, we need the parent SID
		# We need the measure sid from the commodity codes
		# We are fine with the ID for the excluded child

		for geo in g.app.geographical_area_list:
			if geo.geographical_area_id == self.excluded_geographical_area:
			#if geo.geographical_area_id == self.geographical_area_id_group:
				self.geographical_area_sid = geo.geographical_area_sid
				#print (self.geographical_area_sid)
				break


	def xml(self):
		app = g.app
		s = app.template_measure_excluded_geographical_area
		s = s.replace("[TRANSACTION_ID]",               str(app.transaction_id))
		s = s.replace("[MESSAGE_ID]",                   str(app.message_id))
		s = s.replace("[RECORD_SEQUENCE_NUMBER]",       str(app.message_id))
		s = s.replace("[UPDATE_TYPE]",                  "3")
		s = s.replace("[MEASURE_SID]",                  f.mstr(self.measure_sid))
		s = s.replace("[EXCLUDED_GEOGRAPHICAL_AREA]",   f.mstr(self.excluded_geographical_area))
		s = s.replace("[GEOGRAPHICAL_AREA_SID]",        f.mstr(self.geographical_area_sid))

		app.message_id += 1
		return (s)
