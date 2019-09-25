import classes.functions as f
import classes.globals as g
import datetime
import sys

class measure_excluded_geographical_area(object):
	def __init__(self, measure_sid, excluded_geographical_area, geographical_area_sid):
		
		# from parameters
		self.measure_sid				= measure_sid
		self.excluded_geographical_area	= excluded_geographical_area
		self.geographical_area_sid		= geographical_area_sid
		self.action						= ""

	def xml(self):
		app = g.app
		s = app.template_measure_excluded_geographical_area
		s = s.replace("[TRANSACTION_ID]",               str(app.transaction_id))
		s = s.replace("[MESSAGE_ID]",                   str(app.sequence_id))
		s = s.replace("[RECORD_SEQUENCE_NUMBER]",       str(app.sequence_id))

		if self.action == "delete":
			s = s.replace("[UPDATE_TYPE]",                      "2")
		else:
			s = s.replace("[UPDATE_TYPE]",                      "3")

		s = s.replace("[MEASURE_SID]",                  f.mstr(self.measure_sid))
		s = s.replace("[EXCLUDED_GEOGRAPHICAL_AREA]",   f.mstr(self.excluded_geographical_area))
		s = s.replace("[GEOGRAPHICAL_AREA_SID]",        f.mstr(self.geographical_area_sid))

		#print ("here")

		app.sequence_id += 1
		return (s)
