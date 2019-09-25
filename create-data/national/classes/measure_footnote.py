import classes.functions as f
import classes.globals as g
import datetime
import sys

class measure_footnote(object):
	def __init__(self, measure_sid, footnote_type_id, footnote_id):
		# from parameters
		self.measure_sid  		= measure_sid
		self.footnote_type_id   = footnote_type_id
		self.footnote_id  		= footnote_id
		self.action				= ""

	def xml(self):
		app = g.app
		s = app.template_footnote_association_measure

		if not(g.app.retain):
			if self.footnote_type_id == '04':
				self.footnote_type_id = 'FM'
				self.footnote_id = '99' + self.footnote_id

		s = s.replace("[TRANSACTION_ID]",           str(app.transaction_id))
		s = s.replace("[MESSAGE_ID]",               str(app.sequence_id))
		s = s.replace("[RECORD_SEQUENCE_NUMBER]",   str(app.sequence_id))

		if self.action == "delete":
			s = s.replace("[UPDATE_TYPE]",                      "2")
		else:
			s = s.replace("[UPDATE_TYPE]",                      "3")

		s = s.replace("[MEASURE_SID]",              f.mstr(self.measure_sid))
		s = s.replace("[FOOTNOTE_TYPE_ID]",         f.mstr(self.footnote_type_id))
		s = s.replace("[FOOTNOTE_ID]",              f.mstr(self.footnote_id))

		app.sequence_id += 1
		return (s)
