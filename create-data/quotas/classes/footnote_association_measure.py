import classes.functions as f
import classes.globals as g
import datetime
import sys

class footnote_association_measure(object):
	def __init__(self, measure_sid, footnote_type_id, footnote_id):
		self.measure_sid        = measure_sid
		self.footnote_type_id  	= footnote_type_id
		self.footnote_id        = footnote_id


	def xml(self):
		# Get duty amounts for special cases
		s = g.app.template_footnote_association_measure
		s = s.replace("[TRANSACTION_ID]",           str(g.app.transaction_id))
		s = s.replace("[MESSAGE_ID]",               str(g.app.message_id))
		s = s.replace("[RECORD_SEQUENCE_NUMBER]",   str(g.app.message_id))
		s = s.replace("[UPDATE_TYPE]",              "3")
		s = s.replace("[MEASURE_SID]",              f.mstr(self.measure_sid))
		
		s = s.replace("[FOOTNOTE_TYPE_ID]",         self.footnote_type_id)
		s = s.replace("[FOOTNOTE_ID]",              self.footnote_id)

		g.app.message_id += 1
		return (s)
