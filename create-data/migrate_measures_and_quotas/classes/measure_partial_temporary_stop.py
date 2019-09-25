import classes.functions as f
import classes.globals as g
import datetime
import sys

class measure_partial_temporary_stop(object):
	def __init__(self, measure_sid, validity_start_date, validity_end_date, partial_temporary_stop_regulation_id,
			partial_temporary_stop_regulation_officialjournal_number, partial_temporary_stop_regulation_officialjournal_page, 
			abrogation_regulation_id, abrogation_regulation_officialjournal_number, abrogation_regulation_officialjournal_page):
		# from parameters
		self.measure_sid  			    		                        = measure_sid
		self.validity_start_date                          			    = validity_start_date
		self.validity_end_date  			    		                = validity_end_date
		self.partial_temporary_stop_regulation_id  			            = partial_temporary_stop_regulation_id
		self.partial_temporary_stop_regulation_officialjournal_number   = partial_temporary_stop_regulation_officialjournal_number
		self.partial_temporary_stop_regulation_officialjournal_page     = partial_temporary_stop_regulation_officialjournal_page
		self.abrogation_regulation_id  			    		            = abrogation_regulation_id
		self.abrogation_regulation_officialjournal_number  			    = abrogation_regulation_officialjournal_number
		self.abrogation_regulation_officialjournal_page  			    = abrogation_regulation_officialjournal_page
		self.action														= ""

	def xml(self):
		app = g.app
		s = app.template_measure_partial_temporary_stop
		s = s.replace("[TRANSACTION_ID]",                   str(app.transaction_id))
		s = s.replace("[MESSAGE_ID]",                       str(app.sequence_id))
		s = s.replace("[RECORD_SEQUENCE_NUMBER]",           str(app.sequence_id))

		if self.action == "delete":
			s = s.replace("[UPDATE_TYPE]",                      "2")
		else:
			s = s.replace("[UPDATE_TYPE]",                      "3")

		s = s.replace("[MEASURE_SID]",                                              f.mstr(self.measure_sid))
		s = s.replace("[VALIDITY_START_DATE]",                                      f.mdate(self.validity_start_date))
		s = s.replace("[VALIDITY_END_DATE]",                                        f.mdate(self.validity_end_date))
		s = s.replace("[PARTIAL_TEMPORARY_STOP_REGULATION_ID]",                     f.mstr(self.partial_temporary_stop_regulation_id))
		s = s.replace("[PARTIAL_TEMPORARY_STOP_REGULATION_OFFICIALJOURNAL_NUMBER]", f.mstr(self.partial_temporary_stop_regulation_officialjournal_number))
		s = s.replace("[PARTIAL_TEMPORARY_STOP_REGULATION_OFFICIALJOURNAL_PAGE]",   f.mstr(self.partial_temporary_stop_regulation_officialjournal_page))
		s = s.replace("[ABROGATION_REGULATION_ID]",                                 f.mstr(self.abrogation_regulation_id))
		s = s.replace("[ABROGATION_REGULATION_OFFICIALJOURNAL_NUMBER]",             f.mstr(self.abrogation_regulation_officialjournal_number))
		s = s.replace("[ABROGATION_REGULATION_OFFICIALJOURNAL_PAGE]",               f.mstr(self.abrogation_regulation_officialjournal_page))

		s = s.replace("\t\t\t\t\t\t<oub:validity.end.date></oub:validity.end.date>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:partial.temporary.stop.regulation.officialjournal.number></oub:partial.temporary.stop.regulation.officialjournal.number>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:partial.temporary.stop.regulation.officialjournal.page></oub:partial.temporary.stop.regulation.officialjournal.page>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:abrogation.regulation.id></oub:abrogation.regulation.id>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:abrogation.regulation.officialjournal.number></oub:abrogation.regulation.officialjournal.number>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:abrogation.regulation.officialjournal.page></oub:abrogation.regulation.officialjournal.page>\n", "")
		app.sequence_id += 1

		return (s)
