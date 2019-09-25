import classes.functions as f
import classes.globals as g
import datetime
import sys

class base_regulation(object):
    # Not yet in use - may never be
	def __init__(self, base_regulation_role, base_regulation_id, validity_start_date, validity_end_date, community_code, regulation_group_id, replacement_indicator, stopped_flag, information_text, approved_flag, published_date, officialjournal_number, officialjournal_page, effective_end_date, antidumping_regulation_role, related_antidumping_regulation_id, complete_abrogation_regulation_role, complete_abrogation_regulation_id, explicit_abrogation_regulation_role, explicit_abrogation_regulation_id):
		# from parameters
		self.base_regulation_role					= base_regulation_role
		self.base_regulation_id						= base_regulation_id
		self.validity_start_date					= validity_start_date
		self.validity_end_date						= validity_end_date
		self.community_code							= community_code
		self.regulation_group_id					= regulation_group_id
		self.replacement_indicator					= replacement_indicator
		self.stopped_flag							= stopped_flag
		self.information_text						= information_text
		self.approved_flag							= approved_flag
		self.published_date							= published_date
		self.officialjournal_number					= officialjournal_number
		self.officialjournal_page					= officialjournal_page
		self.effective_end_date						= effective_end_date
		self.antidumping_regulation_role			= antidumping_regulation_role
		self.related_antidumping_regulation_id		= related_antidumping_regulation_id
		self.complete_abrogation_regulation_role	= complete_abrogation_regulation_role
		self.complete_abrogation_regulation_id		= complete_abrogation_regulation_id
		self.explicit_abrogation_regulation_role	= explicit_abrogation_regulation_role
		self.explicit_abrogation_regulation_id		= explicit_abrogation_regulation_id

	def xml(self):
		return ""
		app = g.app

		if app.vat_excise == False:
			if app.retain == False:
				self.base_regulation_id = "I1900040"
				self.regulation_group_id = "MLA"

		s = app.template_base_regulation
		s = s.replace("[TRANSACTION_ID]",                   str(app.transaction_id))
		s = s.replace("[MESSAGE_ID]",                       str(app.sequence_id))
		s = s.replace("[RECORD_SEQUENCE_NUMBER]",           str(app.sequence_id))
		s = s.replace("[UPDATE_TYPE]",                      "3")
		s = s.replace("[BASE_REGULATION_ROLE]",     		f.mstr(self.base_regulation_role))
		s = s.replace("[BASE_REGULATION_ID]",               f.mstr(self.base_regulation_id))
		s = s.replace("[PUBLISHED_DATE]",                   f.mdate(self.published_date))
		s = s.replace("[OFFICIALJOURNAL_NUMBER]",           f.mstr(self.officialjournal_number))
		s = s.replace("[OFFICIALJOURNAL_PAGE]",             f.mstr(self.officialjournal_page))
		s = s.replace("[VALIDITY_START_DATE]",              f.mdate(self.validity_start_date))
		s = s.replace("[VALIDITY_END_DATE]",                f.mdate(self.validity_end_date))
		s = s.replace("[EFFECTIVE_END_DATE]",               f.mdate(self.effective_end_date))
		s = s.replace("[COMMUNITY_CODE]",                   f.mstr(self.community_code))
		s = s.replace("[REGULATION_GROUP_ID]",              f.mstr(self.regulation_group_id))
		s = s.replace("[REPLACEMENT_INDICATOR]",            f.mstr(self.replacement_indicator))
		s = s.replace("[STOPPED_FLAG]",                     f.mbool(self.stopped_flag))
		s = s.replace("[INFORMATION_TEXT]",                 f.mstr(self.information_text))
		s = s.replace("[APPROVED_FLAG]",					f.mbool(self.approved_flag))
	
		s = s.replace("\t\t\t\t\t\t<oub:published.date></oub:published.date>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:officialjournal.number></oub:officialjournal.number>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:officialjournal.page></oub:officialjournal.page>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:validity.end.date></oub:validity.end.date>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:effective.end.date></oub:effective.end.date>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:antidumping.regulation.role></oub:antidumping.regulation.role>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:related.antidumping.regulation.id></oub:related.antidumping.regulation.id>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:complete.abrogation.regulation.role></oub:complete.abrogation.regulation.role>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:complete.abrogation.regulation.id></oub:complete.abrogation.regulation.id>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:explicit.abrogation.regulation.role></oub:explicit.abrogation.regulation.role>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:explicit.abrogation.regulation.id></oub:explicit.abrogation.regulation.id>\n", "")
		s = s.replace("\t\t\t\t\t\t<oub:information.text></oub:information.text>\n", "")
		app.sequence_id += 1
		app.transaction_id += 1
		return (s)
