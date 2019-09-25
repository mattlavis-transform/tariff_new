import classes.functions as f
import classes.globals as g
import datetime
import sys

class quota_association(object):
	def __init__(self, main_quota_definition_sid, sub_quota_definition_sid, relation_type, coefficient, action):

		# from parameters
		self.main_quota_definition_sid  = main_quota_definition_sid
		self.sub_quota_definition_sid   = sub_quota_definition_sid
		self.relation_type              = relation_type
		self.coefficient                = coefficient
		self.action                		= action


	def lookup_matching_sids(self):
		app = g.app
		for item in app.quota_definition_sid_mapping_list:
			if item[0] == self.main_quota_definition_sid:
				self.main_quota_definition_sid = item[1]
			if item[0] == self.sub_quota_definition_sid:
				self.sub_quota_definition_sid = item[1]


	def xml(self):
		app = g.app
		if self.action == "insert":
			s = app.template_quota_association_insert
			self.lookup_matching_sids()
			self.update_type = "3"
		elif self.action == "delete":
			s = app.template_quota_association
			self.update_type = "2"

		s = s.replace("[TRANSACTION_ID]",         		str(app.transaction_id))
		s = s.replace("[MESSAGE_ID]",             		str(app.sequence_id))
		s = s.replace("[RECORD_SEQUENCE_NUMBER]", 		str(app.sequence_id))
		s = s.replace("[UPDATE_TYPE]",					self.update_type)

		s = s.replace("[MAIN_QUOTA_DEFINITION_SID]",  	f.mstr(self.main_quota_definition_sid))
		s = s.replace("[SUB_QUOTA_DEFINITION_SID]",   	f.mstr(self.sub_quota_definition_sid))
		s = s.replace("[RELATION_TYPE]",              	f.mstr(self.relation_type))
		s = s.replace("[COEFFICIENT]",                	f.mstr(self.coefficient))

		s = s.replace("\t\t\t\t\t\t<oub:coefficient></oub:coefficient>\n", "")

		app.sequence_id += 1

		if self.action == "insert":
			app.transaction_id += 1
		return (s)

