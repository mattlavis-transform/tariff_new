import classes.functions as f
import classes.globals as g
import datetime
import sys

class quota_association(object):
	def __init__(self, main_quota_order_number_id, sub_quota_order_number_id, relation_type, coefficient):
		self.main_quota_order_number_id = main_quota_order_number_id
		self.sub_quota_order_number_id  = sub_quota_order_number_id
		self.main_quota_definition_sid  = None
		self.sub_quota_definition_sid   = None
		self.relation_type              = relation_type
		self.coefficient                = coefficient

		"""
		USES THE SCRIPT
		select distinct
		qdm.quota_order_number_id as main_quota_order_number_id, qds.quota_order_number_id as sub_quota_order_number_id,
		/*main_quota_definition_sid, sub_quota_definition_sid, */ relation_type, coefficient
		from quota_associations qa, quota_definitions qdm, quota_definitions qds
		where qa.main_quota_definition_sid = qdm.quota_definition_sid
		and qa.sub_quota_definition_sid = qds.quota_definition_sid
		and qdm.validity_start_date >= '2018-01-01'
		and qds.validity_start_date >= '2018-01-01'
		order by 1, 2
		"""


	def xml(self):
		association_xml = ""
		s = g.app.template_quota_association
		s = s.replace("[TRANSACTION_ID]",         		str(g.app.transaction_id))
		s = s.replace("[MESSAGE_ID]",             		str(g.app.message_id))
		s = s.replace("[RECORD_SEQUENCE_NUMBER]", 		str(g.app.message_id))
		s = s.replace("[UPDATE_TYPE]",					"3")

		s = s.replace("[MAIN_QUOTA_DEFINITION_SID]",    str(self.main_quota_definition_sid))
		s = s.replace("[SUB_QUOTA_DEFINITION_SID]",     str(self.sub_quota_definition_sid))
		s = s.replace("[RELATION_TYPE]",                self.relation_type)
		s = s.replace("[COEFFICIENT]",                  str(self.coefficient))

		g.app.message_id += 1

		association_xml += s + "\n"

		return (association_xml)
