import classes.functions as f
import classes.globals as g
import datetime

class quota_order_number(object):
	def __init__(self, quota_order_number_sid, quota_order_number_id, validity_start_date, validity_end_date):
		self.quota_order_number_sid		= quota_order_number_sid
		self.quota_order_number_id  	= quota_order_number_id
		self.validity_start_date    	= validity_start_date
		self.validity_end_date      	= validity_end_date
		self.active						= False
		self.period_type				= ""
		self.single_definition_in_year	= False
		self.omit						= False

		self.quota_definition_list			= []
		self.quota_future_definition_list	= []
		self.measure_list = []
		self.measure_type_ids = []

	def append_definition(self, q):
		self.quota_definition_list.append(q)
		self.period_type = q.period_type