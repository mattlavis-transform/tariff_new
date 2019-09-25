import classes.functions as f
import classes.globals as g

class quota_balance(object):
	def __init__(self, quota_order_number_id, country, method, y1_balance, yx_balance, yx_start):
		self.quota_order_number_id	= quota_order_number_id
		#self.validity_start_date	= validity_start_date
		#self.validity_end_date		= validity_end_date
		self.country				= country
		self.method					= method
		self.y1_balance				= y1_balance
		self.yx_balance				= yx_balance
		self.yx_start				= yx_start
		