import classes.functions as f
import classes.globals as g
import datetime
import sys

class measurement_unit(object):
	def __init__(self, identifier, measurement_unit_code, measurement_unit_qualifier_code):
		self.identifier					        = identifier
		self.measurement_unit_code				= measurement_unit_code
		self.measurement_unit_qualifier_code	= measurement_unit_qualifier_code