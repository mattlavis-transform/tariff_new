import xml.etree.ElementTree as ET
import sys

import classes.functions as f
import classes.globals as g

class business_rules(object):
	def __init__(self, filename):
		app = g.app
		tree = ET.parse(filename)
		root = tree.getroot()
		for node in root.findall('.//oub:measure/../../oub:record', app.namespaces):
			measure_sid					= self.get_value(node, "oub:measure/oub:measure.sid")
			justification_regulation_id	= self.get_value(node, "oub:measure/oub:justification.regulation.id")
			validity_end_date			= self.get_value(node, "oub:measure/oub:validity.end.date")

			if justification_regulation_id == "" and validity_end_date != "":
				self.fail("Measure " + measure_sid + " found with no justification regulation ID and a validity end date")

			if justification_regulation_id != "" and validity_end_date == "":
				self.fail("Measure " + measure_sid + " found with a justification regulation ID and no validity end date")
		app.d("Business rules validated successfully")


	def fail(self, msg):
		print (msg)
		sys.exit()

	def get_value(self, node, s):
		app = g.app
		try:
			sub_node = node.find(s, app.namespaces)
			value = sub_node.text
		except:
			value = ""

		return (value)
