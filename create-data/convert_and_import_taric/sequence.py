# Import custom libraries
import sys
import common.globals as g

app = g.app
for n in app.import_file_list:
	print (n)
	if app.DBASE != "tariff_eu":
		n = n.replace("TGB18", "DIT180")
		n = n.replace("TGB19", "DIT190")
	app.import_xml(n, False)
