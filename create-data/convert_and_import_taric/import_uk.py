# Import custom libraries
import sys
import common.globals as g

if (len(sys.argv) > 1):
	filename = sys.argv[1]
else:
	sys.exit()

app = g.app
app.dbase = "tariff_uk"
app.import_xml(filename)
