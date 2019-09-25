# Import external libraries
import common.globals as g
import sys

files = []
for i in range(1, len(sys.argv)):
	my_arg = sys.argv[i]
	print (my_arg)
	files.append(my_arg)

app = g.app
app.end_date_EU_measures(files)
app.generate_metadata()
app.copy_xml_to_import_folder()