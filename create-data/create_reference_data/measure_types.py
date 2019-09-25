import psycopg2
import sys
import os
import xmlschema
from xml.sax.saxutils import escape

from openpyxl import Workbook
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Border, Side, Alignment, Protection, Font, NamedStyle

import common.objects as o
from common.measure_type import measure_type
from common.application import application

app = o.app
app.getTemplates()

fname = os.path.join(app.SOURCE_DIR, "measure_types.xlsx")
wb = load_workbook(filename=fname, read_only=True)
ws = wb['Updated']

row_count = ws.max_row
col_count = ws.max_column

for i in range(2, row_count + 1):
	MEASURE_TYPE_ID = ws.cell(row = i, column = 1).value
	DESCRIPTION     = ws.cell(row = i, column = 2).value

	obj = measure_type(MEASURE_TYPE_ID, DESCRIPTION, "", "", "", "", "", "", "", "", "update")
	app.measure_type_list.append(obj)    

ws = wb['New']

row_count = ws.max_row
col_count = ws.max_column

for i in range(2, row_count + 1):
	MEASURE_TYPE_ID                     = ws.cell(row = i, column = 1).value
	DESCRIPTION                         = ws.cell(row = i, column = 2).value
	VALIDITY_START_DATE                 = ws.cell(row = i, column = 3).value
	TRADE_MOVEMENT_CODE                 = ws.cell(row = i, column = 4).value
	PRIORITY_CODE                       = ws.cell(row = i, column = 5).value
	MEASURE_COMPONENT_APPLICABLE_CODE   = ws.cell(row = i, column = 6).value
	ORIGIN_DEST_CODE                    = ws.cell(row = i, column = 7).value
	ORDER_NUMBER_CAPTURE_CODE           = ws.cell(row = i, column = 8).value
	MEASURE_EXPLOSION_LEVEL             = ws.cell(row = i, column = 9).value
	MEASURE_TYPE_SERIES_ID              = ws.cell(row = i, column = 10).value

	obj = measure_type(MEASURE_TYPE_ID, DESCRIPTION, VALIDITY_START_DATE, TRADE_MOVEMENT_CODE, PRIORITY_CODE, MEASURE_COMPONENT_APPLICABLE_CODE, ORIGIN_DEST_CODE, ORDER_NUMBER_CAPTURE_CODE, MEASURE_EXPLOSION_LEVEL, MEASURE_TYPE_SERIES_ID, "insert")
	app.measure_type_list.append(obj)

env = app.envelope_XML
env = env.replace("{ENVELOPE_ID}", str(app.base_envelope_id))
out = ""
for obj in app.measure_type_list:
	obj.writeXML(app)
	out += obj.xml

out = env.replace("{BODY}", out)
filename = os.path.join(app.XML_DIR, "measure_types.xml")
f = open(filename, "w", encoding="utf-8") 
f.write(out)
f.close()

schema_path = os.path.join(app.SCHEMA_DIR, "envelope.xsd")
my_schema = xmlschema.XMLSchema(schema_path)
try:
	if my_schema.is_valid(filename):
		print ("The file validated successfully.")
	else:
		print ("The file did not validate.")
except:
	print ("The file did not validate and crashed the validator.")
app.set_config()


