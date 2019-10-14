# Import standard modules
from __future__ import with_statement
import psycopg2
from contextlib import closing
from zipfile import ZipFile, ZIP_DEFLATED
import os
import sys
import codecs
import re
import functions

# Import custom modules
from application import application
from chapter     import chapter

app = functions.app
app.clear()
app.select_document_type()
app.get_full_nomenclature()
app.get_geo_areas_for_excel()
app.get_rates_for_geo_areas()

app.get_categories()
app.get_mfn_rates_for_excel()
if app.document_type == "mfn":
	app.get_preferential_equivalent_for_mfn()
	app.write_mfns_to_excel()
else:
	app.write_full_mfns()
	pass


app.db_disconnect()
app.d("Complete", True)