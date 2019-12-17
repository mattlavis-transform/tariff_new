import psycopg2
import sys
import os
import xmlschema
import csv
from xml.sax.saxutils import escape

from openpyxl import Workbook
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Border, Side, Alignment, Protection, Font, NamedStyle

import common.objects as o
from common.measure import measure
from common.application import application

app = o.app
app.get_templates()
app.get_measure_type_105()
app.get_nomenclature()
app.get_uk_mfns()
app.protect_commodities()
app.create_liberalised_duties()
app.write_measure_count()
app.get_nomenclature_sids()

perform_me32_check = False
if perform_me32_check is True:
    app.perform_me32_checks()

app.write_xml()
app.set_config()
app.validate()
app.write_verification_csv()
