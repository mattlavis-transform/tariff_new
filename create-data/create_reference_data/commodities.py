import psycopg2
import sys
import os
import xmlschema
from xml.sax.saxutils import escape

from openpyxl import Workbook
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Border, Side, Alignment, Protection, Font, NamedStyle

import common.objects as o
from common.goods_nomenclature import goods_nomenclature
from common.application import application

app = o.app
app.getTemplates()
app.get_current_commodities()
app.write_commodities()
