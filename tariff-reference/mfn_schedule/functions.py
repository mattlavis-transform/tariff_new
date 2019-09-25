# Import standard modules
from __future__ import with_statement
import psycopg2
from contextlib import closing
from zipfile import ZipFile, ZIP_DEFLATED
from datetime import datetime
import os
import sys
import codecs
import re

# Import custom modules
from application import application

app = application()
	
def format_date(d):
	try:
		d = datetime.strftime(d, '%d-%m-%y')
	except:
		d = ""
	return d

def zipdir(archivename):
	BASE_DIR     = os.path.dirname(os.path.realpath(__file__))
	MODEL_DIR = os.path.join(BASE_DIR, "model")
	with closing(ZipFile(archivename, "w", ZIP_DEFLATED)) as z:
		for root, dirs, files in os.walk(MODEL_DIR):
			#NOTE: ignore empty directories
			for fn in files:
				if fn != ".DS_Store":
					absfn = os.path.join(root, fn)
					zfn = absfn[len(MODEL_DIR)+len(os.sep):] #XXX: relative path
					z.write(absfn, zfn)


def mstr(x):
	if x is None:
		return ""
	else:
		return str(x)


def mnum(x):
	if x is None:
		return ""
	else:
		return int(x)


def debug(x):
	if app.debug:
		print (x)


def surround(x):
	if "<w:t>" in x:
		return x
	else:
		return "<w:r><w:t>" + x + "</w:t></w:r>"


def format_seasonal_expression(s):
	s = mstr(s)
	s = s.replace("EUR", "€")
	s = s.replace("DTN G", "/ 100 kg gross")
	s = s.replace("DTN", "/ 100 kg")
	return s


def reduce(s):
	s = s.replace("\r", "; ")
	s = s.replace(",", ";")
	s = s.replace('"', "'")
	s = s.replace("\n", "; ")
	s = s.replace("; ; ", "; ")
	s = s.replace("<w:t>", "")
	s = s.replace("</w:t>", "")
	s = s.replace("-<w:tab/>", "")
	s = s.replace("</w:r>", "")
	s = s.replace("<w:r>", "")
	s = s.replace("<w:t xml:space='preserve'>", "")

	s = s.replace("</w:rPr>", "")
	s = s.replace("<w:iCs/>", "")
	s = s.replace("<w:i/>", "")
	s = s.replace("<w:rPr>", "")
	s = s.replace("<w:b/>", "")
	s = s.replace("  ", " ")
	s = s.replace(" )", ")")
	s = s.replace("<w:br/>", "; ")

	
	return (s)