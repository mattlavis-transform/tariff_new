import re
from unidecode import unidecode
from datetime import datetime

def mstr(x):
	if x is None:
		return ""
	else:
		x = str(x)
		x = x.strip()
		return str(x)

def mnum(x):
	if x is None:
		return ""
	else:
		return int(x)

def mdate(s):
	if s is None or s == "":
		return ""
	else:
		s2 = s.strftime("%Y-%m-%d")
		return s2

def fmtDate(d):
	try:
		d = datetime.strftime(d, '%d/%m/%y')
	except:
		d = ""
	return d

def cleanse(s):
	s = s.replace("&", "and")
	s = s.replace("<", "&lt;")
	s = s.replace(">", "&gt;")
	s = s.replace('"', "&quot;")
	s = s.replace("'", "&apos;")
	s = s.replace("º", "o")
	s = s.replace("‘", "'")
	s = s.replace("’", "'")
	s = s.replace("—", "-")
	s = s.replace("|", "")
	"""
	s = unidecode(str(s))
	"""
	return (s)
