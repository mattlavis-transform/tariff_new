import sys

def mstr(s):
	if s is None:
		return ""
	else:
		return str(s).strip()

def mdate(s):
	if s is None:
		return ""
	else:
		s2 = s.strftime("%Y-%m-%d")
		return s2

def mbool(s):
	if s is None:
		return ""
	else:
		s = str(s).strip()
		if s == "False":
			return "0"
		else:
			return "1"

def mbool2(s):
	if s == 1:
		return True
	else:
		return False

def dq(s):
	print (s)
	sys.exit()

def quit():
	sys.exit()

def cleanse(s):
	s = s.replace ("<", "&lt;")
	s = s.replace (">", "&gt;")
	return (s)

def yes_or_no(question):
	reply = str(input(question+' (y/n): ')).lower().strip()
	if reply[0] == 'y':
		return True
	if reply[0] == 'n':
		return False
	else:
		return yes_or_no("Uhhhh... please enter ")
