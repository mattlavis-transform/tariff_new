import sys

def mcleanse(s):
	s = str(s)
	s = s.replace("&", "&amp;")
	s = s.replace("&ampamp;", "&amp;")
	s = s.replace("<", "&lt;")
	s = s.replace(">", "&gt;")
	s = s.replace("&amp;#", "&#")

	return (s)

"""
def mcleanse_safe(s):
	s = str(s)
	s = s.replace("&", "&amp;")
	s = s.replace("&ampamp;", "&amp;")
	s = s.replace("<P>", "|")
	s = s.replace("</P>", "|")
	s = s.replace("<p>", "|")
	s = s.replace("</p>", "|")
	s = s.replace("||", "|")
	s = s.replace("<", "&lt;")
	s = s.replace(">", "&gt;")
	if "|" in s:
		t = s.split("|")
		s = ""
		#s += "<![CDATA["
		for t1 in t:
			t1 = t1.strip()
			if t1 != "":
				#s += "<p>" + t1 + "</p>"
				s += t1 + "\n"
		#s += "]]>"

	#print (s)
	#sys.exit()
	return (s)
"""
def mstr(s):
	if s is None:
		return ""
	else:
		return str(s).strip()

def mdate(s):
	if s is None:
		return ""
	else:
		try:
			s2 = s.strftime("%Y-%m-%d")
		except:
			s2 = s
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


def list_to_sql(my_list):
	s = ""
	if my_list != "":
		for o in my_list:
			s += "'" + o + "', "
		s = s.strip()
		s = s.strip(",")
	return (s)
