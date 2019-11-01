import psycopg2
import sys
import os
import csv
import re
import common.functions as fn
import common.objects as o
from unidecode import unidecode

class goods_nomenclature(object):
	def __init__(self, goods_nomenclature_sid, goods_nomenclature_item_id, productline_suffix, description, force = False):
		self.goods_nomenclature_sid 	= fn.mstr(goods_nomenclature_sid)
		self.goods_nomenclature_item_id = fn.mstr(goods_nomenclature_item_id)
		self.productline_suffix			= fn.mstr(productline_suffix)
		self.description		     	= fn.mstr(description)
		self.force						= force

		self.requires_update = False
		if force == False:
			self.check_change_required()
		else:
			o.app.last_goods_nomenclature_description_period_sid += 1
			self.goods_nomenclature_description_period_sid = o.app.last_goods_nomenclature_description_period_sid

		
	def check_change_required(self):
		self.replace("\xa0", " ")
		self.replace("<p/>", "<br>")
		self.replace(" <br>", "<br>")
		self.replace("<br><br><br><br>", "<br>")
		self.replace("<br><br><br>", "<br>")
		self.replace("<br><br>", "<br>")
		self.replace("acrilyc", "acrylic")
		self.replace("Sumach", "Sumac")
		self.replace("citrusfruit", "citrus fruit")
		self.replace(" anf ", " and ")
		self.replace("licorice", "liquorice")
		self.replace("expessed", "expessed")
		self.replace("capsicin", "capsaicin")
		self.replace("of stach", "of starch")
		self.replace("ize", "ise")
		self.replace("izati", "isati")
		self.replace("([ -])sise", "\\1size")
		self.replace("Sise", "Size")
		self.replace("([Mm])aise", "\\1aize")
		self.replace("([Aa])luminum", "\\1luminium")
		self.replace("([G])elatin ", "\\1elatine ")
		self.replace("morefor", "more for")
		self.replace("propriatory", "proprietary")
		self.replace("less then", "less than")
		self.replace("fiber", "fibre")
		self.replace("glasstransitiontemperature", "glass transition temperature")
		self.replace("Self adhesive", "Self-adhesive")
		self.replace("an other", "another")
		self.replace("substances,of", "substances, of")
		self.replace("f inely", "finely")
		self.replace("Womens'", "Women's")
		self.replace("continous", "continuous")
		self.replace("foilwith", "foil with")
		self.replace("exceding", "exceeding")
		self.replace("not not ", "not ")
		self.replace("accesories", "accessories")
		self.replace("acessories", "accessories")
		self.replace("center ", "centre ")
		self.replace("one ore more", "one or more")
	
		self.replace("liters", "litres")
		self.replace("\|!x!\|", " x ")
		self.replace("\|!x! ", " x ")
		#self.replace("|of the CN", " of the goods classification")
		self.replace(" ([0-9]{1,4}),([0-9]{1,4}) ", " \\1.\\2 ")

		self.replace(" ([0-9]{1,4}),([0-9]{1,4}) ", " \\1.\\2 ")
		self.replace("([0-9]{1,4}),([0-9]{1,4})/", "\\1.\\2/")
		self.replace("([0-9]{1,4}),([0-9]{1,4})\\|", "\\1.\\2|")
		self.replace(" ([0-9]{1,4}),([0-9]{1,4})%", " \\1.\\2%")
		self.replace(" ([0-9]{1,4}),([0-9]{1,4})\\)", " \\1.\\2)")
		self.replace("([0-9]{1,4}),([0-9]{1,4})%", "\\1.\\2%")
		self.replace("([0-9]{1,4}),([0-9]{1,4}) kg", "\\1.\\2 kg")
		self.replace("([0-9]{1,4}),([0-9]{1,4}) Kg", "\\1.\\2 kg")
		self.replace("([0-9]{1,4}),([0-9]{1,4}) C", "\\1.\\2 C")
		self.replace("([0-9]{1,4}),([0-9]{1,4})kg", "\\1.\\2kg")
		self.replace("([0-9]{1,4}),([0-9]{1,4}) g", "\\1.\\2 g")
		self.replace("([0-9]{1,4}),([0-9]{1,4})g", "\\1.\\2g")
		self.replace("([0-9]{1,4}),([0-9]{1,4}) dl", "\\1.\\2 dl")
		self.replace("([0-9]{1,4}),([0-9]{1,4}) m", "\\1.\\2 m")
		self.replace("([0-9]{1,4}),([0-9]{1,4})m", "\\1.\\2m")
		self.replace("([0-9]{1,4}),([0-9]{1,4}) decitex", "\\1.\\2 decitex")
		self.replace("([0-9]{1,4}),([0-9]{1,4}) l", "\\1.\\2 l")
		self.replace("([0-9]{1,4}),([0-9]{1,4}) kW", "\\1.\\2 kW")
		self.replace("([0-9]{1,4}),([0-9]{1,4}) W", "\\1.\\2 W")
		self.replace("([0-9]{1,4}),([0-9]{1,4}) V", "\\1.\\2 V")
		self.replace("([0-9]{1,4}),([0-9]{1,4}) Ah", "\\1.\\2 Ah")
		self.replace("([0-9]{1,4}),([0-9]{1,4}) bar", "\\1.\\2 bar")
		self.replace("([0-9]{1,4}),([0-9]{1,4}) cm", "\\1.\\2 cm")
		self.replace("([0-9]{1,4}),([0-9]{1,4}) mm", "\\1.\\2 mm")
		self.replace("([0-9]{1,4}),([0-9]{1,4}) Nm", "\\1.\\2 Nm")
		self.replace("([0-9]{1,4}),([0-9]{1,4}) kV", "\\1.\\2 kV")
		self.replace("([0-9]{1,4}),([0-9]{1,4}) kHz", "\\1.\\2 kHz")
		self.replace("([0-9]{1,4}),([0-9]{1,4}) kV", "\\1.\\2 kV")
		self.replace("([0-9]{1,4}),([0-9]{1,4}) MHz", "\\1.\\2 MHz")
		self.replace("([0-9]{1,4}),([0-9]{1,4}) μm", "\\1.\\2 μm")
		self.replace("([0-9]{1,4}),([0-9]{1,4}) Ohm", "\\1.\\2 Ohm")
		self.replace("([0-9]{1,4}),([0-9]{1,4}) dB", "\\1.\\2 dB")
		self.replace("([0-9]{1,4}),([0-9]{1,4}) kvar", "\\1.\\2 kvar")
		self.replace("±([0-9]{1,4}),([0-9]{1,4})", "±\\1.\\2")
		self.replace("€ ([0-9]{1,4}),([0-9]{1,4})", "€ \\1.\\2")
		

		if self.requires_update == True:
			o.app.last_goods_nomenclature_description_period_sid += 1
			self.goods_nomenclature_description_period_sid = o.app.last_goods_nomenclature_description_period_sid
			f = open("guru99.txt", "a+")
			f.write(self.goods_nomenclature_item_id + ", " + self.productline_suffix + ", " + self.description + "\n")


	def replace(self, from_string, to_string):
		pos = re.search(from_string, self.description)
		if pos:
			self.description = re.sub(from_string, to_string, self.description, flags = re.MULTILINE)
			self.requires_update = True


	def writeXML(self):
		out = o.app.update_goods_nomenclature_description_XML
		
		self.description = fn.cleanse(self.description)
		out = out.replace("[GOODS_NOMENCLATURE_ITEM_ID]", self.goods_nomenclature_item_id)
		out = out.replace("[PRODUCTLINE_SUFFIX]", self.productline_suffix)
		out = out.replace("[DESCRIPTION]", self.description)
		out = out.replace("[GOODS_NOMENCLATURE_DESCRIPTION_PERIOD_SID]", str(self.goods_nomenclature_description_period_sid))
		out = out.replace("[GOODS_NOMENCLATURE_SID]", str(self.goods_nomenclature_sid))
		out = out.replace("[VALIDITY_START_DATE]", fn.mdate(o.app.critical_date_plus_one))
		out = out.replace("[LANGUAGE_ID]", "EN")
		out = out.replace("[TRANSACTION_ID]", str(o.app.transaction_id))
		out = out.replace("[MESSAGE_ID1]", str(o.app.message_id))
		out = out.replace("[MESSAGE_ID2]", str(o.app.message_id + 1))
		out = out.replace("[MESSAGE_ID3]", str(o.app.message_id + 2))
		out = out.replace("[RECORD_SEQUENCE_NUMBER1]", str(o.app.message_id))
		out = out.replace("[RECORD_SEQUENCE_NUMBER2]", str(o.app.message_id + 1))
		out = out.replace("[RECORD_SEQUENCE_NUMBER3]", str(o.app.message_id + 2))

		self.xml = out

		o.app.transaction_id += 1
		o.app.message_id += 2
		