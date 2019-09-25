import psycopg2
import sys
import os, re
from os import system, name 
import csv
import json
import codecs
from datetime import datetime

import functions as f
from commodity import commodity
from commodity import davidowen_commodity

from duty import duty
from progressbar import ProgressBar
from hierarchy import hierarchy
from hierarchy import subheading_hierarchy

class application(object):
	def __init__(self):
		self.clear()
		self.siv_list               = []
		self.mfn_component_list		= []
		self.meursing_list			= []
		self.vessels_list           = []
		self.civilair_list			= []
		self.airworthiness_list		= []
		self.aircraft_list			= []
		self.pharmaceuticals_list	= []
		self.ita_list 				= []
		self.generalrelief_list		= []
		self.authoriseduse_list		= []
		self.seasonal_list			= []
		self.special_list			= []
		self.section_chapter_list	= []
		self.lstFootnotes			= []
		self.lstFootnotesUnique		= []
		self.debug					= False
		self.suppress_duties		= False
		self.country_codes			= ""
		self.siv_data_list			= []
		self.seasonal_fta_duties	= []
		self.suspensions_xml		= ""
		self.david_owen_list		= []
		
		self.partial_temporary_stops	= []

		self.BASE_DIR			= os.path.dirname(os.path.abspath(__file__))
		self.SOURCE_DIR			= os.path.join(self.BASE_DIR, "source")
		self.OUTPUT_DIR			= os.path.join(self.BASE_DIR, "output")
		self.COMPONENT_DIR		= os.path.join(self.BASE_DIR, "xmlcomponents")
		self.CONFIG_DIR			= os.path.join(self.BASE_DIR, "..")
		self.CONFIG_DIR			= os.path.join(self.CONFIG_DIR, "..")
		self.CONFIG_DIR			= os.path.join(self.CONFIG_DIR, "create-data")
		self.CONFIG_DIR			= os.path.join(self.CONFIG_DIR, "config")
		self.CONFIG_FILE		= os.path.join(self.CONFIG_DIR, "config_common.json")
		self.CONFIG_FILE_LOCAL	= os.path.join(self.CONFIG_DIR, "config_migrate_measures_and_quotas.json")

		self.BALANCE_DIR		= os.path.join(self.BASE_DIR, "..")
		self.BALANCE_DIR		= os.path.join(self.BALANCE_DIR, "..")
		self.BALANCE_DIR		= os.path.join(self.BALANCE_DIR, "create-data")
		self.BALANCE_DIR		= os.path.join(self.BALANCE_DIR, "migrate_measures_and_quotas")
		self.BALANCE_DIR		= os.path.join(self.BALANCE_DIR, "source")
		self.BALANCE_DIR		= os.path.join(self.BALANCE_DIR, "quotas")
		self.BALANCE_FILE		= os.path.join(self.BALANCE_DIR, "quota_volume_master.csv")

		arg1 = ""
		self.include_do_only = False
		try:
			arg1 = sys.argv[1].strip()
		except:
			arg1 = ""
		if arg1 != "":
			self.include_do_only = True
		

		self.get_config()
		self.readTemplates()

	def clear(self):
		# for windows
		if name == 'nt':
			_ = system('cls')
		# for mac and linux(here, os.name is 'posix')
		else:
			#_ = system('clear')
			_ = system("printf '\33c\e[3J'")

	def get_config(self):
		# Get global config items
		with open(self.CONFIG_FILE, 'r') as f:
			my_dict = json.load(f)

		self.DBASE			= my_dict['dbase']
		self.p				= my_dict['p']
		#print (self.DBASE)
		#sys.exit()

		# Get local config items
		with open(self.CONFIG_FILE_LOCAL, 'r') as f:
			my_dict = json.load(f)

		self.all_country_profiles = my_dict['country_profiles']

		# Connect to the database
		self.connect()

	def get_country_list(self):
		try:
			self.country_codes = self.all_country_profiles[self.country_profile]["country_codes"]
		except:
			print ("Country profile does not exist")
			sys.exit()
		self.agreement_name			= self.all_country_profiles[self.country_profile]["agreement_name"]

		self.agreement_date_short	= self.all_country_profiles[self.country_profile]["agreement_date"]
		temp = datetime.strptime(self.agreement_date_short, "%d/%m/%Y")
		self.agreement_date_long	= datetime.strftime(temp, "%d %B %Y")

		self.table_per_country		= self.all_country_profiles[self.country_profile]["table_per_country"]
		self.version				= self.all_country_profiles[self.country_profile]["version"]
		self.country_name			= self.all_country_profiles[self.country_profile]["country_name"]

	def connect(self):
		self.conn = psycopg2.connect("dbname=" + self.DBASE + " user=postgres password=" + self.p)

	def shutDown(self):
		self.conn.close()

	def get_chapter_description(self, chapter_string):
		###############################################################
		# Get the chapter description
		# Relevant to both the classification and the schedule
		sql = "SELECT description FROM ml.chapters WHERE chapter = '" + chapter_string + "'"
		cur = self.conn.cursor()
		cur.execute(sql)
		row = cur.fetchone()
		try:
			chapter_description = row[0]
			chapter_description = chapter_description.replace(" Of ", " of ")
			chapter_description = chapter_description.replace(" Or ", " or ")
		except:
			chapter_description = ""

		return chapter_description

	def getSectionsChapters(self):
		sql = """
		SELECT LEFT(gn.goods_nomenclature_item_id, 2) as chapter, cs.section_id
		FROM chapters_sections cs, goods_nomenclatures gn
		WHERE cs.goods_nomenclature_sid = gn.goods_nomenclature_sid AND gn.producline_suffix = '80'
		ORDER BY 1
		"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows_sections_chapters = cur.fetchall()
		self.section_chapter_list = []
		for rd in rows_sections_chapters:
			sChapter = rd[0]
			iSection = rd[1]
			self.section_chapter_list.append([sChapter, iSection, False])
			
 		# The last parameter is "1" if the chapter equates to a new section
		iLastSection = -1
		for r in self.section_chapter_list:
			iSection = r[1]
			if iSection != iLastSection:
				r[2] = True
			iLastSection = iSection

	def readTemplates(self):
		self.COMPONENT_DIR = os.path.join(self.COMPONENT_DIR, "")

		file = open(os.path.join(self.COMPONENT_DIR, "document.xml"), "r")
		self.sDocumentXML = file.read()

		file = open(os.path.join(self.COMPONENT_DIR, "tablerow.xml"), "r")
		self.sTableRowXML = file.read()

		file = open(os.path.join(self.COMPONENT_DIR, "tier1.xml"), "r")
		self.sTier1XML = file.read()

		file = open(os.path.join(self.COMPONENT_DIR, "tier2.xml"), "r")
		self.sTier2XML = file.read()

		file = open(os.path.join(self.COMPONENT_DIR, "tier3.xml"), "r")
		self.sTier3XML = file.read()

		file = open(os.path.join(self.COMPONENT_DIR, "tier4.xml"), "r")
		self.sTier4XML = file.read()

		file = open(os.path.join(self.COMPONENT_DIR, "tier5.xml"), "r")
		self.sTier5XML = file.read()

		file = open(os.path.join(self.COMPONENT_DIR, "tier6.xml"), "r")
		self.sTier6XML = file.read()

		file = open(os.path.join(self.COMPONENT_DIR, "tier7.xml"), "r")
		self.sTier7XML = file.read()

		file = open(os.path.join(self.COMPONENT_DIR, "tier8.xml"), "r")
		self.sTier8XML = file.read()

		file = open(os.path.join(self.COMPONENT_DIR, "tier1bullet.xml"), "r")
		self.sTier1BulletXML = file.read()

		file = open(os.path.join(self.COMPONENT_DIR, "tier2bullet.xml"), "r")
		self.sTier2BulletXML = file.read()

		file = open(os.path.join(self.COMPONENT_DIR, "tier3bullet.xml"), "r")
		self.sTier3BulletXML = file.read()

		file = open(os.path.join(self.COMPONENT_DIR, "tier4bullet.xml"), "r")
		self.sTier4BulletXML = file.read()

		file = open(os.path.join(self.COMPONENT_DIR, "tier5bullet.xml"), "r")
		self.sTier5BulletXML = file.read()

		file = open(os.path.join(self.COMPONENT_DIR, "tier6bullet.xml"), "r")
		self.sTier6BulletXML = file.read()

		file = open(os.path.join(self.COMPONENT_DIR, "tier7bullet.xml"), "r")
		self.sTier7BulletXML = file.read()

		file = open(os.path.join(self.COMPONENT_DIR, "tier8bullet.xml"), "r")
		self.sTier8BulletXML = file.read()

		#bullet = ">" # chr(149)
		bullet = chr(149)
		self.sTier1BulletXML = self.sTier1BulletXML.replace("#", bullet)
		self.sTier2BulletXML = self.sTier2BulletXML.replace("#", bullet)
		self.sTier3BulletXML = self.sTier3BulletXML.replace("#", bullet)
		self.sTier4BulletXML = self.sTier4BulletXML.replace("#", bullet)
		self.sTier5BulletXML = self.sTier5BulletXML.replace("#", bullet)
		self.sTier6BulletXML = self.sTier6BulletXML.replace("#", bullet)
		self.sTier7BulletXML = self.sTier7BulletXML.replace("#", bullet)
		self.sTier8BulletXML = self.sTier8BulletXML.replace("#", bullet)


		# New for authorised use
		file = open(os.path.join(self.COMPONENT_DIR, "starttable.xml"), "r")
		self.sStartTableXML = file.read()

		file = open(os.path.join(self.COMPONENT_DIR, "endtable.xml"), "r")
		self.sEndTableXML = file.read()

		file = open(os.path.join(self.COMPONENT_DIR, "heading1.xml"), "r")
		self.sHeading1XML = file.read()

		file = open(os.path.join(self.COMPONENT_DIR, "heading2.xml"), "r")
		self.sHeading2XML = file.read()



	def getSivs(self):
		# Be aware that this is deliberately using old data, meaning that 
		# in reality, the duty charged will be zero
		sql = """SELECT DISTINCT m.goods_nomenclature_item_id
		FROM measures m, measure_conditions mc
		WHERE m.measure_sid = mc.measure_sid
		AND m.validity_start_date > '2018-01-01'
		AND mc.condition_code = 'V'
		ORDER BY m.goods_nomenclature_item_id
		"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		for r in rows:
			self.siv_list.append(r[0])
		
		self.siv_list.append("0707000510")
		self.siv_list.append("0707000520")
			
	def getVessels(self):
		sql = """SELECT DISTINCT goods_nomenclature_item_id FROM ml.v5_brexit_day m WHERE measure_type_id = '117';"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		for r in rows:
			self.vessels_list.append(r[0])

	def getCivilAir(self):
		sql = """SELECT DISTINCT gn.goods_nomenclature_item_id
		FROM goods_nomenclature_descriptions gnd, goods_nomenclatures gn
		WHERE gn.goods_nomenclature_item_id = gnd.goods_nomenclature_item_id
		AND (LOWER(description) LIKE '%civil air%' OR LOWER(description) LIKE '%civil use%')
		AND gn.validity_start_date <= '2019/01/01'
		AND (gn.validity_end_date >= '2019/03/29' OR gn.validity_end_date IS NULL) ORDER BY 1"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		for r in rows:
			self.civilair_list.append(r[0])

		"""
		filename = os.path.join(self.SOURCE_DIR, "civilair_commodities.csv")
		with open(filename, "r") as f:
			reader = csv.reader(f)
			temp = list(reader)
		for i in temp:
			self.civilair_list.append(i[0])
		"""

	def getAirworthiness(self):
		sql = """SELECT DISTINCT goods_nomenclature_item_id FROM ml.v5_brexit_day m WHERE measure_type_id = '119';"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		for r in rows:
			self.airworthiness_list.append(r[0])

	def getAircraft(self):
		sql = """SELECT DISTINCT goods_nomenclature_item_id FROM ml.v5_brexit_day m WHERE measure_type_id = '115' AND regulation_id = 'R953050' ORDER BY 1;"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		for r in rows:
			self.aircraft_list.append(r[0])

	def getPharmaceuticals(self):
		sql = """SELECT DISTINCT goods_nomenclature_item_id FROM ml.v5_brexit_day m WHERE additional_code_type_id = '2' AND additional_code_id = '500' ORDER BY 1;"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		for r in rows:
			self.pharmaceuticals_list.append(r[0])

	def getITAProducts(self):
		filename = os.path.join(self.SOURCE_DIR, "ita_commodities.csv")
		with open(filename, "r") as f:
			reader = csv.reader(f)
			temp = list(reader)
		for i in temp:
			self.ita_list.append(i[0])

	def getGeneralRelief(self):
		filename = os.path.join(self.SOURCE_DIR, "generalrelief_commodities.csv")
		with open(filename, "r") as f:
			reader = csv.reader(f)
			temp = list(reader)
		for i in temp:
			self.generalrelief_list.append(i[0])

	def getAuthorisedUse(self):
		sql = """SELECT DISTINCT goods_nomenclature_item_id FROM ml.v5_2019 m WHERE measure_type_id = '105';"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		for r in rows:
			self.authoriseduse_list.append(r[0])
		
		# Also add in cucumbers: the data cannot find these, therefore manually added
		self.authoriseduse_list.append("0707000510")
		self.authoriseduse_list.append("0707000520")
		
	def getSpecials(self):
		filename = os.path.join(self.SOURCE_DIR, "special_notes.csv")
		with open(filename, "r") as f:
			reader = csv.reader(f)
			temp = list(reader)
		for row in temp:
			commodity_code	= row[0]
			note			= row[1]
			oSpecial = special(commodity_code, note)

			self.special_list.append(oSpecial)

	def getSeasonal(self):
		filename = os.path.join(self.SOURCE_DIR, "seasonal_commodities.csv")
		with open(filename, "r") as f:
			reader = csv.reader(f)
			temp = list(reader)
		for row in temp:
			commodity_code		= row[0]
			season1_start		= row[1]
			season1_end			= row[2]
			season1_expression	= row[3]
			season2_start		= row[4]
			season2_end			= row[5]
			season2_expression	= row[6]
			season3_start		= row[7]
			season3_end			= row[8]
			season3_expression	= row[9]
			oSeasonal = seasonal(commodity_code, season1_start, season1_end, season1_expression, season2_start, season2_end, season2_expression, season3_start, season3_end, season3_expression)

			self.seasonal_list.append(oSeasonal)

	def getSIVProducts(self):
		# The SQL below gets all products that have a V condition, therefore entry price system against them
		"""SELECT DISTINCT goods_nomenclature_item_id FROM measures m, measure_conditions mc
		WHERE m.measure_sid = mc.measure_sid
		AND mc.condition_code = 'V'
		AND m.validity_start_date >= '2018-01-01' AND (m.validity_end_date <= '2020-01-01' OR m.validity_end_date IS NULL)
		ORDER BY 1"""
		filename = os.path.join(self.SOURCE_DIR, "siv_products.csv")
		with open(filename, "r") as f:
			reader = csv.reader(f)
			temp = list(reader)
		for i in temp:
			self.siv_list.append(i[0])

	def getCountryAdValoremForSIV(self):
		sql = """SELECT m.goods_nomenclature_item_id, mcc.duty_amount, mcc.duty_expression_id, m.validity_start_date, m.validity_end_date
		FROM measures m, measure_conditions mc, measure_condition_components mcc
		WHERE m.measure_sid = mc.measure_sid
		AND mcc.measure_condition_sid = mc.measure_condition_sid
		AND m.geographical_area_id IN (""" + self.geo_ids + """)
		AND mc.condition_code = 'V'
		AND mcc.duty_expression_id = '01'
		AND validity_start_date <= CURRENT_DATE
		AND (validity_end_date >= CURRENT_DATE OR validity_end_date IS NULL)
		ORDER BY validity_start_date DESC"""
		#print (sql)
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		self.partial_temporary_stops = []
		for rw in rows:
			goods_nomenclature_item_id	= rw[0]
			duty_amount					= rw[1]
			duty_expression_id			= rw[2]
			validity_start_date			= rw[3]
			validity_end_date			= rw[4]

			siv = siv_data(goods_nomenclature_item_id, duty_amount, duty_expression_id, validity_start_date, validity_end_date)
			self.siv_data_list.append (siv)

		# Ensure that there is only one (the latest record listed here; delete all the others)
		unique_siv_data_list_commodities_only	= []
		unique_siv_data_list					= []
		for item in self.siv_data_list:
			if item.goods_nomenclature_item_id not in unique_siv_data_list_commodities_only:
				unique_siv_data_list.append (item)

			unique_siv_data_list_commodities_only.append (item.goods_nomenclature_item_id)

		self.siv_data_list = unique_siv_data_list
		

	def get_authorised_use_commodities(self):
		sql = """
		-- Ships (2357 commodities); this is the whole of 117
		SELECT DISTINCT measure_sid, goods_nomenclature_item_id, 'ships' as measure_type FROM ml.v5 m WHERE measure_type_id = '117'

		UNION

		-- Non preferential duty under end-use (479 commodities) - this is the whole of 105
		SELECT DISTINCT measure_sid, goods_nomenclature_item_id, 'non-pref' as measure_type
		FROM ml.v5 m
		WHERE measure_type_id = '105' AND (m.additional_code_id IS NULL OR  m.additional_code_id = '550')

		UNION

		-- Aircraft (0 commodies: 732 rows)
		SELECT DISTINCT m.measure_sid, m.goods_nomenclature_item_id, 'certainair' as measure_type
		FROM goods_nomenclature_descriptions gnd, goods_nomenclatures gn, ml.v5 m
		WHERE gn.goods_nomenclature_item_id = m.goods_nomenclature_item_id
		AND gn.goods_nomenclature_item_id = gnd.goods_nomenclature_item_id
		AND (LOWER(description) LIKE '%aircraft%' OR LOWER(description) LIKE '%aircraft%')
		AND m.measure_type_id = '115'
		ORDER BY 2
		LIMIT 10000
		"""

		measure_list_string = ""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		self.commodity_list = []
		for row in rows:
			measure_sid					= row[0]
			goods_nomenclature_item_id	= row[1]
			measure_type				= row[2]
			if 2 > 1:
			#if goods_nomenclature_item_id[0:2] == "94":
				validity_start_date			= ""
				my_commodity = commodity(measure_sid, goods_nomenclature_item_id, validity_start_date, measure_type)
				self.commodity_list.append(my_commodity)

				measure_list_string			+= "'" + str(measure_sid) + "', "

		measure_list_string = measure_list_string.strip()
		measure_list_string = measure_list_string.strip(",")

		#print (measure_list_string)

		sql = """
		SELECT mc.measure_sid, mc.duty_expression_id, mc.duty_amount, mc.monetary_unit_code, measurement_unit_code,
		measurement_unit_qualifier_code, m.goods_nomenclature_item_id
		FROM measure_components mc, ml.v5 m
		WHERE m.measure_sid = mc.measure_sid
		AND m.measure_sid IN (""" + measure_list_string + """)
		ORDER BY m.goods_nomenclature_item_id, duty_expression_id
		"""

		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()

		self.duty_list = []
		for row in rows:
			measure_sid						= row[0]
			duty_expression_id				= row[1]
			duty_amount						= row[2]
			monetary_unit_code				= row[3]
			measurement_unit_code			= row[4]
			measurement_unit_qualifier_code	= row[5]
			goods_nomenclature_item_id		= row[6]

			my_duty = duty(goods_nomenclature_item_id, "", "", "", duty_expression_id, duty_amount, monetary_unit_code, measurement_unit_code, measurement_unit_qualifier_code, measure_sid)
			self.duty_list.append(my_duty)


		# Put the MFN commodities into a set
		mfn_list = []
		for mfn in self.mfn_component_list:
			mfn_list.append(mfn.commodity_code)

		mfn_set = set(mfn_list)
		#print (len(mfn_set))
		#sys.exit()

		for my_commodity in self.commodity_list:
			if my_commodity.goods_nomenclature_item_id in mfn_set:
				print (my_commodity.goods_nomenclature_item_id, "non-zero")
				for my_duty in self.duty_list:
					if my_duty.measure_sid == my_commodity.measure_sid:
						my_commodity.duty_list.append(my_duty)
						#break
			else:
				#print ("zero")
				my_commodity.append_zero_duty()

		# Now add in the military set
		filename = os.path.join(self.SOURCE_DIR, "military_list.csv")
		with open(filename, "r") as f:
			reader = csv.reader(f)
			rows = list(reader)
		
		for row in rows:
			goods_nomenclature_item_id	= row[0]
			measure_sid					= int(goods_nomenclature_item_id)
			validity_start_date			= ""
			measure_type				= "military"
			my_commodity = commodity(measure_sid, goods_nomenclature_item_id, validity_start_date, measure_type)

			duty_expression_id				= "01"
			duty_amount						= 0
			monetary_unit_code				= ""
			measurement_unit_code			= ""
			measurement_unit_qualifier_code	= ""

			my_duty = duty(goods_nomenclature_item_id, "", "", "", duty_expression_id, duty_amount, monetary_unit_code, measurement_unit_code, measurement_unit_qualifier_code, measure_sid)
			my_commodity.duty_list.append(my_duty)

			self.commodity_list.append(my_commodity)

		# Now the military list is added, the whole list needs to be resorted
		self.commodity_list.sort(key=lambda x: x.goods_nomenclature_item_id, reverse = False)


		print ("Combining duties")
		for my_commodity in self.commodity_list:
			my_commodity.combineDuties()

		subheading_list = []
		for my_commodity in self.commodity_list:
			subheading_list.append(my_commodity.subheading)
		
		print("Getting subheading hierarchies")
		self.list_of_hierarchies = []
		my_set = set(subheading_list)
		p = ProgressBar(len(my_set), sys.stdout)
		cnt = 1
		for subheading in my_set:
			obj_hierarchy = self.get_hierarchy(subheading)

			p.print_progress(cnt)
			cnt +=1
			obj_subheading_hierarchy = subheading_hierarchy(subheading, obj_hierarchy)
			self.list_of_hierarchies.append(obj_subheading_hierarchy)

		self.list_of_hierarchies.sort(key=lambda x: x.subheading, reverse = False)

		print("\n\nWriting XML for Word document")
		p = ProgressBar(len(self.commodity_list), sys.stdout)
		cnt = 1

		# We need to form a new table wherever the chapter changes
		# The overarching document is formed of
		#	- a single document.xml, which has a placeholder entitles [TABLES]
		#	- into which the multiple chapter tables are inserted
		# 	- a new chapter is created whenever the commodity's chapter property changes
		#	- at which point a heading 1 object, followed by a start table object, then the full table, and eventually an end table object are inserted

		self.table_xml = ""
		last_chapter = "-1"

		last_goods_nomenclature_item_id = "dummy"
		last_combined_duty				= "dummy"
		last_context					= "dummy"

		for my_commodity in self.commodity_list:
			if my_commodity.include:
				my_context = my_commodity.goods_nomenclature_item_id + my_commodity.combined_duty
				if my_context != "a dummy string": # last_context:
					my_commodity.get_hierarchy()
					row_xml = ""
					if my_commodity.chapter != last_chapter:
						if last_chapter != "-1":
							row_xml += self.sEndTableXML

						heading1_xml = self.sHeading1XML
						chapter_description = "Chapter " + str(int(my_commodity.chapter)) + " : " +  self.get_chapter_description(my_commodity.chapter)
						heading1_xml = heading1_xml.replace("[HEADING]", chapter_description)
						row_xml += heading1_xml
						row_xml += self.sStartTableXML

					row = self.sTableRowXML
					row = row.replace("[COMMODITY_CODE]", 	my_commodity.goods_nomenclature_item_id)
					row = row.replace("[DESCRIPTION]", 		my_commodity.hierarchy)
					row = row.replace("[DUTY_EXPRESSION]",	my_commodity.combined_duty)
					row_xml += row
					self.table_xml += row_xml
					last_chapter = my_commodity.chapter

			last_goods_nomenclature_item_id = my_commodity.goods_nomenclature_item_id
			last_combined_duty				= my_commodity.combined_duty
			last_context = last_goods_nomenclature_item_id + last_combined_duty

			p.print_progress(cnt)
			cnt +=1

		self.table_xml += self.sEndTableXML
		self.cleanse_xml()
		
		self.suspensions_xml = self.sDocumentXML
		self.suspensions_xml = self.suspensions_xml.replace("[TABLES]", self.table_xml)

	def get_hierarchy(self, subheading):
		# This gets an entire section of the catalogue: it is not a hierarchy in itself
		sql = """
		SELECT goods_nomenclature_item_id, producline_suffix, number_indents, description
		FROM ml.goods_nomenclature_export3('""" + subheading + """%')
		ORDER BY goods_nomenclature_item_id, producline_suffix
		"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		hierarchy_list = []

		for row in rows:
			goods_nomenclature_item_id	= row[0]
			producline_suffix			= row[1]
			number_indents				= row[2]
			description					= row[3]
			my_hierarchy = hierarchy(goods_nomenclature_item_id, producline_suffix, number_indents, description)
			hierarchy_list.append(my_hierarchy)
		return hierarchy_list

	def cleanse_xml(self):
		self.table_xml = re.sub("<w:t>(.*)m2</w:t>", "<w:t>\g<1>m</w:t></w:r><w:r><w:rPr><w:vertAlign w:val=\"superscript\"/></w:rPr><w:t>2</w:t>", self.table_xml, flags=re.MULTILINE)
		self.table_xml = re.sub("<w:t>(.*)m3</w:t>", "<w:t>\g<1>m</w:t></w:r><w:r><w:rPr><w:vertAlign w:val=\"superscript\"/></w:rPr><w:t>3</w:t>", self.table_xml, flags=re.MULTILINE)
		self.table_xml = re.sub("<w:t>(.*)K2O</w:t>", "<w:t>\g<1>K</w:t></w:r><w:r><w:rPr><w:vertAlign w:val=\"subscript\"/></w:rPr><w:t>2</w:t></w:r><w:r><w:t>O</w:t>", self.table_xml, flags=re.MULTILINE)
		self.table_xml = re.sub("<w:t>(.*)H2O2</w:t>", "<w:t>\g<1>H</w:t></w:r><w:r><w:rPr><w:vertAlign w:val=\"subscript\"/></w:rPr><w:t>2</w:t></w:r><w:r><w:t>O</w:t></w:r><w:r><w:rPr><w:vertAlign w:val=\"subscript\"/></w:rPr><w:t>2</w:t>", self.table_xml, flags=re.MULTILINE)
		self.table_xml = re.sub("<w:t>(.*)P2O5</w:t>", "<w:t>\g<1>P</w:t></w:r><w:r><w:rPr><w:vertAlign w:val=\"subscript\"/></w:rPr><w:t>2</w:t></w:r><w:r><w:t>O</w:t></w:r><w:r><w:rPr><w:vertAlign w:val=\"subscript\"/></w:rPr><w:t>5</w:t>", self.table_xml, flags=re.MULTILINE)
		
		# Subscripts
		self.table_xml = re.sub("@(.)", '</w:t></w:r><w:r><w:rPr><w:vertAlign w:val="subscript"/></w:rPr><w:t>\\1</w:t></w:r><w:r><w:t>', self.table_xml, flags=re.MULTILINE)
		self.table_xml = re.sub("\$(.)", '</w:t></w:r><w:r><w:rPr><w:vertAlign w:val="superscript"/></w:rPr><w:t>\\1 </w:t></w:r><w:r><w:t>', self.table_xml, flags=re.MULTILINE)

		# Missing commas
		self.table_xml = re.sub("([0-9]),([0-9]) kg", "\\1.\\2 kg", self.table_xml, flags=re.MULTILINE)
		self.table_xml = re.sub("([0-9]),([0-9]) Kg", "\\1.\\2 kg", self.table_xml, flags=re.MULTILINE)
		self.table_xml = re.sub("([0-9]),([0-9]) C", "\\1.\\2 C", self.table_xml, flags=re.MULTILINE)
		self.table_xml = re.sub("([0-9]),([0-9])kg", "\\1.\\2kg", self.table_xml, flags=re.MULTILINE)
		self.table_xml = re.sub("([0-9]),([0-9]{1,3}) g", "\\1.\\2 g", self.table_xml, flags=re.MULTILINE)
		self.table_xml = re.sub("([0-9]),([0-9]{1,3})g", "\\1.\\2g", self.table_xml, flags=re.MULTILINE)
		self.table_xml = re.sub("([0-9]),([0-9]{1,3}) dl", "\\1.\\2 dl", self.table_xml, flags=re.MULTILINE)
		self.table_xml = re.sub("([0-9]),([0-9]{1,3}) m", "\\1.\\2 m", self.table_xml, flags=re.MULTILINE)
		self.table_xml = re.sub("([0-9]),([0-9]{1,3})m", "\\1.\\2m", self.table_xml, flags=re.MULTILINE)
		self.table_xml = re.sub("([0-9]),([0-9]{1,3}) decitex", "\\1.\\2 decitex", self.table_xml, flags=re.MULTILINE)
		self.table_xml = re.sub("([0-9]),([0-9]{1,3}) l", "\\1.\\2 l", self.table_xml, flags=re.MULTILINE)
		self.table_xml = re.sub("([0-9]),([0-9]{1,3}) kW", "\\1.\\2 kW", self.table_xml, flags=re.MULTILINE)
		self.table_xml = re.sub("([0-9]),([0-9]{1,3}) W", "\\1.\\2 W", self.table_xml, flags=re.MULTILINE)
		self.table_xml = re.sub("([0-9]),([0-9]{1,3}) V", "\\1.\\2 V", self.table_xml, flags=re.MULTILINE)
		self.table_xml = re.sub("([0-9]),([0-9]{1,3}) Ah", "\\1.\\2 Ah", self.table_xml, flags=re.MULTILINE)
		self.table_xml = re.sub("([0-9]),([0-9]{1,3}) bar", "\\1.\\2 bar", self.table_xml, flags=re.MULTILINE)
		self.table_xml = re.sub("([0-9]),([0-9]{1,3}) cm", "\\1.\\2 cm", self.table_xml, flags=re.MULTILINE)
		self.table_xml = re.sub("([0-9]),([0-9]{1,3}) Nm", "\\1.\\2 Nm", self.table_xml, flags=re.MULTILINE)
		self.table_xml = re.sub("([0-9]),([0-9]{1,3}) kV", "\\1.\\2 kV", self.table_xml, flags=re.MULTILINE)
		self.table_xml = re.sub("([0-9]),([0-9]{1,3}) kHz", "\\1.\\2 kHz", self.table_xml, flags=re.MULTILINE)
		self.table_xml = re.sub("([0-9]),([0-9]{1,3}) kV", "\\1.\\2 kV", self.table_xml, flags=re.MULTILINE)
		self.table_xml = re.sub("([0-9]),([0-9]{1,3}) MHz", "\\1.\\2 MHz", self.table_xml, flags=re.MULTILINE)
		self.table_xml = re.sub("([0-9]),([0-9]{1,3}) μm", "\\1.\\2 μm", self.table_xml, flags=re.MULTILINE)
		self.table_xml = re.sub("([0-9]),([0-9]{1,3}) Ohm", "\\1.\\2 Ohm", self.table_xml, flags=re.MULTILINE)
		self.table_xml = re.sub("([0-9]),([0-9]{1,3}) dB", "\\1.\\2 dB", self.table_xml, flags=re.MULTILINE)
		self.table_xml = re.sub("([0-9]),([0-9]{1,3}) kvar", "\\1.\\2 kvar", self.table_xml, flags=re.MULTILINE)
		self.table_xml = re.sub("±([0-9]),([0-9]{1,3})", "±\\1.\\2", self.table_xml, flags=re.MULTILINE)
		self.table_xml = re.sub("€ ([0-9]{1,3}),([0-9]{1,3})", "€ \\1.\\2", self.table_xml, flags=re.MULTILINE)
		self.table_xml = self.table_xml.replace(" %", "%")

	def write_file(self):
		basedir = os.path.dirname(os.path.abspath(__file__))

		filename = os.path.join(basedir, "model")
		filename = os.path.join(filename, "word")
		filename = os.path.join(filename, "document.xml")

		print (filename)

		file = codecs.open(filename, "w", "utf-8")
		file.write(self.suspensions_xml)
		file.close() 

		###########################################################################
		## Finally, ZIP everything up
		###########################################################################
		self.word_filename = os.path.join(self.OUTPUT_DIR, "Authorised_use_rates_document.docx")
		f.zipdir(self.word_filename)

	def get_DavidOwen_list(self):
		filename = os.path.join(self.SOURCE_DIR, "david_owen_excel.csv")
		with open(filename, "r") as f:
			reader = csv.reader(f)
			temp = list(reader)
		for i in temp:
			goods_nomenclature_item_id	= i[0]
			duty_expression				= i[1]
			end_date					= i[2]
			obj_do = davidowen_commodity(goods_nomenclature_item_id, duty_expression, end_date)
			self.david_owen_list.append(obj_do)

	def get_mfn_rates(self):
		# Get a list of the MFN components
		filename = os.path.join(self.SOURCE_DIR, "mfn_components.csv")
		with open(filename, "r") as f:
			reader = csv.reader(f)
			rows = list(reader)
		cnt = 0
		for row in rows:
			cnt += 1
			# commodity_code = "", additional_code_type_id = "", additional_code_id = "", measure_type_id = "", duty_expression_id = "", duty_amount = 0, monetary_unit_code = "",
			# measurement_unit_code = "", measurement_unit_qualifier_code = "", measure_sid = 0, quota_order_number_id = ""):
			# 0201100000,04,176.8,EUR,DTN,
			#print ("doing", str(cnt))
			commodity_code					= row[0]
			additional_code_type_id			= ""
			additional_code_id				= ""
			measure_type_id					= ""
			duty_expression_id				= str(row[1])
			duty_amount						= float(row[2])
			monetary_unit_code				= row[3]
			measurement_unit_code			= row[4]
			measurement_unit_qualifier_code = row[5]
			measure_sid						= ""
			quota_order_number_id			= ""
			my_duty = duty(commodity_code, additional_code_type_id, additional_code_id, measure_type_id, duty_expression_id, duty_amount, monetary_unit_code,
			measurement_unit_code, measurement_unit_qualifier_code, measure_sid, quota_order_number_id)
			self.mfn_component_list.append(my_duty)

	def get_special_paragraphs(self):
		# Get ships
		filename = os.path.join(self.SOURCE_DIR, "ships.txt")
		self.para_ships = []
		with open(filename, "r", encoding="utf-8") as f:
			reader = csv.reader(f, delimiter="|")
			rows = list(reader)
			for row in rows:
				self.para_ships.append(row[0])

		# Get civil air
		filename = os.path.join(self.SOURCE_DIR, "civilair.txt")
		self.para_civilair = []
		with open(filename, "r", encoding="utf-8") as f:
			reader = csv.reader(f, delimiter="|")
			rows = list(reader)
			for row in rows:
				self.para_civilair.append(row[0])

		# Get ships
		filename = os.path.join(self.SOURCE_DIR, "certainair.txt")
		self.para_certainair = []
		with open(filename, "r", encoding="utf-8") as f:
			reader = csv.reader(f, delimiter="|")
			rows = list(reader)
			for row in rows:
				self.para_certainair.append(row[0])

		# Get military
		filename = os.path.join(self.SOURCE_DIR, "military.txt")
		self.para_military = []
		with open(filename, "r", encoding="utf-8") as f:
			reader = csv.reader(f, delimiter="|")
			rows = list(reader)
			for row in rows:
				self.para_military.append(row[0])