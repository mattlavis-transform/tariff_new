import xlsxwriter
import psycopg2
import sys
import os
import re
from os import system, name 
import csv
import json

import functions
from seasonal import seasonal
from measure import measure
from duty import duty
from category import category
from special import special
from geographical_area import geographical_area
from goods_nomenclature import goods_nomenclature

class bcolors:
	HEADER = '\033[95m'
	OKBLUE = '\033[94m'
	OKGREEN = '\033[92m'
	WARNING = '\033[93m'
	FAIL = '\033[91m'
	ENDC = '\033[0m'
	BOLD = '\033[1m'
	UNDERLINE = '\033[4m'

class application(object):
	def __init__(self):
		self.authoriseduse_list		= []
		self.seasonal_list			= []
		self.special_list			= []
		self.latin_phrases			= []
		self.section_chapter_list	= []
		self.debug					= False

		self.BASE_DIR			= os.path.dirname(os.path.abspath(__file__))
		self.SOURCE_DIR			= os.path.join(self.BASE_DIR, 	"source")
		self.TEMP_DIR			= os.path.join(self.BASE_DIR, 	"temp")
		self.CHAPTER_NOTES_DIR	= os.path.join(self.SOURCE_DIR, "chapter_notes")
		self.COMPONENT_DIR		= os.path.join(self.BASE_DIR, 	"xmlcomponents")
		self.MODEL_DIR			= os.path.join(self.BASE_DIR, 	"model")
		self.CONFIG_DIR			= os.path.join(self.BASE_DIR, 	"..")
		self.CONFIG_DIR			= os.path.join(self.CONFIG_DIR, "..")
		self.CONFIG_DIR			= os.path.join(self.CONFIG_DIR, "create-data")
		self.CONFIG_DIR			= os.path.join(self.CONFIG_DIR, "config")
		self.CONFIG_FILE		= os.path.join(self.CONFIG_DIR, "config_common.json")
		self.CONFIG_FILE_LOCAL	= os.path.join(self.CONFIG_DIR, "config_migrate_measures_and_quotas.json")

		self.get_config()
		self.get_latin_terms()
		

		# Define the parameters - document type
		try:
			self.document_type = sys.argv[1]
			if self.document_type == "s":
				self.document_type = "schedule"
			if self.document_type == "c":
				self.document_type = "classification"
		except:
			self.document_type = "schedule"

		self.OUTPUT_DIR			= os.path.join(self.BASE_DIR,	"output")
		self.ODS_DIR			= os.path.join(self.OUTPUT_DIR,	"ods")
		self.OUTPUT_DIR			= os.path.join(self.OUTPUT_DIR, self.document_type)

		# Define the parameters - first chapter
		try:
			self.first_chapter = int(sys.argv[2])
		except:
			self.first_chapter = 1
			self.last_chapter = 99
			
		# Define the parameters - last chapter
		try:
			self.last_chapter   = int(sys.argv[3])
		except:
			self.last_chapter   = self.first_chapter
		if self.last_chapter > 99:
			self.last_chapter = 99
			
		if (self.document_type != "classification" and self.document_type != "schedule"):
			self.document_type = "schedule"

		self.clear()

	def get_latin_terms(self):
		latin_folder	= os.path.join(self.SOURCE_DIR,	"latin")
		latin_file		= os.path.join(latin_folder, 	"latin_phrases.txt")
		with open(latin_file, "r") as f:
			reader = csv.reader(f)
			temp = list(reader)
		
		for row in temp:
			latin_phrase	= row[0]
			self.latin_phrases.append(latin_phrase)


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

		#self.DBASE	= my_dict['dbase']
		#self.DBASE	= "tariff_staging"
		#self.DBASE	= "tariff_eu"
		#self.DBASE	= "tariff_staging"
		self.DBASE	= my_dict['dbase']
		self.p		= my_dict['p']

		# Connect to the database
		self.connect()


	def connect(self):
		#self.DBASE = "tariff_fta"
		self.conn = psycopg2.connect("dbname=" + self.DBASE + " user=postgres password=" + self.p)


	def db_disconnect(self):
		self.conn.close()


	def get_sections_chapters(self):
		# Function determines which chapters belong to which sections
		sql = """
		SELECT LEFT(gn.goods_nomenclature_item_id, 2) as chapter, cs.section_id
		FROM chapters_sections cs, goods_nomenclatures gn
		WHERE cs.goods_nomenclature_sid = gn.goods_nomenclature_sid
		AND gn.producline_suffix = '80'
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


	def read_templates(self):
		self.COMPONENT_DIR = os.path.join(self.COMPONENT_DIR, "")

		# Main document templates
		if self.document_type == "classification":
			fDocument = open(os.path.join(self.COMPONENT_DIR, "document_classification.xml"), "r")
		else:
			fDocument = open(os.path.join(self.COMPONENT_DIR, "document_schedule.xml"), "r")
		self.document_xml_string = fDocument.read()

		fHeading1 = open(os.path.join(self.COMPONENT_DIR, "heading1.xml"), "r") 
		self.sHeading1XML = fHeading1.read()

		fHeading2 = open(os.path.join(self.COMPONENT_DIR, "heading2.xml"), "r") 
		self.sHeading2XML = fHeading2.read()

		fHeading3 = open(os.path.join(self.COMPONENT_DIR, "heading3.xml"), "r") 
		self.sHeading3XML = fHeading3.read()

		fPara = open(os.path.join(self.COMPONENT_DIR, "paragraph.xml"), "r") 
		self.sParaXML = fPara.read()

		fBullet = open(os.path.join(self.COMPONENT_DIR, "bullet.xml"), "r") 
		self.sBulletXML = fBullet.read()

		fBanner = open(os.path.join(self.COMPONENT_DIR, "banner.xml"), "r") 
		self.sBannerXML = fBanner.read()

		fPageBreak = open(os.path.join(self.COMPONENT_DIR, "pagebreak.xml"), "r") 
		self.sPageBreakXML = fPageBreak.read()

		if (self.document_type == "classification"):
			fTable    = open(os.path.join(self.COMPONENT_DIR, "table_classification.xml"), "r") 
			fTableRow = open(os.path.join(self.COMPONENT_DIR, "tablerow_classification.xml"), "r") 
		else:
			fTable    = open(os.path.join(self.COMPONENT_DIR, "table_schedule.xml"), "r") 
			fTableRow = open(os.path.join(self.COMPONENT_DIR, "tablerow_schedule.xml"), "r") 

		self.table_xml_string = fTable.read()
		self.sTableRowXML = fTableRow.read()


	def get_authorised_use_commodities(self):
		# This function is required - this is used to identify any commodity codes
		# where there has been a 105 measure type assigned since the start of 2018
		# (up to the end of 2019), i.e. taking into account the measures that were
		# in place before No Deal Brexit

		# If a commodity code has a 105 instead of a 103 assigned to it, this means that there is
		# a need to insert an authorised use message in the notes column for the given commodity

		sql = """SELECT DISTINCT goods_nomenclature_item_id FROM ml.v5_2019 m WHERE measure_type_id = '105' ORDER BY 1;"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		for r in rows:
			self.authoriseduse_list.append(r[0])
		
		# Also add in cucumbers: the data cannot find these, therefore manually added, 
		# as per instruction from David Owen
		self.authoriseduse_list.append("0707000510")
		self.authoriseduse_list.append("0707000520")

		self.authoriseduse_list.append("8708701080")
		self.authoriseduse_list.append("8708701085")
		self.authoriseduse_list.append("8708701092")
		self.authoriseduse_list.append("8708701095")
		"""
		self.authoriseduse_list.append("8802110000")
		self.authoriseduse_list.append("8802120000")
		self.authoriseduse_list.append("8802200000")
		self.authoriseduse_list.append("8802300000")
		self.authoriseduse_list.append("8802400000")
		"""
		
	def get_special_notes(self):
		# This function is required - it looks in the file special_notes.csv
		# and finds a list of commodities with 'special 'notes that go alongside them
		# In actual fact, there is only one record in here at the point of
		# writing this note - 5701109000,"Dutiable surface shall not include the heading,
		# the selvedges and the fringes"

		# We may need to consider how we manage this CSV file

		filename = os.path.join(self.SOURCE_DIR, "special_notes.csv")
		with open(filename, "r") as f:
			reader = csv.reader(f)
			temp = list(reader)
		for row in temp:
			commodity_code	= row[0]
			note			= row[1]
			oSpecial = special(commodity_code, note)

			self.special_list.append(oSpecial)

	def get_seasonal(self):
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


	def get_mfn_rates_for_excel(self):
		# Get the MFN measures
		sql = """select distinct on(m.goods_nomenclature_item_id)
		m.measure_sid, m.goods_nomenclature_item_id, m.measure_type_id,
		m.validity_start_date, m.validity_end_date,
		coalesce(g.description, gnd.description) as combined_description,
		g.description as short_description, gnd.description as main_description
		from goods_nomenclature_descriptions gnd, 
		ml.measures_real_end_dates m
		left outer join ml.commodity_friendly_names g on left(m.goods_nomenclature_item_id, 8) = g.goods_nomenclature_item_id
		where m.goods_nomenclature_item_id = gnd.goods_nomenclature_item_id
		and gnd.productline_suffix = '80'
		and m.validity_start_date >= '2019-11-01'
		and m.measure_type_id in ('103', '105')
		order by goods_nomenclature_item_id"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		self.mfn_measures = []
		for rw in rows:
			m = measure()
			m.measure_sid					= rw[0]
			m.goods_nomenclature_item_id	= rw[1]
			m.measure_type_id				= rw[2]
			m.validity_start_date			= rw[3]
			m.validity_end_date				= rw[4]
			m.combined_description			= rw[5]
			self.mfn_measures.append(m)
		
		# Get the MFN measure components
		sql = """select m.measure_sid, mc.duty_expression_id, mc.duty_amount, mc.monetary_unit_code,
		mc.measurement_unit_code, mc.measurement_unit_qualifier_code
		from ml.measures_real_end_dates m, measure_components mc
		where mc.measure_sid = m.measure_sid
		and validity_start_date >= '2019-11-01'
		and measure_type_id in ('103', '105') order by measure_sid, duty_expression_id"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		self.mfn_duties = []
		for rw in rows:
			d = duty()
			d.measure_sid						= rw[0]
			d.duty_expression_id				= rw[1]
			d.duty_amount						= rw[2]
			d.monetary_unit_code				= functions.mstr(rw[3])
			d.measurement_unit_code				= functions.mstr(rw[4])
			d.measurement_unit_qualifier_code	= functions.mstr(rw[5])
			d.get_duty_string()
			self.mfn_duties.append(d)

		# Assign components (duties) to measures
		for d in self.mfn_duties:
			for m in self.mfn_measures:
				if m.measure_sid == d.measure_sid:
					m.duty_list.append (d)
					break

		# Work out which measures need to be listed
		for m in self.mfn_measures:
			for d in m.duty_list:
				if d.duty_amount != 0:
					m.is_zero = False
					m.combine_duties()
					break

		self.goods_nomenclature_item_string = ""
		for m in self.mfn_measures:
			if m.is_zero == False:
				m.get_category()
				self.goods_nomenclature_item_string += "'" + m.goods_nomenclature_item_id + "', "


		if self.document_type == "mfn":
			self.mfn_measures = sorted(self.mfn_measures, key = lambda x: x.category, reverse = False)
		else:
			self.mfn_measures = sorted(self.mfn_measures, key = lambda x: x.goods_nomenclature_item_id, reverse = False)


		self.goods_nomenclature_item_string = self.goods_nomenclature_item_string.strip()
		self.goods_nomenclature_item_string = self.goods_nomenclature_item_string.strip(",")

		# Now get the quota assignments to these commodity codes
		sql = """
		select distinct measure_type_id, goods_nomenclature_item_id, ordernumber, geographical_area_id
		from ml.measures_real_end_dates where goods_nomenclature_item_id in 
		(""" + self.goods_nomenclature_item_string + """)
		and validity_start_date >= '2019-11-01'
		and measure_type_id in ('122', '123', '143', '146')
		and ordernumber is not null
		order by goods_nomenclature_item_id, measure_type_id
		"""

		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		self.quota_assignments = []
		for row in rows:
			obj = dict(measure_type_id = row[0], goods_nomenclature_item_id = row[1], ordernumber = row[2], geographical_area_id = row[3])
			self.quota_assignments.append(obj)


		for m in self.mfn_measures:
			m.mfn_quota = ""
			if m.is_zero == False:
				for quota in self.quota_assignments:
					if quota["goods_nomenclature_item_id"] == m.goods_nomenclature_item_id:
						if quota["measure_type_id"] in ('122', '123'):
							if quota["ordernumber"] in ("094067", "094069") and quota["geographical_area_id"] in ("AR", "BR", "CL", "TH"):
								pass
							elif quota["ordernumber"] in ("094422") and quota["geographical_area_id"] in ("BR", "CL"):
								pass
							else:
								temp = self.format_quota_order_number(quota["ordernumber"])
								geo = quota["geographical_area_id"]
								if geo not in ("1008", "1011"):
									temp += " (" + geo + ")"
								if temp not in m.mfn_quota:
									m.mfn_quota += temp + "\n"

			m.mfn_quota = m.mfn_quota.strip("\n")


	def format_quota_order_number(self, v):
		v = v.strip()
		if len(v) == 6:
			v = v[0:2] + "." + v[2:6]
		return (v)


	def populate_guidance_sheet(self, worksheet_guidance):
		worksheet_guidance.set_column(0, 5, 3.5)
		worksheet_guidance.set_column(3, 3, 85)

		# The black bits
		for i in range(1, 6):
			worksheet_guidance.write(1, i, "", self.guidance_black)
			worksheet_guidance.write(8, i, "", self.guidance_black)

		for i in range(1, 9):
			worksheet_guidance.write(i, 1, "", self.guidance_black)
			worksheet_guidance.write(i, 5, "", self.guidance_black)

		worksheet_guidance.write(1, 3, "Guidance - Using this worksheet", self.guidance_black)
		worksheet_guidance.set_row(1, 30)
		worksheet_guidance.write(3, 3, "Tariffs", self.cell_bold)
		"""
		worksheet_guidance.write(4, 3, "To see information about applicable Tariff preferences after EU Exit, select the \"tariff_preferences\" tab below. " + \
		"This tab lists each commodity against which an import duty will continue to apply after EU Exit. Additionally the tab " + \
		"shows any WTO Quota or Autonomous Tariff Quota (ATQ) that is applicable to that commodity, as well as the applicable " + \
		"Preferential Tariff for trading nations or blocs with which the UK has provisionally agreed a Trade Agreement.\n\n" + \
		"Where there is no preference with the third country or if the preference is no more advantageous than the applicable MFN duty, " + \
		"the third country preferential duty is listed as '-'", self.cell_left)
		"""


		worksheet_guidance.write(4, 3, "This document shows goods that have a Most Favoured Nation (MFN) tariff rate. " + \
		"You can find them under the 'tariff_preferences' tab. If there is no trade agreement between the UK and another country " + \
		"after Brexit, you will have to use MFN rates.\n\n" + \
		"A preferential tariff rate will apply if the country you are importing from has a trade agreement with the UK or is " + \
		"part of the Generalised Scheme of Preferences.\n\n" + \
		"Any goods not listed have a zero MFN rate.\n\n" + \
		"This tab also shows any WTO quotas or Autonomous Tariff Quota (ATQ) for each commodity code.", self.cell_left)


		worksheet_guidance.write(5, 3, "\nQuotas", self.cell_bold)
		"""
		worksheet_guidance.write(6, 3, "To see information about applicable preferential Tariff Rate Quotas after EU Exit, select the \"tariff_rate_quotas\" tab below. " + \
		"As with the Tariffs tab, this tab lists each commodity against which an import duty will continue to apply after EU Exit " + \
		"and applicable WTO Quotas or Autonomous Tariff Quota (ATQ).\n\n" + \
		"In addition, if a Preferential Tariff Rate Quota (TRQ) applies to a commodity code for a given exporting country, the order " + \
		"number of that TRQ is displayed.\n\n" + \
		"Where there is no preferential Tariff Rate Quota in place with the third country, the TRQ is listed as '-'", self.cell_left)
		"""

		worksheet_guidance.write(6, 3, "You can find information about any applicable quotas for preferential tariffs under the 'tariff_rate_quotas' tab.\n\n" + \
		"Where there is a preferential tariff rate quota you can find the order number for that commodity code.", self.cell_left)


	def get_categories(self):
		self.d("Get categories of products", True)
		self.categories = []
		self.category_file = os.path.join(self.SOURCE_DIR, 	"data for tariff categorisation.csv")
		with open(self.category_file, 'r') as csvfile:
			reader = csv.reader(csvfile, delimiter=',')
			for row in reader:
				code = row[0]
				description = row[1]
				c = category(code, description)
				self.categories.append(c)

	def get_geo_areas_for_excel(self):
		self.d("Getting list of all preferential areas to include", True)
		self.preferential_areas = []
		with open(self.CONFIG_FILE_LOCAL, 'r') as f:
			my_dict = json.load(f)
			for item in my_dict["country_profiles"]:
				create_excel_template	= my_dict["country_profiles"][item]["excel_template"]
				name					= my_dict["country_profiles"][item]["excel_country_name"]
				if create_excel_template == "Yes":
					g = geographical_area()
					g.name					= my_dict["country_profiles"][item]["excel_country_name"]
					g.content				= my_dict["country_profiles"][item]
					g.country_codes			= my_dict["country_profiles"][item]["country_codes"]
					g.primary_country_code	= g.country_codes[0]
					try:
						g.excluded_country_codes = my_dict["country_profiles"][item]["excluded_country_codes"]
					except:
						g.excluded_country_codes = []
					try:
						g.weighting		= my_dict["country_profiles"][item]["weighting"]
					except:
						pass

					if "xmerica" not in g.name:
						self.preferential_areas.append (g)
					a = 1

		self.preferential_areas = sorted(self.preferential_areas, key = lambda x: x.name, 		reverse = False)
		self.preferential_areas = sorted(self.preferential_areas, key = lambda x: x.weighting,	reverse = False)


	def d(self, msg, heading = False):
		if heading == True:
			print ("\n" + msg.upper())
		else:
			print (msg)


	def get_rates_for_geo_areas(self):
		self.d("Getting rates for preferential areas", True)
		for preferential_area in self.preferential_areas:
			print ("- Getting rates for", bcolors.OKBLUE, preferential_area.name, bcolors.ENDC, "from database")
			country_codes			= preferential_area.country_codes_to_sql()
			excluded_country_codes	= preferential_area.excluded_country_codes_to_sql()
			# Get the measure components (duties)
			sql = """select mc.measure_sid, mc.duty_expression_id, mc.duty_amount, mc.monetary_unit_code,
			mc.measurement_unit_code, mc.measurement_unit_qualifier_code
			from ml.measures_real_end_dates m, measure_components mc
			where m.measure_sid = mc.measure_sid
			and m.validity_start_date >= '2019-11-01'
			and m.geographical_area_id in (""" + country_codes + """)
			and measure_type_id in ('142', '143', '145', '146', '122', '123')
			order by m.goods_nomenclature_item_id, mc.duty_expression_id
			"""

			cur = self.conn.cursor()
			cur.execute(sql)
			rows = cur.fetchall()
			preferential_area.duty_list = []
			for rw in rows:
				d = duty()
				d.measure_sid						= rw[0]
				d.duty_expression_id				= rw[1]
				d.duty_amount						= rw[2]
				d.monetary_unit_code				= functions.mstr(rw[3])
				d.measurement_unit_code				= functions.mstr(rw[4])
				d.measurement_unit_qualifier_code	= functions.mstr(rw[5])
				d.get_duty_string()
				
				preferential_area.duty_list.append(d)
			
			
			# Get the measures
			if len(preferential_area.excluded_country_codes) == 0:
				sql = """
				select measure_sid, goods_nomenclature_item_id, measure_type_id, ordernumber,
				validity_start_date, validity_end_date, geographical_area_id
				from ml.measures_real_end_dates m
				where m.validity_start_date >= '2019-11-01' and m.geographical_area_id in (""" + country_codes + """)
				and measure_type_id in ('142', '143', '145', '146', '122', '123')
				order by m.goods_nomenclature_item_id, ordernumber
				"""
			else:
				sql = """
				select measure_sid, goods_nomenclature_item_id, measure_type_id, ordernumber,
				validity_start_date, validity_end_date, geographical_area_id
				from ml.measures_real_end_dates m
				where m.validity_start_date >= '2019-11-01' and m.geographical_area_id in (""" + country_codes + """)
				and measure_type_id in ('142', '143', '145', '146', '122', '123')
				and m.measure_sid not in (
					select m.measure_sid from measure_excluded_geographical_areas mega, measures m
					where mega.measure_sid = m.measure_sid
					and m.validity_start_date >= '2019-11-01'
					and m.geographical_area_id in (""" + country_codes + """)
					and measure_type_id in ('142', '143', '145', '146', '122', '123')
					and mega.excluded_geographical_area in (""" + excluded_country_codes + """)
				)
				order by m.goods_nomenclature_item_id, ordernumber
				"""

			cur = self.conn.cursor()
			cur.execute(sql)
			rows = cur.fetchall()
			preferential_area.measure_list_preference	= []
			preferential_area.measure_list_quota		= []

			preferential_area.measure_dict_preference	= {}
			preferential_area.measure_dict_quota		= {}
			for rw in rows:
				m = measure()
				m.measure_sid					= rw[0]
				m.goods_nomenclature_item_id	= rw[1]
				m.measure_type_id				= rw[2]
				m.ordernumber					= rw[3]
				m.validity_start_date			= rw[4]
				m.validity_end_date				= rw[5]
				m.geographical_area_id			= rw[6]
				
				if m.measure_type_id in ('142', '145'):
					preferential_area.measure_list_preference.append(m)
				elif m.measure_type_id in ('143', '146', '122', '123'):
					preferential_area.measure_list_quota.append(m)


			# Assign the duties to the measures
			for d in preferential_area.duty_list:
				for m in preferential_area.measure_list_preference:
					if d.measure_sid == m.measure_sid:
						m.duty_list.append(d)
						break
				for m in preferential_area.measure_list_quota:
					if d.measure_sid == m.measure_sid:
						m.duty_list.append(d)
						break
			
			for m in preferential_area.measure_list_preference:
				m.combine_duties()
				preferential_area.measure_dict_preference[m.goods_nomenclature_item_id] = m.combined_duty

			# Simplify the list of quota measures and replace the duplicates
			"""
			for m in preferential_area.measure_list_quota:
				print ("Before", m.goods_nomenclature_item_id, m.geographical_area_id)
			"""

			temp_list = []

			for m in preferential_area.measure_list_quota:
				item = m.goods_nomenclature_item_id + "|" + m.geographical_area_id
				if item in temp_list:
					m.mark_for_removal = True
				else:
					temp_list.append (item)
					m.mark_for_removal = False

			for i in range(len(preferential_area.measure_list_quota) -1, -1, -1):
				m = preferential_area.measure_list_quota[i]
				if m.mark_for_removal == True:
					preferential_area.measure_list_quota.pop(i)

			"""
			for m in preferential_area.measure_list_quota:
				print ("After", m.goods_nomenclature_item_id, m.geographical_area_id, m.ordernumber)
			"""
			#sys.exit()

			# Combine duties into a single string for quota measures
			for m in preferential_area.measure_list_quota:
				m.combine_duties()
				preferential_area.measure_dict_quota[m.goods_nomenclature_item_id] = m.combined_duty


	def get_significant_digits(self, code):		
		if code[-8:] == '00000000':
			significant_digits = 2
		elif code[-6:] == '000000':
			significant_digits = 4
		elif code[-4:] == '0000':
			significant_digits = 6
		elif code[-2:] == '00':
			significant_digits = 8
		else:
			significant_digits = 10
		
		return significant_digits


	def get_full_nomenclature(self):
		"""
		This function is used to get a full list of commodities from an external CSV file, that needs to be re-run on a periodic basis
		from ml.goods_nomenclature_export_new
		The function puts the data into a variable self.full_commodity_list
		and then goes on to find the direct children of every commodity code
		"""
		self.d("Getting a full list of all nomenclature", True)
		self.full_commodity_list = []
		filename = os.path.join(self.SOURCE_DIR, "commodities.csv")
		with open(filename, "r") as f:
			reader = csv.reader(f)
			next (f)
			for row in reader:
				significant_digits = self.get_significant_digits(row[1])
				number_indents = int(row[6])
				if significant_digits != 2:
					number_indents += 1
				obj = dict(commodity_code = row[1], productline_suffix = int(row[2]), number_indents = number_indents,
				significant_digits = significant_digits, parentage = "", children = "", description = row[5], leaf = row[9])
				self.full_commodity_list.append (obj)

		commodity_count = len(self.full_commodity_list)

		self.d("Getting ancestry & children for nomenclature", True)
		for loop1 in range(1, commodity_count):
			commodity_code1		= self.full_commodity_list[loop1]["commodity_code"]
			productline_suffix1	= int(self.full_commodity_list[loop1]["productline_suffix"])
			number_indents1		= self.full_commodity_list[loop1]["number_indents"]
			
			my_indents = number_indents1
			for loop2 in range(loop1, -1, -1):
				commodity_code2		= self.full_commodity_list[loop2]["commodity_code"]
				productline_suffix2	= int(self.full_commodity_list[loop2]["productline_suffix"])
				number_indents2		= self.full_commodity_list[loop2]["number_indents"]
				
				if number_indents2 < my_indents:
					if productline_suffix2 == 80:
						self.full_commodity_list[loop1]["parentage"] += commodity_code2 + ","
					my_indents = number_indents2
				
				if number_indents2 == 0:
					self.full_commodity_list[loop1]["parentage"] = self.full_commodity_list[loop1]["parentage"].strip(",")
					break

			my_indents = number_indents1
			for loop2 in range(loop1 + 1, commodity_count):
				commodity_code2		= self.full_commodity_list[loop2]["commodity_code"]
				productline_suffix2	= int(self.full_commodity_list[loop2]["productline_suffix"])
				number_indents2		= self.full_commodity_list[loop2]["number_indents"]

				if number_indents2 == my_indents + 1:
					if productline_suffix2 == 80:
						if productline_suffix2 == 80:
							self.full_commodity_list[loop1]["children"] += commodity_code2 + ","
					
				if number_indents2 <= my_indents:
					self.full_commodity_list[loop1]["children"] = self.full_commodity_list[loop1]["children"].strip(",")
					break
		


	def get_preferential_equivalent_for_mfn(self):
		self.d("Getting preferential equivalents for MFN duties", True)

		"""
		Loop through all of the MFN measures that are not zero-rated, i.e. those that would be listed in the Excel spreadsheet
		and then find the FTA equivalent applicable rates. The rates in FTAs (and GSP etc) may be:
		- directly applicable to the commodity code (rare)
		- inherited down from a parent (most likely)
		- not applicable at all (i.e. there is no coverage / reduction from the base non-zero MFN rate)
		"""
		"""
		Loop through all MFN measures that are not zero
			Loop through all pref areas (say, Fiji)
			Then loop through all preferential rates in those areas

		"""
		for preferential_area in self.preferential_areas:
			print (" - Working out preferential equivalent for", bcolors.OKBLUE, preferential_area.name, bcolors.ENDC)
			preferential_area.preferential_rates		= {}
			preferential_area.quota_order_numbers		= {}
			preferential_area.quota_order_number_list	= []
			for mfn_measure in self.mfn_measures:
				if mfn_measure.is_zero == False:

					# first do preferences
					matched = False
					
					# First, check if there is a match against the actual commodity code for the preferential agreement
					for m2 in preferential_area.measure_list_preference:
						if matched == False:
							if m2.goods_nomenclature_item_id == mfn_measure.goods_nomenclature_item_id:
								# There is a direct match between the trade agreement and the non-zero MFN
								m2.combine_duties()
								preferential_area.preferential_rates[m2.goods_nomenclature_item_id] = m2.combined_duty
								matched = True
								break

					# Second, look at the parentage for downward inheritance
					if matched == False:
						for g in self.full_commodity_list:
							if g["commodity_code"] == mfn_measure.goods_nomenclature_item_id and int(g["productline_suffix"]) == 80:
								ancestry = g["parentage"].split(",")
								for m2 in preferential_area.measure_list_preference:
									if m2.goods_nomenclature_item_id in ancestry:
										m2.combine_duties()
										preferential_area.preferential_rates[mfn_measure.goods_nomenclature_item_id] = m2.combined_duty
										matched = True
										break

								break
					
					# Thirdly, look at the children
					if matched == False:
						for g in self.full_commodity_list:
							if g["commodity_code"] == mfn_measure.goods_nomenclature_item_id and int(g["productline_suffix"]) == 80:
								children = g["children"].split(",")
								if children == "":
									child_count = 0
									break
								else:
									child_count = len(children)

								if child_count > 0:
									child_duties = []
									for child in children:
										if child in preferential_area.measure_dict_preference:
											child_duties.append (preferential_area.measure_dict_preference[child])

									child_duties2 = set(child_duties)
									if len(child_duties2) == 0:
										preferential_area.preferential_rates[mfn_measure.goods_nomenclature_item_id] = "-"
									elif len(child_duties2) == 1:
										preferential_area.preferential_rates[mfn_measure.goods_nomenclature_item_id] = child_duties[0]
									else:
										preferential_area.preferential_rates[mfn_measure.goods_nomenclature_item_id] = "Variable"

								break



					# Second, do quotas
					# At this point, I am looping first through every preferential area, and then through every MFN non-zero commodity
					# The variables are   "preferential_area"   and   "mfn_measure"

					matched = False
					
					# First, check if there is a match against the actual commodity code for the preferential agreement
					for m2 in preferential_area.measure_list_quota:
						if 1 > 0 : # matched == False:

							if m2.goods_nomenclature_item_id == mfn_measure.goods_nomenclature_item_id:
								# There is a direct match between the trade agreement and the non-zero MFN
								preferential_area.quota_order_numbers[m2.goods_nomenclature_item_id] = m2.ordernumber
								quota_measure = measure()
								quota_measure.goods_nomenclature_item_id	= mfn_measure.goods_nomenclature_item_id
								quota_measure.ordernumber					= m2.ordernumber
								quota_measure.geographical_area_id			= m2.geographical_area_id

								preferential_area.quota_order_number_list.append (quota_measure)
								matched = True
								#break

					# Second, look at the parentage for downward inheritance
					# At this point I am looping first round the preferential areas and then round the non-zero MFN measures

					if matched == False:
						for g in self.full_commodity_list:
							if g["commodity_code"] == mfn_measure.goods_nomenclature_item_id and int(g["productline_suffix"]) == 80:
								ancestry = g["parentage"].split(",")

								for m2 in preferential_area.measure_list_quota:
									if m2.goods_nomenclature_item_id in ancestry:
										preferential_area.quota_order_numbers[mfn_measure.goods_nomenclature_item_id] = m2.ordernumber
										quota_measure = measure()
										quota_measure.goods_nomenclature_item_id	= mfn_measure.goods_nomenclature_item_id
										quota_measure.ordernumber					= m2.ordernumber
										quota_measure.geographical_area_id			= m2.geographical_area_id

										preferential_area.quota_order_number_list.append (quota_measure)
										matched = True

								break
					
					# Thirdly, look at the children
					if matched == False:
						for g in self.full_commodity_list:
							if g["commodity_code"] == mfn_measure.goods_nomenclature_item_id and int(g["productline_suffix"]) == 80:
								children = g["children"].split(",")
								if children == "":
									child_count = 0
									break
								else:
									child_count = len(children)

								if child_count > 0:
									child_duties = []
									for child in children:
										if child in preferential_area.measure_dict_quota:
											child_duties.append (preferential_area.measure_dict_quota[child])

									child_duties2 = set(child_duties)
									if len(child_duties2) == 0:
										preferential_area.quota_order_numbers[mfn_measure.goods_nomenclature_item_id] = "-"
									elif len(child_duties2) == 1:
										preferential_area.quota_order_numbers[mfn_measure.goods_nomenclature_item_id] = child_duties[0]
									else:
										preferential_area.quota_order_numbers[mfn_measure.goods_nomenclature_item_id] = "Variable"

								break
				
			"""
			for quota_measure in preferential_area.quota_order_number_list:
				print ("Assigned", quota_measure.goods_nomenclature_item_id, quota_measure.ordernumber, quota_measure.geographical_area_id)

			# Simplify the list of quota measures and replace the duplicates
			for m in preferential_area.quota_order_number_list:
				print ("Before", m.goods_nomenclature_item_id, m.geographical_area_id)
			"""

			temp_list = []

			for m in preferential_area.quota_order_number_list:
				item = m.goods_nomenclature_item_id + "|" + m.ordernumber + "|" + m.geographical_area_id
				if item in temp_list:
					m.mark_for_removal = True
				else:
					temp_list.append (item)
					m.mark_for_removal = False

				#print ("Before", m.goods_nomenclature_item_id, m.geographical_area_id)

			for i in range(len(preferential_area.quota_order_number_list) -1, -1, -1):
				m = preferential_area.quota_order_number_list[i]
				if m.mark_for_removal == True:
					preferential_area.quota_order_number_list.pop(i)

			"""
			for m in preferential_area.quota_order_number_list:
				print ("After", m.goods_nomenclature_item_id, m.geographical_area_id)
			"""


	def create_excel_styles(self, workbook):
		self.column_header = workbook.add_format()
		self.column_header.set_bold()
		self.column_header.set_bg_color('#cccccc')
		self.column_header.set_align('vcenter')
		self.column_header.set_text_wrap()
		self.column_header.set_border(1)
		self.column_header.set_font_name('Times New Roman')
		self.column_header.set_indent(1)

		self.column_header_centre = workbook.add_format()
		self.column_header_centre.set_bold()
		self.column_header_centre.set_bg_color('#cccccc')
		self.column_header_centre.set_align('vcenter')
		self.column_header_centre.set_text_wrap()
		self.column_header_centre.set_border(1)
		self.column_header_centre.set_align('center')
		self.column_header_centre.set_font_name('Times New Roman')

		self.cell = workbook.add_format()
		self.cell.set_text_wrap()
		self.cell.set_indent(1)
		self.cell.set_align('top')
		self.cell.set_border(1)
		self.cell.set_font_name('Times New Roman')

		self.cell_left = workbook.add_format()
		self.cell_left.set_text_wrap()
		self.cell_left.set_indent(1)
		self.cell_left.set_align('top')
		self.cell_left.set_align('left')
		self.cell_left.set_border(0)
		self.cell_left.set_font_name('Times New Roman')
		self.cell_left.set_font_size(12)

		self.cell_bold = workbook.add_format()
		self.cell_bold.set_text_wrap()
		self.cell_bold.set_align('top')
		self.cell_bold.set_align('left')
		self.cell_bold.set_border(0)
		self.cell_bold.set_font_name('Times New Roman')
		self.cell_bold.set_indent(1)
		self.cell_bold.set_bold()
		self.cell_bold.set_font_size(12)

		self.cell_blue = workbook.add_format()
		self.cell_blue.set_text_wrap()
		self.cell_blue.set_align('top')
		self.cell_blue.set_bg_color('cceeff')
		self.cell_blue.set_border(1)
		self.cell_blue.set_align('center')
		self.cell_blue.set_font_name('Times New Roman')

		self.cell_centre = workbook.add_format()
		self.cell_centre.set_text_wrap()
		self.cell_centre.set_align('top')
		self.cell_centre.set_border(1)
		self.cell_centre.set_align('center')
		self.cell_centre.set_font_name('Times New Roman')

		self.category = workbook.add_format()
		self.category.set_bg_color('black')
		self.category.set_color('white')
		self.category.set_bold()
		self.category.set_align('vcenter')
		self.category.set_font_name('Times New Roman')
		self.category.set_font_size(12)
		self.category.set_indent(1)

		self.guidance_black = workbook.add_format()
		self.guidance_black.set_bg_color('black')
		self.guidance_black.set_color('white')
		self.guidance_black.set_bold()
		self.guidance_black.set_align('vcenter')
		self.guidance_black.set_font_size(14)
		self.guidance_black.set_font_name('Times New Roman')

		self.description_indents = []
		for i in range(0, 20):
			obj = workbook.add_format()
			obj.set_text_wrap()
			obj.set_indent(i)
			obj.set_align('top')
			obj.set_border(1)
			obj.set_font_name('Times New Roman')
			if i < 2:
				obj.set_bold()
			
			self.description_indents.append(obj)



	def write_mfns_to_excel(self):
		print ("Write data to Excel spreadsheet")

		path			= os.path.join(self.ODS_DIR, "mfn_and_preferential_duties.xlsx")
		workbook		= xlsxwriter.Workbook(path)

		self.create_excel_styles(workbook)

		# Create worksheets
		worksheet_guidance	= workbook.add_worksheet("guidance")
		worksheet			= workbook.add_worksheet("tariff_preferences")
		worksheet2			= workbook.add_worksheet("tariff_rate_quotas")

		self.populate_guidance_sheet(worksheet_guidance)

		preferential_area_count = len(self.preferential_areas)
		worksheet.set_column(0, 0, 20)
		worksheet.set_column(1, 1, 15)
		worksheet.set_column(2, 2, 60) 
		worksheet.set_column(3, 4, 20)
		worksheet.set_column(5, preferential_area_count + 5, 20)

		worksheet2.set_column(0, 0, 20)
		worksheet2.set_column(1, 1, 15)
		worksheet2.set_column(2, 2, 60) 
		worksheet2.set_column(3, 4, 20)
		worksheet2.set_column(5, preferential_area_count + 5, 20)

		worksheet.freeze_panes(1, 0)
		worksheet2.freeze_panes(1, 0)

		my_row = 0
		current_category = ""

		for m in self.mfn_measures:
			if m.is_zero == False:
				quota_matched = False
				if m.category != "Unspecified":
					if m.category != current_category and m.category != "Unspecified":
						# Write the columns headers for the MFN sheet
						my_row += 1
						worksheet.set_row(my_row - 1, 45)
						worksheet.write('A' + str(my_row), 'Category', self.column_header)
						worksheet.write('B' + str(my_row), 'Commodity code', self.column_header)
						worksheet.write('C' + str(my_row), 'Description', self.column_header)
						worksheet.write('D' + str(my_row), 'Most favoured nation (MFN) rate', self.column_header_centre)
						worksheet.write('E' + str(my_row), 'MFN TRQ', self.column_header_centre)

						# Write the columns headers for the quotas sheet
						worksheet2.set_row(my_row - 1, 45)
						worksheet2.write('A' + str(my_row), 'Category', self.column_header)
						worksheet2.write('B' + str(my_row), 'Commodity code', self.column_header)
						worksheet2.write('C' + str(my_row), 'Description', self.column_header)
						worksheet2.write('D' + str(my_row), 'Most favoured nation (MFN) rate', self.column_header_centre)
						worksheet2.write('E' + str(my_row), 'MFN TRQ', self.column_header_centre)

						my_col = 5
						for item in self.preferential_areas:
							# Write the preferential area name
							worksheet.write(my_row - 1, my_col, item.name, self.column_header_centre)
							worksheet2.write(my_row - 1, my_col, item.name, self.column_header_centre)
							my_col += 1

						# Write the self.category row 
						my_row += 1
						#worksheet.merge_range(my_row - 1, 0, my_row -1, preferential_area_count + 3, m.category, self.category)
						worksheet.set_row(my_row - 1, 30)
						#worksheet2.merge_range(my_row - 1, 0, my_row -1, preferential_area_count + 3, m.category, self.category)
						worksheet2.set_row(my_row - 1, 30)

					my_row += 1
					if m.mfn_quota == "":
						m.mfn_quota = "-"

					# Write the commodity code, description, duty and MFN quota data to the MFN sheet
					worksheet.write('A' + str(my_row), m.category, self.cell)
					worksheet.write('B' + str(my_row), m.goods_nomenclature_formatted(), self.cell)
					worksheet.write('C' + str(my_row), m.combined_description, self.cell)
					worksheet.write('D' + str(my_row), m.combined_duty, self.cell_blue)
					worksheet.write('E' + str(my_row), m.mfn_quota, self.cell_blue)

					# Write the commodity code, description, duty and MFN quota data to the quotas sheet
					worksheet2.write('A' + str(my_row), m.category, self.cell)
					worksheet2.write('B' + str(my_row), m.goods_nomenclature_formatted(), self.cell)
					worksheet2.write('C' + str(my_row), m.combined_description, self.cell)
					worksheet2.write('D' + str(my_row), m.combined_duty, self.cell_blue)
					worksheet2.write('E' + str(my_row), m.mfn_quota, self.cell_blue)

					# Write the preferential rates, and the quota rates
					my_col = 5
					preferential_rate	= "-"
					for preferential_area in self.preferential_areas:
						if m.goods_nomenclature_item_id in preferential_area.preferential_rates:
							preferential_rate = preferential_area.preferential_rates[m.goods_nomenclature_item_id]

						quota_order_number_string = ""
						for quota_measure in preferential_area.quota_order_number_list:
							if m.goods_nomenclature_item_id == quota_measure.goods_nomenclature_item_id:
								quota_order_number_string += self.format_quota_order_number(quota_measure.ordernumber)
								if quota_measure.geographical_area_id != preferential_area.primary_country_code:
									quota_order_number_string += " (" + quota_measure.geographical_area_id + ")"
								
								
								quota_order_number_string += "\n "
						
							quota_order_number_string = quota_order_number_string.strip("")
							quota_order_number_string = quota_order_number_string.strip("\n")

						if quota_order_number_string == "":
							quota_order_number_string = "-"
						#print (m.goods_nomenclature_item_id, quota_order_number_string)

						
						worksheet.write(my_row - 1, my_col, preferential_rate, self.cell_centre)
						worksheet2.write(my_row - 1, my_col, quota_order_number_string, self.cell_centre)
						my_col += 1

					current_category = m.category

		workbook.close()


	def write_full_mfns(self):
		self.d ("Write data to Excel spreadsheet", True)

		path			= os.path.join(self.ODS_DIR, "mfn_and_preferential_duties_full.xlsx")
		workbook		= xlsxwriter.Workbook(path)

		self.create_excel_styles(workbook)

		# Create worksheets
		worksheet_guidance	= workbook.add_worksheet("guidance")
		worksheet			= workbook.add_worksheet("tariff_preferences")
		worksheet2			= workbook.add_worksheet("tariff_rate_quotas")

		self.populate_guidance_sheet(worksheet_guidance)

		preferential_area_count = len(self.preferential_areas)
		worksheet.set_column(0, 0, 15)
		worksheet.set_column(1, 1, 90) 
		worksheet.set_column(2, 2, 15)
		worksheet.set_column(3, 4, 20)
		worksheet.set_column(5, preferential_area_count + 5, 20)

		worksheet2.set_column(0, 0, 15)
		worksheet2.set_column(1, 1, 90) 
		worksheet2.set_column(2, 2, 15)
		worksheet2.set_column(3, 4, 20)
		worksheet2.set_column(5, preferential_area_count + 5, 20)

		my_row = 0

		# Write the columns headers for the MFN sheet
		my_row += 1
		worksheet.set_row(my_row - 1, 45)
		worksheet.write('A' + str(my_row), 'Commodity code', self.column_header)
		worksheet.write('B' + str(my_row), 'Description', self.column_header)
		worksheet.write('C' + str(my_row), 'End line', self.column_header_centre)
		worksheet.write('D' + str(my_row), 'Most favoured nation (MFN) rate', self.column_header_centre)
		worksheet.write('E' + str(my_row), 'MFN TRQ', self.column_header_centre)
		worksheet.freeze_panes(2, 0)

		# Write the columns headers for the quotas sheet
		worksheet2.set_row(my_row - 1, 45)
		worksheet2.write('A' + str(my_row), 'Commodity code', self.column_header)
		worksheet2.write('B' + str(my_row), 'Description', self.column_header)
		worksheet2.write('C' + str(my_row), 'End line', self.column_header)
		worksheet2.write('D' + str(my_row), 'Most favoured nation (MFN) rate', self.column_header_centre)
		worksheet2.write('E' + str(my_row), 'MFN TRQ', self.column_header_centre)
		worksheet2.freeze_panes(2, 0)


		my_col = 5
		for item in self.preferential_areas:
			# Write the preferential area name
			worksheet.write(my_row - 1, my_col, item.name, self.column_header_centre)
			worksheet2.write(my_row - 1, my_col, item.name, self.column_header_centre)
			my_col += 1


		for g in self.full_commodity_list:
			my_row += 1

			# Write the commodity code, description, duty and MFN quota data to the MFN sheet
			worksheet.write('A' + str(my_row), self.format_goods_nomenclature(g["commodity_code"], g["productline_suffix"]), self.cell)
			worksheet.write('B' + str(my_row), self.cleanse_description(g["description"]), self.description_indents[g["number_indents"]])
			worksheet.write('C' + str(my_row), self.yn(g["leaf"], "Y", ""), self.cell_centre)
			"""
			worksheet.write('D' + str(my_row), m.combined_duty, self.cell_blue)
			worksheet.write('E' + str(my_row), m.mfn_quota, self.cell_blue)

			# Write the commodity code, description, duty and MFN quota data to the quotas sheet
			worksheet2.write('A' + str(my_row), m.goods_nomenclature_formatted(), self.cell)
			worksheet2.write('B' + str(my_row), m.combined_description, self.cell)
			worksheet2.write('D' + str(my_row), m.combined_duty, self.cell_blue)
			worksheet2.write('E' + str(my_row), m.mfn_quota, self.cell_blue)
			"""


		workbook.close()

	def yn(self, s, yes, no):
		s = str(s).lower()
		if s in ("1", "Y"):
			s = yes
		else:
			s = no
		return s


	def format_goods_nomenclature(self, s, productline_suffix):
		productline_suffix = str(productline_suffix)
		if productline_suffix != "80":
			commodity_code_formatted = ""
		else:
			if s[4:10] == "000000":
				commodity_code_formatted = s[0:4]
			elif s[6:10] == "0000":
				commodity_code_formatted = s[0:4] + ' ' + s[4:6]
			elif s[8:10] == "00":
				commodity_code_formatted = s[0:4] + ' ' + s[4:6] + ' ' + s[6:8]
			else:
				commodity_code_formatted = s[0:4] + ' ' + s[4:6] + ' ' + s[6:8] + ' ' + s[8:10]
		return (commodity_code_formatted)

	def select_document_type(self):
		try:
			if sys.argv[1] == "full":
				self.document_type = "full"
			else:
				self.document_type = "mfn"
		except:
			self.document_type = "mfn"

	def cleanse_description(self, s):
		s = s.replace(" <br>", "<br>")
		s = s.replace("<p/>", "<br>")
		s = s.replace("<br />", "<br>")
		s = s.replace("<br/>", "<br>")
		for i in range(1, 5):
			s = s.replace("<br><br>", "<br>")

		if s[-4:] == "<br>":
			s = s[:-4]

		s = s.replace("<br>", "\n")
		s = s.replace("<br />", "\n")
		s = s.replace("<br/>", "\n")

		s = s.replace("<sup>", ' ')
		s = s.replace("</sup>", ' ')

		s = s.replace("<sub>", ' ')
		s = s.replace("</sub>", ' ')

		s = s.replace("|of the CN", "")
		s = s.replace("liters", "litres")
		s = s.replace("|%|", "% ")
		s = s.replace("|gram", " gram")
		s = s.replace("|g", "g")
		s = s.replace("|kg", "kg")
		s = s.replace("|", " ")
		s = re.sub("([0-9]) %", "\\1%", s)
		s = s.replace("!x!", "x")
		s = s.replace(" kg", "kg")
		s = s.replace(" -goods", " - goods")

		if s[-3:] == "!1!":
			s = s[:-3]
		s = s.replace("\r\r", "\r")
		s = s.replace("\r\n", "\n")
		s = s.replace("\n\r", "\n")
		s = s.replace("\n\n", "\n")
		s = s.replace("\r", "\n")
		s = s.replace("!1!", "\n")
		s = s.replace("!o!", chr(176))
		s = s.replace("\xA0", " ")
		s = s.replace(" %", "%")
		s = re.sub("arcases", "arcasses", s, flags=re.MULTILINE)


		s = re.sub(" ([0-9]{1,4}),([0-9]{1,4}) ", " \\1.\\2 ", s, flags=re.MULTILINE)
		s = re.sub("([0-9]{1,4}),([0-9]{1,4})/", "\\1.\\2/", s, flags=re.MULTILINE)
		s = re.sub(" ([0-9]{1,4}),([0-9]{1,4})%", " \\1.\\2%", s, flags=re.MULTILINE)
		s = re.sub(" ([0-9]{1,4}),([0-9]{1,4})\\)", " \\1.\\2)", s, flags=re.MULTILINE)
		s = re.sub("([0-9]),([0-9])%", "\\1.\\2%", s, flags=re.MULTILINE)
		s = re.sub("([0-9]),([0-9]) kg", "\\1.\\2 kg", s, flags=re.MULTILINE)
		s = re.sub("([0-9]),([0-9]) Kg", "\\1.\\2 kg", s, flags=re.MULTILINE)
		s = re.sub("([0-9]),([0-9]) C", "\\1.\\2 C", s, flags=re.MULTILINE)
		s = re.sub("([0-9]),([0-9])kg", "\\1.\\2kg", s, flags=re.MULTILINE)
		s = re.sub("([0-9]),([0-9]{1,3}) g", "\\1.\\2 g", s, flags=re.MULTILINE)
		s = re.sub("([0-9]),([0-9]{1,3})g", "\\1.\\2g", s, flags=re.MULTILINE)
		s = re.sub("([0-9]),([0-9]{1,3}) dl", "\\1.\\2 dl", s, flags=re.MULTILINE)
		s = re.sub("([0-9]),([0-9]{1,3}) m", "\\1.\\2 m", s, flags=re.MULTILINE)
		s = re.sub("([0-9]),([0-9]{1,3})m", "\\1.\\2m", s, flags=re.MULTILINE)
		s = re.sub("([0-9]),([0-9]{1,3}) decitex", "\\1.\\2 decitex", s, flags=re.MULTILINE)
		s = re.sub("([0-9]),([0-9]{1,3}) l", "\\1.\\2 l", s, flags=re.MULTILINE)
		s = re.sub("([0-9]),([0-9]{1,3}) kW", "\\1.\\2 kW", s, flags=re.MULTILINE)
		s = re.sub("([0-9]),([0-9]{1,3}) W", "\\1.\\2 W", s, flags=re.MULTILINE)
		s = re.sub("([0-9]),([0-9]{1,3}) V", "\\1.\\2 V", s, flags=re.MULTILINE)
		s = re.sub("([0-9]),([0-9]{1,3}) Ah", "\\1.\\2 Ah", s, flags=re.MULTILINE)
		s = re.sub("([0-9]),([0-9]{1,3}) bar", "\\1.\\2 bar", s, flags=re.MULTILINE)
		s = re.sub("([0-9]),([0-9]{1,3}) cm", "\\1.\\2 cm", s, flags=re.MULTILINE)
		s = re.sub("([0-9]),([0-9]{1,3}) Nm", "\\1.\\2 Nm", s, flags=re.MULTILINE)
		s = re.sub("([0-9]),([0-9]{1,3}) kV", "\\1.\\2 kV", s, flags=re.MULTILINE)
		s = re.sub("([0-9]),([0-9]{1,3}) kHz", "\\1.\\2 kHz", s, flags=re.MULTILINE)
		s = re.sub("([0-9]),([0-9]{1,3}) kV", "\\1.\\2 kV", s, flags=re.MULTILINE)
		s = re.sub("([0-9]),([0-9]{1,3}) MHz", "\\1.\\2 MHz", s, flags=re.MULTILINE)
		s = re.sub("([0-9]),([0-9]{1,3}) μm", "\\1.\\2 μm", s, flags=re.MULTILINE)
		s = re.sub("([0-9]),([0-9]{1,3}) Ohm", "\\1.\\2 Ohm", s, flags=re.MULTILINE)
		s = re.sub("([0-9]),([0-9]{1,3}) dB", "\\1.\\2 dB", s, flags=re.MULTILINE)
		s = re.sub("([0-9]),([0-9]{1,3}) kvar", "\\1.\\2 kvar", s, flags=re.MULTILINE)
		s = re.sub("±([0-9]),([0-9]{1,3})", "±\\1.\\2", s, flags=re.MULTILINE)
		s = re.sub("€ ([0-9]{1,3}),([0-9]{1,3})", "€ \\1.\\2", s, flags=re.MULTILINE)

		return s