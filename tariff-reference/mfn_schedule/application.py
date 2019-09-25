import xlsxwriter
import psycopg2
import sys
import os
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

		self.DBASE	= my_dict['dbase']
		#self.DBASE	= "tariff_staging"
		#self.DBASE	= "tariff_eu"
		self.DBASE	= "tariff_staging"
		self.p		= my_dict['p']

		# Connect to the database
		self.connect()


	def connect(self):
		self.DBASE = "tariff_fta"
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

		8802110000
		
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


		self.mfn_measures = sorted(self.mfn_measures, key = lambda x: x.category, reverse = False)


		self.goods_nomenclature_item_string = self.goods_nomenclature_item_string.strip()
		self.goods_nomenclature_item_string = self.goods_nomenclature_item_string.strip(",")

		# Now get the quota assignments to these commodity codes
		sql = """
		select distinct measure_type_id, goods_nomenclature_item_id, ordernumber, geographical_area_id from measures
		where goods_nomenclature_item_id in 
		(""" + self.goods_nomenclature_item_string + """)
		and validity_start_date >= '2018-01-01'
		and measure_type_id in ('122', '123', '143', '146')
		and ordernumber is not null
		order by goods_nomenclature_item_id, measure_type_id
		"""
		print (sql)
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
							m.mfn_quota = "Tariff rate quota"
							break
							"""
							temp = quota["ordernumber"]
							temp = temp[0:2] + "." + temp[2:6]
							m.mfn_quota += temp + "\n"
							print ("found a quota match")
							"""

			#m.mfn_quota = m.mfn_quota.strip("\n")




	def write_mfns_to_excel(self):
		print ("Write data to Excel spreadsheet")
		workbook = xlsxwriter.Workbook('hello.xlsx')

		column_header = workbook.add_format()
		column_header.set_bold()
		column_header.set_bg_color('#cccccc')
		column_header.set_align('vcenter')
		column_header.set_text_wrap()
		column_header.set_border(1)

		column_header_centre = workbook.add_format()
		column_header_centre.set_bold()
		column_header_centre.set_bg_color('#cccccc')
		column_header_centre.set_align('vcenter')
		column_header_centre.set_text_wrap()
		column_header_centre.set_border(1)
		column_header_centre.set_align('center')

		cell = workbook.add_format()
		cell.set_text_wrap()
		cell.set_align('top')
		cell.set_border(1)

		cell_blue = workbook.add_format()
		cell_blue.set_text_wrap()
		cell_blue.set_align('top')
		cell_blue.set_bg_color('cceeff')
		cell_blue.set_border(1)
		cell_blue.set_align('center')

		cell_centre = workbook.add_format()
		cell_centre.set_text_wrap()
		cell_centre.set_align('top')
		cell_centre.set_border(1)
		cell_centre.set_align('center')

		category = workbook.add_format()
		category.set_bg_color('black')
		category.set_color('white')
		category.set_bold()
		category.set_align('vcenter')

		worksheet = workbook.add_worksheet("tariff_preferences")
		worksheet2 = workbook.add_worksheet("tariff_rate_quotas")

		preferential_area_count = len(self.preferential_areas)
		worksheet.set_column(0, 0, 15)
		worksheet.set_column(1, 1, 60) 
		worksheet.set_column(2, 3, 20)
		worksheet.set_column(4, preferential_area_count + 4, 20)

		worksheet2.set_column(0, 0, 15)
		worksheet2.set_column(1, 1, 60) 
		worksheet2.set_column(2, 3, 20)
		worksheet2.set_column(4, preferential_area_count + 4, 20)

		my_row = 0
		current_category = ""

		for m in self.mfn_measures:
			if m.is_zero == False:
				quota_matched = False
				if m.category != current_category and m.category != "Unspecified":
					# Write the category row 
					my_row += 1
					worksheet.merge_range(my_row - 1, 0, my_row -1, preferential_area_count + 3, m.category, category)
					worksheet.set_row(my_row - 1, 30)
					worksheet2.merge_range(my_row - 1, 0, my_row -1, preferential_area_count + 3, m.category, category)
					worksheet2.set_row(my_row - 1, 30)

					# Write the columns headers
					my_row += 1
					worksheet.set_row(my_row - 1, 30)
					worksheet.write('A' + str(my_row), 'Commodity code', column_header)
					worksheet.write('B' + str(my_row), 'Description', column_header)
					worksheet.write('C' + str(my_row), 'Most favoured nation (MFN) rate', column_header_centre)
					worksheet.write('D' + str(my_row), 'MFN TRQ', column_header_centre)

					worksheet2.set_row(my_row - 1, 30)
					worksheet2.write('A' + str(my_row), 'Commodity code', column_header)
					worksheet2.write('B' + str(my_row), 'Description', column_header)
					worksheet2.write('C' + str(my_row), 'Most favoured nation (MFN) rate', column_header_centre)
					worksheet2.write('D' + str(my_row), 'MFN TRQ', column_header_centre)

					my_col = 4
					for item in self.preferential_areas:
						worksheet.write(my_row - 1, my_col, item.name, column_header_centre)
						worksheet2.write(my_row - 1, my_col, item.name, column_header_centre)
						my_col += 1

				my_row += 1
				if m.mfn_quota == "":
					m.mfn_quota = "-"
				worksheet.write('A' + str(my_row), m.goods_nomenclature_formatted(), cell)
				worksheet.write('B' + str(my_row), m.combined_description, cell)
				worksheet.write('C' + str(my_row), m.combined_duty, cell_blue)
				worksheet.write('D' + str(my_row), m.mfn_quota, cell_blue)

				worksheet2.write('A' + str(my_row), m.goods_nomenclature_formatted(), cell)
				worksheet2.write('B' + str(my_row), m.combined_description, cell)
				worksheet2.write('C' + str(my_row), m.combined_duty, cell_blue)
				worksheet2.write('D' + str(my_row), m.mfn_quota, cell_blue)

				# Write the preferential rates
				my_col = 4
				preferential_rate	= "-"
				for preferential_area in self.preferential_areas:
					preferential_quota	= "-"
					if m.goods_nomenclature_item_id in preferential_area.preferential_rates:
						preferential_rate = preferential_area.preferential_rates[m.goods_nomenclature_item_id]

					if preferential_area.name == "Chile":
						for quota in self.quota_assignments:
							if quota["goods_nomenclature_item_id"] == m.goods_nomenclature_item_id:
								print ("Chile match on commodity code", quota["measure_type_id"], quota["geographical_area_id"])
								if quota["measure_type_id"] in ('143', '146'):
									if quota["geographical_area_id"] == "CL":
										preferential_quota = "Applicable"
										quota_matched = True
										break

					
					worksheet.write(my_row - 1, my_col, preferential_rate, cell_centre)
					worksheet2.write(my_row - 1, my_col, preferential_quota, cell_centre)
					my_col += 1

				current_category = m.category

		workbook.close()
			
	def get_categories(self):
		print ("Get categories of products")
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
		print ("Getting list of all preferential areas to include")
		self.preferential_areas = []
		with open(self.CONFIG_FILE_LOCAL, 'r') as f:
			my_dict = json.load(f)
			for item in my_dict["country_profiles"]:
				create_excel_template = my_dict["country_profiles"][item]["excel_template"]
				if create_excel_template == "Yes":
					g = geographical_area()
					g.name			= my_dict["country_profiles"][item]["excel_country_name"]
					g.content		= my_dict["country_profiles"][item]
					g.country_codes	= my_dict["country_profiles"][item]["country_codes"]

					self.preferential_areas.append (g)
					a = 1

		self.preferential_areas = sorted(self.preferential_areas, key = lambda x: x.name, reverse = False)


	def get_rates_for_geo_areas(self):
		print ("Getting rates for preferential areas")
		for item in self.preferential_areas:
			print ("- Getting rates for", item.name)
			country_codes = item.country_codes_to_sql()
			#print (country_codes)
			# Get the measure components (duties)
			sql = """select mc.measure_sid, mc.duty_expression_id, mc.duty_amount, mc.monetary_unit_code,
			mc.measurement_unit_code, mc.measurement_unit_qualifier_code
			from ml.measures_real_end_dates m, measure_components mc
			where m.measure_sid = mc.measure_sid
			and m.validity_start_date >= '2019-11-01'
			and m.geographical_area_id in (""" + country_codes + """)
			and measure_type_id in ('142', '143', '145', '146')
			order by m.goods_nomenclature_item_id, mc.duty_expression_id
			"""
			cur = self.conn.cursor()
			cur.execute(sql)
			rows = cur.fetchall()
			item.duty_list = []
			for rw in rows:
				d = duty()
				d.measure_sid						= rw[0]
				d.duty_expression_id				= rw[1]
				d.duty_amount						= rw[2]
				d.monetary_unit_code				= functions.mstr(rw[3])
				d.measurement_unit_code				= functions.mstr(rw[4])
				d.measurement_unit_qualifier_code	= functions.mstr(rw[5])
				d.get_duty_string()
				
				item.duty_list.append(d)
			
			# Get the measures
			sql = """
			select measure_sid, goods_nomenclature_item_id, measure_type_id, ordernumber, validity_start_date, validity_end_date
			from ml.measures_real_end_dates m
			where m.validity_start_date >= '2019-11-01' and m.geographical_area_id in (""" + country_codes + """)
			and measure_type_id in ('142', 'x143', '145', 'x146')
			order by m.goods_nomenclature_item_id
			"""
			cur = self.conn.cursor()
			cur.execute(sql)
			rows = cur.fetchall()
			item.measure_list = []
			item.measure_dict = {}
			for rw in rows:
				m = measure()
				m.measure_sid					= rw[0]
				m.goods_nomenclature_item_id	= rw[1]
				m.measure_type_id				= rw[2]
				m.ordernumber					= rw[3]
				m.validity_start_date			= rw[4]
				m.validity_end_date				= rw[5]
				
				item.measure_list.append(m)

			# Assign the duties to the measures
			for d in item.duty_list:
				for m in item.measure_list:
					if d.measure_sid == m.measure_sid:
						m.duty_list.append(d)
						break
			
			for m in item.measure_list:
				m.combine_duties()
				item.measure_dict[m.goods_nomenclature_item_id] = m.combined_duty

			


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
		print ("Getting a full list of all nomenclature")
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
				significant_digits = significant_digits, parentage = "", children = "")
				self.full_commodity_list.append (obj)

		commodity_count = len(self.full_commodity_list)

		print ("Getting ancestry & children for nomenclature")
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
		print ("Getting preferential equivalents for MFN duties")

		"""
		Loop through all of the MFN measures that are not zero-rated, i.e. those that would be listed in the Excel spreadsheet
		and then find the FTA equivalent applicable rates. The rates in FTAs (and GSP etc) may be:
		- directly applicable to the commodity code (rare)
		- inherited down from a parent (most likely)
		- not applicable at all (i.e. there is no coverage / reduction from the base non-zero MFN rate)
		"""
		"""
		Loop through all MFN measures thare not zero
			Loop through all pref areas (say, Fiji)
			Then loop through all preferential rates in those areas

		"""
		for preferential_area in self.preferential_areas:
			print (" - Working out preferential equivalent for", preferential_area.name)
			preferential_area.preferential_rates = {}
			for mfn_measure in self.mfn_measures:
				if mfn_measure.is_zero == False:
					matched = False
					
					# First, check if there is a match against the actual commodity code for the preferential agreement
					for m2 in preferential_area.measure_list:
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
								for m2 in preferential_area.measure_list:
									if m2.goods_nomenclature_item_id in ancestry:
										m2.combine_duties()
										preferential_area.preferential_rates[mfn_measure.goods_nomenclature_item_id] = m2.combined_duty
										matched = True
										break

								break
					
					# Thirdly, look at the children
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
										if child in preferential_area.measure_dict:
											child_duties.append (preferential_area.measure_dict[child])

									child_duties2 = set(child_duties)
									if len(child_duties2) == 0:
										preferential_area.preferential_rates[mfn_measure.goods_nomenclature_item_id] = "-"
									elif len(child_duties2) == 1:
										preferential_area.preferential_rates[mfn_measure.goods_nomenclature_item_id] = child_duties[0]
									else:
										preferential_area.preferential_rates[mfn_measure.goods_nomenclature_item_id] = "Variable"

								break
