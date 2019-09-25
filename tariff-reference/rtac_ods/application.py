import psycopg2
import sys
import os
from os import system, name 
import csv
import json

import functions
from seasonal import seasonal
from special import special
from friendly import friendly
from measure_condition import measure_condition
from duty import duty
from category import category
import xlsxwriter

class application(object):
	def __init__(self):
		self.authoriseduse_list		= []
		self.seasonal_list			= []
		self.special_list			= []
		self.latin_phrases			= []
		self.section_chapter_list	= []
		self.country_profile		= ""
		self.debug					= False
		self.suppress_duties		= False

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
		self.EXCEL_DIR			= os.path.join(self.OUTPUT_DIR,	"excel")
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
			
		# Define the parameters - last chapter
		try:
			self.country_profile = sys.argv[4]
		except:
			self.country_profile = ""

		self.get_config()
		self.get_latin_terms()

		if (self.document_type != "classification" and self.document_type != "schedule"):
			self.document_type = "schedule"

		self.create_mfn_workbook()
		self.create_rta_workbook()
		self.get_friendly_names()
		self.get_categorisation_data()

		self.clear()


	def close_mfn_workbook(self):
		self.workbook_mfn.close()


	def close_rta_workbook(self):
		return
		# Row 1 of instructions
		self.rta_excel_row_count += 2
		self.rta_write_cell('A', self.rta_excel_row_count, 'Instructions', self.bold)

		# Row 2 of instructions
		self.rta_excel_row_count += 1
		self.rta_write_cell('A', self.rta_excel_row_count, 'Column No.', self.bold)
		self.rta_write_cell('B', self.rta_excel_row_count, 'Column Name', self.bold)
		self.rta_write_cell('C', self.rta_excel_row_count, 'Details', self.bold)

		# Row 3 of instructions
		self.rta_excel_row_count += 1
		self.rta_write_cell('A', self.rta_excel_row_count, '(1)', self.bold)
		self.rta_write_cell('B', self.rta_excel_row_count, 'Tariff Line', self.wrap)
		self.rta_write_cell('C', self.rta_excel_row_count, 'Detailed breakdown of national customs nomenclature (HS codes with, e.g. 8, 10 or more digits)', self.nowrap)

		# Row 4 of instructions
		self.rta_excel_row_count += 1
		self.rta_write_cell('A', self.rta_excel_row_count, '(2)', self.bold)
		self.rta_write_cell('B', self.rta_excel_row_count, 'Description', self.wrap)
		self.rta_write_cell('C', self.rta_excel_row_count, 'Product Description - in one of the three standard languages of WTO (English, Spanish or French)', self.nowrap)

		# Row 5 of instructions
		self.rta_excel_row_count += 1
		self.rta_write_cell('A', self.rta_excel_row_count, '(3)', self.bold)
		self.rta_write_cell('B', self.rta_excel_row_count, 'Year', self.wrap)
		self.rta_write_cell('C', self.rta_excel_row_count, 'Year of entry into force of the agreement.  Please also provide Ad valorem equivalents (AVE)s for non-ad valorem duties', self.nowrap)

		# Row 6 of instructions
		self.rta_excel_row_count += 1
		self.rta_write_cell('A', self.rta_excel_row_count, '(4)', self.bold)
		self.rta_write_cell('B', self.rta_excel_row_count, 'Fill in the first year of liberalization and also fill in duty rate applied to this tariff item', self.wrap)
		self.rta_write_cell('C', self.rta_excel_row_count, '', self.nowrap)

		# Row 7 of instructions
		self.rta_excel_row_count += 1
		self.rta_write_cell('A', self.rta_excel_row_count, '(5)', self.bold)
		self.rta_write_cell('B', self.rta_excel_row_count, 'Please fill in years and duty rates till end of the implementation period', self.wrap)
		self.rta_write_cell('C', self.rta_excel_row_count, '', self.nowrap)

		# Row 8 of instructions
		self.rta_excel_row_count += 1
		self.rta_write_cell('A', self.rta_excel_row_count, '(6)', self.bold)
		self.rta_write_cell('B', self.rta_excel_row_count, '', self.wrap)
		self.rta_write_cell('C', self.rta_excel_row_count, '', self.nowrap)

		# Row 9 of instructions
		self.rta_excel_row_count += 1
		self.rta_write_cell('A', self.rta_excel_row_count, '(7)', self.bold)
		self.rta_write_cell('B', self.rta_excel_row_count, '', self.wrap)
		self.rta_write_cell('C', self.rta_excel_row_count, '', self.nowrap)

		# Row 10 of instructions
		self.rta_excel_row_count += 1
		self.rta_write_cell('A', self.rta_excel_row_count, '(8)', self.bold)
		self.rta_write_cell('B', self.rta_excel_row_count, '', self.wrap)
		self.rta_write_cell('C', self.rta_excel_row_count, '', self.nowrap)

		# Row 11 of instructions
		self.rta_excel_row_count += 1
		self.rta_write_cell('A', self.rta_excel_row_count, '(9)', self.bold)
		self.rta_write_cell('B', self.rta_excel_row_count, 'Remarks', self.wrap)
		self.rta_write_cell('C', self.rta_excel_row_count, 'Please provide additional information, e.g., Tariff rate quotas (TRQ)s, categories for the tariff liberalization scheme, etc.', self.nowrap)

		# Row 12 of instructions
		self.rta_excel_row_count += 1
		self.rta_write_cell('A', self.rta_excel_row_count, '(10)', self.bold)
		self.rta_write_cell('B', self.rta_excel_row_count, 'Year 1', self.wrap)
		self.rta_write_cell('C', self.rta_excel_row_count, '', self.nowrap)

		# Row 13 of instructions
		self.rta_excel_row_count += 1
		self.rta_write_cell('A', self.rta_excel_row_count, '(11)', self.bold)
		self.rta_write_cell('B', self.rta_excel_row_count, 'Year 2', self.wrap)
		self.rta_write_cell('C', self.rta_excel_row_count, '', self.nowrap)

		# Row 14 of instructions
		self.rta_excel_row_count += 1
		self.rta_write_cell('A', self.rta_excel_row_count, '(12)', self.bold)
		self.rta_write_cell('B', self.rta_excel_row_count, 'Year 3', self.wrap)
		self.rta_write_cell('C', self.rta_excel_row_count, '', self.nowrap)
		# Close the document (which saves it)
		self.workbook_rta.close()


	def create_rta_workbook(self):
		return
		self.rta_filename		= os.path.join(self.EXCEL_DIR, 	"fiji.xlsx")
		self.workbook_rta		= xlsxwriter.Workbook(self.rta_filename)
		self.worksheet_rta		= self.workbook_rta.add_worksheet()

		self.worksheet_rta.set_column('A:A', 20)
		self.worksheet_rta.set_column('B:B', 70)
		self.worksheet_rta.set_column('C:C', 40, None, {'hidden': 0})
		self.worksheet_rta.set_column('D:D', 40, None, {'hidden': 0})
		self.worksheet_rta.set_column('E:E', 40)
		self.worksheet_rta.set_column('F:F', 10, None, {'hidden': 1})
		self.worksheet_rta.set_column('G:G', 10, None, {'hidden': 1})
		self.worksheet_rta.set_column('H:H', 10, None, {'hidden': 1})
		self.worksheet_rta.freeze_panes(7, 0)

		self.country_profile = "Fiji"

		
		# Cell formats for the RTA document
		self.bold = self.workbook_rta.add_format({'bold': True, 'font_name':'Verdana'})
		self.title = self.workbook_rta.add_format({'bold': True, 'font_size': 20, 'font_name':'Verdana'})
		self.wrap = self.workbook_rta.add_format({'text_wrap': True, 'align': 'left', 'valign': 'top', 'font_name':'Verdana'})
		self.nowrap = self.workbook_rta.add_format({'text_wrap': False, 'align': 'left', 'valign': 'top', 'font_name':'Verdana'})
		self.center = self.workbook_rta.add_format({'text_wrap': False, 'align': 'center', 'valign': 'top', 'font_name':'Verdana'})
		self.boldcenter = self.workbook_rta.add_format({'bold': True, 'text_wrap': False, 'align': 'center', 'valign': 'top', 'font_name':'Verdana'})
		self.indent_formats = []
		for i in range(0, 14):
			tmp = self.workbook_rta.add_format({'text_wrap': True, 'align': 'left', 'valign': 'top', 'indent': i * 2, 'font_name':'Verdana'})
			self.indent_formats.append(tmp)

		self.superscript	= self.workbook_rta.add_format({'font_script': 1})
		self.subscript		= self.workbook_rta.add_format({'font_script': 2})

		self.worksheet_rta.write('A1', 'Tariff Schedule and Import Data', self.title)
		self.worksheet_rta.write('A2', 'RTA: ' + self.rta_agreement_name, self.bold)
		self.worksheet_rta.write('A3', 'Party to the Agreement: Reporter', self.bold)

		self.worksheet_rta.write('A5', 'Tariff line', self.boldcenter)
		self.worksheet_rta.write('B5', 'Description', self.boldcenter)
		self.worksheet_rta.write('C5', 'MFN applied rate', self.boldcenter)
		self.worksheet_rta.write('D5', "First year of liberalisation (" + self.country_profile + ")", self.boldcenter) # self.app.country_profile_formatted, self.boldcenter)
		self.worksheet_rta.write('E5', '..', self.boldcenter)
		self.worksheet_rta.write('F5', '..', self.boldcenter)

		self.worksheet_rta.write('A6', '(1)', self.center)
		self.worksheet_rta.write('B6', '(2)', self.center)
		self.worksheet_rta.write('C6', '(3)', self.center)		
		self.worksheet_rta.write('D6', '(4)', self.center)		
		self.worksheet_rta.write('E6', '(5)', self.center)		
		self.worksheet_rta.write('F6', '(6)', self.center)		



	def create_mfn_workbook(self):
		self.mfn_filename		= os.path.join(self.EXCEL_DIR, 	"mfn_ods.xlsx")
		self.workbook_mfn		= xlsxwriter.Workbook(self.mfn_filename)
		self.worksheet_mfn		= self.workbook_mfn.add_worksheet()

		self.h1_format			= self.workbook_mfn.add_format({'color': 'black', 'bold': True, 'text_wrap': True, 'valign': 'top', 'bg_color': '#f0f0f0'})
		self.h2_format			= self.workbook_mfn.add_format({'color': 'gray', 'bold': True, 'text_wrap': True, 'valign': 'top', 'bg_color': '#f0f0f0'})
		self.text_format		= self.workbook_mfn.add_format({'color': 'black', 'bold': False, 'text_wrap': True, 'valign': 'top'})
		self.centered_format	= self.workbook_mfn.add_format({'color': 'black', 'bold': False, 'text_wrap': True, 'valign': 'top', 'align': 'center'})
		self.right_format		= self.workbook_mfn.add_format({'color': 'black', 'bold': False, 'text_wrap': True, 'valign': 'top', 'align': 'right'})

		self.worksheet_mfn.set_column(0, 10, 20)
		self.worksheet_mfn.set_column(2, 2, 75)

		self.worksheet_mfn.freeze_panes(2, 0)

		self.worksheet_mfn.write("A1", "Category of products", self.h1_format)
		self.worksheet_mfn.write("B1", "CN8 tariff code - 2019 nomenclature", self.h1_format)
		self.worksheet_mfn.write("C1", "Product description", self.h1_format)
		self.worksheet_mfn.write("D1", "Does the product have a tariff quota rate", self.h1_format)
		self.worksheet_mfn.write("E1", "If an MFN rate is applied - what is that rate in specific terms or in ad valorem", self.h1_format)
		self.worksheet_mfn.write("F1", "Chile", self.h1_format)
		self.worksheet_mfn.write("G1", "Chile", self.h1_format)
		self.worksheet_mfn.write("H1", "ESA Countries", self.h1_format)
		self.worksheet_mfn.write("I1", "ESA Countries", self.h1_format)
		self.worksheet_mfn.write("J1", "Faroe Islands", self.h1_format)
		self.worksheet_mfn.write("K1", "Faroe Islands", self.h1_format)

		self.worksheet_mfn.write("A2", "Category", self.h2_format)
		self.worksheet_mfn.write("B2", "Commodity code", self.h2_format)
		self.worksheet_mfn.write("C2", "Description", self.h2_format)
		self.worksheet_mfn.write("D2", "Tariff quota rate", self.h2_format)
		self.worksheet_mfn.write("E2", "Most-favoured-nation (MFN) rate", self.h2_format)
		self.worksheet_mfn.write("F2", "Preferential Tariff", self.h2_format)
		self.worksheet_mfn.write("G2", "Preferential TRQ", self.h2_format)
		self.worksheet_mfn.write("H2", "Preferential Tariff", self.h2_format)
		self.worksheet_mfn.write("I2", "Preferential TRQ", self.h2_format)
		self.worksheet_mfn.write("J2", "Preferential Tariff", self.h2_format)
		self.worksheet_mfn.write("K2", "Preferential TRQ", self.h2_format)

		self.mfn_excel_row_count = 3
		self.rta_excel_row_count = 7

		#self.close_mfn_workbook()
	
	def excel_write(self, column, content, fmt):
		cl = column + str(self.current_row)
		self.worksheet_rta.write(cl, content, fmt)

	def rta_write_cell(self, column_letter, row_number, content, fmt):
		cl = column_letter + str(row_number)
		self.worksheet_rta.write(cl, content, fmt)

		
	def excel_write(self, column, content, fmt):
		cl = column + str(self.current_row)
		self.worksheet_rta.write(cl, content, fmt)


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
		self.DBASE	= "tariff_staging"
		#self.DBASE	= "tariff_eu"

		self.p		= my_dict['p']

		# Get local config items
		with open(self.CONFIG_FILE_LOCAL, 'r') as f:
			my_dict = json.load(f)

		self.all_country_profiles = my_dict['country_profiles']

		# Connect to the database
		self.connect()

		if self.country_profile != "":
			try:
				self.country_codes		= self.all_country_profiles[self.country_profile]["country_codes"]
				self.geo_ids = self.list_to_sql(self.country_codes)
				self.rta_agreement_name	= self.all_country_profiles[self.country_profile]["agreement_name"]
			except:
				print ("Country profile does not exist")
				sys.exit()

			# Get exclusions
			try:
				self.exclusion_check = self.all_country_profiles[self.country_profile]["exclusion_check"]
			except:
				self.exclusion_check = ""
				pass


			self.get_duties("preferences")


	def get_duties(self, instrument_type):
		print (" - Getting duties for " + instrument_type + " for " + self.country_profile)

		###############################################################
		# Work out which measures to capture
		###############################################################
		if instrument_type == "preferences":
			measure_type_list = "'142', '145'"
		else:
			measure_type_list = "'143', '146'"

		###############################################################
		# Before getting the duties, get the measure component conditions
		# These are used in adding in SIV components whenever the duty is no present
		# due to the fact that there are SIVs applied via measure components

		# This needs to be derived from the EU database, as the conditions will  ot exist in
		# in the UK database

		print (" - Getting measure conditions")
		self.measure_condition_list = []
		sql = """
		SELECT DISTINCT mc.measure_sid, mcc.duty_amount
		FROM measure_conditions mc, measure_condition_components mcc, measures m
		WHERE mc.measure_condition_sid = mcc.measure_condition_sid
		AND m.measure_sid = mc.measure_sid AND condition_code = 'V' AND mcc.duty_expression_id = '01'
		AND m.measure_type_id IN (""" + measure_type_list + """)
		AND m.geographical_area_id IN (""" + self.geo_ids + """)
		AND m.validity_start_date < '2019-12-31' AND m.validity_end_date >= '2018-01-01'
		ORDER BY measure_sid;
		"""

		cur = self.conn_eu.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		for row in rows:
			measure_sid				= row[0]
			condition_duty_amount	= row[1]
			mc = measure_condition(0, measure_sid, "V", 1, condition_duty_amount, "", "", "", "", "", "")
			self.measure_condition_list.append (mc)



		# Now get the country exclusions
		# This should be taken from the UK database - not sure if there will be any
		# relevant exclusions in reality
		exclusion_list = []
		if self.exclusion_check != "":
			sql = """SELECT m.measure_sid FROM measure_excluded_geographical_areas mega, ml.v5_2019 m
			WHERE m.measure_sid = mega.measure_sid
			AND excluded_geographical_area = '""" + self.exclusion_check + """'
			ORDER BY validity_start_date DESC"""
			cur = self.conn_uk.cursor()
			cur.execute(sql)
			rows = cur.fetchall()
			for row in rows:
				measure_sid = row[0]
				exclusion_list.append (measure_sid)

		# Get the duties (i.e. the measure components) - these will all be used unless they
		# are excluded due to geo. exclusion

		# This data must come from the UK database
		sql = """
		SELECT DISTINCT m.goods_nomenclature_item_id, m.additional_code_type_id, m.additional_code_id,
		m.measure_type_id, mc.duty_expression_id, mc.duty_amount, mc.monetary_unit_code,
		mc.measurement_unit_code, mc.measurement_unit_qualifier_code, m.measure_sid, m.ordernumber,
		m.validity_start_date, m.validity_end_date, m.geographical_area_id, m.reduction_indicator
		FROM goods_nomenclatures gn, ml.v5_2019 m LEFT OUTER JOIN measure_components mc ON m.measure_sid = mc.measure_sid
		WHERE (m.measure_type_id IN (""" + measure_type_list + """)
		AND m.geographical_area_id IN (""" + self.geo_ids + """)
		AND m.goods_nomenclature_item_id = gn.goods_nomenclature_item_id
		AND gn.validity_end_date IS NULL AND gn.producline_suffix = '80'
		) ORDER BY m.goods_nomenclature_item_id, validity_start_date DESC, mc.duty_expression_id
		"""

		cur = self.conn_uk.cursor()
		cur.execute(sql)
		rows = cur.fetchall()

		# Do a pass through the duties table and create a full duty expression
		# Duty is a mnemonic for measure component
		temp_commodity_list				= []
		temp_quota_order_number_list	= []
		temp_measure_list				= []
		self.duty_list					= []
		self.measure_list				= []
		self.commodity_list				= []
		self.quota_order_number_list	= []

		for row in rows:
			measure_sid						= row[9]
			if measure_sid not in (exclusion_list):
				commodity_code					= mstr(row[0])
				additional_code_type_id			= mstr(row[1])
				additional_code_id				= mstr(row[2])
				measure_type_id					= mstr(row[3])
				duty_expression_id				= row[4]
				duty_amount						= row[5]
				monetary_unit_code				= mstr(row[6])
				monetary_unit_code				= monetary_unit_code.replace("EUR", "€")
				measurement_unit_code			= mstr(row[7])
				measurement_unit_qualifier_code = mstr(row[8])
				quota_order_number_id			= mstr(row[10])
				validity_start_date				= row[11]
				validity_end_date				= row[12]
				geographical_area_id			= mstr(row[13])
				reduction_indicator				= row[14]

				# Hypothesis would be that the only reason why the duty amount is None is when
				# there is a "V" code attached to the measure
				#if ((duty_amount is None) and (duty_expression_id == "01")):
				if duty_amount is None and duty_expression_id is None:
					is_eps = True
					for mc in self.measure_condition_list:
						if mc.measure_sid == measure_sid:
							duty_expression_id = "01"
							duty_amount = mc.condition_duty_amount
				else:
					is_eps = False

				# Create a new duty object (i.e. measure component)
				obj_duty = duty_fta(commodity_code, additional_code_type_id, additional_code_id, measure_type_id, duty_expression_id,
				duty_amount, monetary_unit_code, measurement_unit_code, measurement_unit_qualifier_code,
				measure_sid, quota_order_number_id, geographical_area_id, validity_start_date, validity_end_date, reduction_indicator, is_eps)
				# Add the object to the duty_list
				self.duty_list.append(obj_duty)

				
				if measure_sid not in temp_measure_list:
					obj_measure = measure(measure_sid, commodity_code, quota_order_number_id, validity_start_date, validity_end_date, geographical_area_id, reduction_indicator)
					self.measure_list.append(obj_measure)
					temp_measure_list.append(measure_sid)

				if commodity_code not in temp_commodity_list:
					obj_commodity = commodity_fta(commodity_code)
					self.commodity_list.append(obj_commodity)
					temp_commodity_list.append(commodity_code)

				if quota_order_number_id not in temp_quota_order_number_list:
					if quota_order_number_id != "":
						obj_quota_order_number = quota_order_number(quota_order_number_id)
						self.quota_order_number_list.append(obj_quota_order_number)
						temp_quota_order_number_list.append(quota_order_number_id)
				

		# Loop through the measures and assign duties to them
		for m in self.measure_list:
			for d in self.duty_list:
				if m.measure_sid == d.measure_sid:
					m.duty_list.append(d)

		#  Loop through the commodities and assign measures to them
		for c in self.commodity_list:
			for m in self.measure_list:
				if m.commodity_code == c.commodity_code:
					c.measure_list.append(m)
		
		# Combine duties into a string
		for m in self.measure_list:
			m.combine_duties()

		# Finally, form the measures into a consolidated string
		for c in self.commodity_list:
			c.resolve_measures()

	def connect(self):
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
		# in place before No Deal Brexi

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

	def get_friendly_names(self):
		# This function gets friendly names for commodities
		self.friendly_names = []

		filename = os.path.join(self.SOURCE_DIR, "tariffnumber_commodities.csv")
		with open(filename, "r") as f:
			reader = csv.reader(f, delimiter=',', quotechar='"')
			temp = list(reader)

		for row in temp:
			commodity_code	= row[0]
			description		= row[1]
			friendly_name	= friendly(commodity_code, description)
			self.friendly_names.append(friendly_name)

	def get_categorisation_data(self):
		# This function gets categorisation data for commodities
		self.categories = []

		filename = os.path.join(self.SOURCE_DIR, "data for tariff categorisation.csv")
		with open(filename, "r") as f:
			reader = csv.reader(f, delimiter=',', quotechar='"')
			temp = list(reader)

		for row in temp:
			stem		= row[0]
			description	= row[1]
			c			= category(stem, description)
			self.categories.append(c)

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

	def list_to_sql(self, my_list):
		s = ""
		if my_list != "":
			for o in my_list:
				s += "'" + o + "', "
			s = s.strip()
			s = s.strip(",")
		return (s)


def mstr(x):
	if x is None:
		return ""
	else:
		return str(x)