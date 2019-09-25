import xml.etree.ElementTree as ET
import xmlschema
import psycopg2
import sys
import shutil
import csv
import os
import json
from os import system, name
import re
import codecs
from datetime import datetime
from datetime import timedelta

# Custom code

from classes.progressbar							import ProgressBar
from classes.quota_order_number						import quota_order_number
from classes.quota_order_number_origin				import quota_order_number_origin
from classes.quota_order_number_origin_exclusion	import quota_order_number_origin_exclusion
from classes.quota_definition						import quota_definition
from classes.quota_association						import quota_association
from classes.geographical_area						import geographical_area
from classes.measure								import measure
from classes.fta_quota								import fta_quota

import classes.functions as fn

class application(object):
	def __init__(self):
		self.clear()
		self.debug = True

		self.BASE_DIR				= os.path.dirname(os.path.abspath(__file__))
		self.BASE_DIR				= os.path.join(self.BASE_DIR,	"..")
		self.TEMPLATE_DIR			= os.path.join(self.BASE_DIR,	"templates")
		self.CSV_DIR				= os.path.join(self.BASE_DIR,	"csv")
		self.SOURCE_DIR 			= os.path.join(self.BASE_DIR,	"source")
		self.XML_OUT_DIR			= os.path.join(self.BASE_DIR,	"xml_out")
		self.XML_REPORT_DIR			= os.path.join(self.BASE_DIR,	"xml_report")
		self.TEMP_DIR				= os.path.join(self.BASE_DIR,	"temp")
		self.TEMP_FILE				= os.path.join(self.TEMP_DIR,	"temp.xml")
		self.LOG_DIR				= os.path.join(self.BASE_DIR,	"log")
		self.IMPORT_LOG_DIR			= os.path.join(self.LOG_DIR,	"import")
		self.LOG_FILE				= os.path.join(self.LOG_DIR,	"log.csv")
		self.MERGE_DIR				= os.path.join(self.BASE_DIR,	"..")
		self.MERGE_DIR				= os.path.join(self.MERGE_DIR,	"migrate_reference_data")
		self.MERGE_DIR				= os.path.join(self.MERGE_DIR,	"xml")
		self.DUMP_DIR				= os.path.join(self.BASE_DIR,	"dump")

		self.CONFIG_DIR				= os.path.join(self.BASE_DIR, "..")
		self.CONFIG_DIR				= os.path.join(self.CONFIG_DIR, "config")
		self.CONFIG_FILE			= os.path.join(self.CONFIG_DIR, "config_common.json")
		self.CONFIG_FILE_LOCAL		= os.path.join(self.CONFIG_DIR, "config_migrate_measures_and_quotas.json")

		self.SCHEMA_DIR				= os.path.join(self.BASE_DIR, "..")
		self.SCHEMA_DIR				= os.path.join(self.SCHEMA_DIR, "xsd")

		self.SOURCE_DIR				= os.path.join(self.BASE_DIR, "source")
		self.QUOTA_DIR				= os.path.join(self.SOURCE_DIR, "quotas")
		self.BALANCE_FILE			= os.path.join(self.QUOTA_DIR, "quota_volume_master.csv")
		self.QUOTA_DESCRIPTION_FILE	= os.path.join(self.QUOTA_DIR, "quota definitions.csv")
		self.MFN_COMPONENTS_FILE	= os.path.join(self.SOURCE_DIR, "mfn_components.csv")
		self.TEMPLATE_DIR			= os.path.join(self.BASE_DIR, "templates")

		self.fta_content			= ""

		self.envelope_id            = "100000001"
		self.sequence_id            = 1
		self.content				= ""
		self.namespaces = {'oub': 'urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0', 'env': 'urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0', } # add more as needed

		self.measure_list						= []
		self.quota_definition_list				= []
		self.quota_order_number_list			= []
		self.completed_quota_definition_list	= []

		self.get_config()
		self.connect()
		self.get_minimum_sids()
		self.get_templates()
		self.transaction_id = 100000
		self.message_id = 1

		if "real_quota_excel.py" in sys.argv[0]:
			self.input_profile	= sys.argv[1]
			self.output_profile = sys.argv[2]
			self.output_file	= os.path.join(self.XML_OUT_DIR, self.output_profile)
			self.input_file		= os.path.join(self.SOURCE_DIR,  self.input_profile)

			print ("Input profile =", self.input_profile)
			print ("Output profile =", self.output_profile)
			print ("Input file =", self.input_file)
			print ("Output file =", self.output_file)
		else:
			if len(sys.argv) > 1:
				self.output_profile = sys.argv[1]
				self.CSV_DIR = os.path.join(self.CSV_DIR, self.output_profile)
				self.output_filename = os.path.join(self.XML_OUT_DIR, "quotas_" + self.output_profile.strip() + ".xml")
			else:
				print ("No profile specified")
				sys.exit()

			self.d("Writing XML data for profile " + self.output_profile, False)


	def d(self, s, include_indent = True):
		if self.debug:
			if include_indent:
				s = "- " + s
			else:
				s = "\n" + s.upper()
			print (s)



	def get_config(self):
		with open(self.CONFIG_FILE, 'r') as f:
			my_dict = json.load(f)
		
		self.p						= my_dict['p']
		self.DBASE					= my_dict['dbase_migrate_measures']
		#self.DBASE					= my_dict['dbase']
		self.d ("Creating quotas, using database " + self.DBASE, False)
		self.critical_date			= datetime.strptime(my_dict['critical_date'], '%Y-%m-%d')
		self.critical_date_plus_one	= self.critical_date + timedelta(days = 1)


	def get_templates(self):
		filename = os.path.join(self.TEMPLATE_DIR, "quota.order.number.xml")
		file = open(filename, "r") 
		self.template_quota_order_number = file.read() 

		filename = os.path.join(self.TEMPLATE_DIR, "quota.definition.xml")
		file = open(filename, "r") 
		self.template_quota_definition = file.read() 

		filename = os.path.join(self.TEMPLATE_DIR, "quota.association.xml")
		file = open(filename, "r") 
		self.template_quota_association = file.read() 

		filename = os.path.join(self.TEMPLATE_DIR, "quota.order.number.origin.xml")
		file = open(filename, "r") 
		self.template_quota_order_number_origin = file.read() 

		filename = os.path.join(self.TEMPLATE_DIR, "quota.order.number.origin.exclusion.xml")
		file = open(filename, "r") 
		self.template_quota_order_number_origin_exclusion = file.read() 

		filename = os.path.join(self.TEMPLATE_DIR, "measure.xml")
		file = open(filename, "r") 
		self.template_measure = file.read() 

		filename = os.path.join(self.TEMPLATE_DIR, "measure.component.xml")
		file = open(filename, "r") 
		self.template_measure_component = file.read() 

		filename = os.path.join(self.TEMPLATE_DIR, "measure.excluded.geographical.area.xml")
		file = open(filename, "r") 
		self.template_measure_excluded_geographical_area = file.read() 


	def write_xml(self):
		self.d("Writing XML", True)
		xml = '<?xml version="1.0" encoding="UTF-8"?>\n'
		xml += '<env:envelope xmlns="urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0" xmlns:env="urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0" id="ENV">\n'
		
		# Write the quotas
		for qon in self.quota_order_number_list:
			xml += qon.xml()

		# Write the quota associations
		for qd in self.completed_quota_definition_list:
			xml += qd.quota_association_xml()

		# Write the measures
		for qon in self.quota_order_number_list:
			xml += qon.measure_xml()

		xml += '</env:envelope>'
		file = open(self.output_filename, "w") 
		file.write(xml) 
		file.close() 

	def get_commodities_from_db(self):
		self.measure_list = []
		clause = ""
		for q in self.quota_order_number_list:
			clause += "'" + q.quota_order_number_id + "', "
		clause = clause.strip()
		clause = clause.strip(",")
		
		sql = """SELECT m.measure_sid, m.goods_nomenclature_item_id, m.measure_type_id, mc.duty_expression_id,
		mc.duty_amount, mc.monetary_unit_code, mc.measurement_unit_code, mc.measurement_unit_qualifier_code,
		ordernumber
		FROM ml.measures_real_end_dates m, goods_nomenclatures gn, measure_components mc
		WHERE gn.goods_nomenclature_item_id = m.goods_nomenclature_item_id
		AND m.measure_sid = mc.measure_sid AND gn.producline_suffix = '80'
		AND gn.validity_end_date IS NULL AND ordernumber IN (""" + clause + """)
		ORDER BY ordernumber, measure_sid"""
		
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		for row in rows:
			measure_sid						= row[0]
			goods_nomenclature_item_id		= row[1]
			measure_type_id					= row[2]
			duty_expression_id				= row[3]
			duty_amount						= row[4]
			monetary_unit_code				= fn.mstr(row[5])
			measurement_unit_code			= fn.mstr(row[6])
			measurement_unit_qualifier_code	= fn.mstr(row[7])
			quota_order_number_id			= row[8]

			measure_sid						= -1
			goods_nomenclature_item_id		= row[0]
			quota_order_number_id			= row[1]
			measure_type_id					= "122" # row[2]
			#duty_expression_id				= row[1]
			duty_amount						= row[2]
			monetary_unit_code				= fn.mstr(row[3])
			measurement_unit_code			= fn.mstr(row[4])
			measurement_unit_qualifier_code	= fn.mstr(row[5])


			m = measure(goods_nomenclature_item_id, quota_order_number_id, duty_amount, monetary_unit_code,
			measurement_unit_code, measurement_unit_qualifier_code, measure_sid)
			self.measure_list.append (m)
		
		line = ""


		file = open("commodities.txt", "w")
		old_order_number = ""
		old_comm_code = ""
		for m in self.measure_list:
			if old_order_number != m.quota_order_number_id:
				line = "\n\n" + m.quota_order_number_id + "\n======\n"
				file.write(line)
			if ((m.goods_nomenclature_item_id != old_comm_code) or (m.goods_nomenclature_item_id == old_comm_code) and (old_order_number != m.quota_order_number_id)):
				line = m.goods_nomenclature_item_id + ";\n"
				file.write(line)

			old_comm_code = m.goods_nomenclature_item_id
			old_order_number = m.quota_order_number_id
		file.close()

		file = open("duties.txt", "w") 
		old_order_number = ""
		old_comm_code = ""
		for m in self.measure_list:
			if old_order_number != m.quota_order_number_id:
				line = "\n\n" + m.quota_order_number_id + "\n======\n"
				file.write(line)
			if ((m.goods_nomenclature_item_id != old_comm_code) or (m.goods_nomenclature_item_id == old_comm_code) and (old_order_number != m.quota_order_number_id)):
				line = m.goods_nomenclature_item_id + " - " + m.duty_string() + ";\n"
				file.write(line)
			
			old_comm_code = m.goods_nomenclature_item_id
			old_order_number = m.quota_order_number_id
		file.close()

		sys.exit()


	def get_quota_descriptions(self):
		sql = """select quota_order_number_id, geographical_area_id, description
		from ml.quota_descriptions order by 1"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		self.quota_descriptions = []
		for row in rows:
			quota_order_number_id	= row[0]
			geographical_area_id	= row[1]
			description				= self.cleanse_description(row[2])
			obj = list()
			obj.append (quota_order_number_id)
			obj.append (geographical_area_id)
			obj.append (description)
			self.quota_descriptions.append (obj)


	def cleanse_description(self, s):
		s = s.replace("\n", " ")
		for i in range(1, 3):
			s = s.replace("  ", " ")
		return s


	def get_geographical_areas(self):
		self.geographical_areas = []
		my_file = os.path.join(self.SOURCE_DIR, "geographical_area_lookup.csv")
		with open(my_file) as csv_file:
			csv_reader = csv.reader(csv_file, delimiter = ",")
			for row in csv_reader:
				if (len(row) > 0):
					country_description					= row[0]
					geographical_area_id				= row[1]
					measure_generating_regulation_id	= row[2]
					obj = geographical_area(country_description, geographical_area_id, measure_generating_regulation_id)
					self.geographical_areas.append (obj)


	def get_measures_from_csv(self):
		self.d("Getting commodities from quota_commodities.csv", True)
		self.quota_order_number_list = []
		my_file = os.path.join(self.CSV_DIR, "quota_commodities.csv")
		with open(my_file) as csv_file:
			csv_reader = csv.reader(csv_file, delimiter = ",")
			for row in csv_reader:
				if (len(row) > 0):
					goods_nomenclature_item_id		= row[0]
					quota_order_number_id			= row[1]
					origin_identifier				= row[2]
					duty_amount						= row[3]
					monetary_unit_code				= row[4]
					measurement_unit_code			= row[5]
					measurement_unit_qualifier_code	= row[6]
					start_date_override				= row[7]
					end_date_override				= row[8]
					
					if (goods_nomenclature_item_id != "goods nomenclature") and (goods_nomenclature_item_id != ""):
						obj = measure(goods_nomenclature_item_id, quota_order_number_id, origin_identifier, duty_amount,
						monetary_unit_code, measurement_unit_code, measurement_unit_qualifier_code, start_date_override, end_date_override)
						self.measure_list.append(obj)

	def get_quota_order_numbers_from_csv(self):
		self.d("Getting order numbers from quota_order_numbers.csv", True)
		# Think about countries other than China
		self.quota_order_number_list = []
		my_file = os.path.join(self.CSV_DIR, "quota_order_numbers.csv")
		with open(my_file, 'r', encoding='latin-1') as csv_file:
			csv_reader = csv.reader(csv_file, delimiter = ",")
			for row in csv_reader:
				if (len(row) > 0):
					quota_order_number_id	= row[0]
					regulation_id 			= row[1]
					method					= row[2]
					measure_type_id	 		= row[3]
					origin_string			= row[4]
					origin_exclusion_string = row[5]
					validity_start_date		= row[6]
					subject					= row[7]

					try:
						status = row[8]
					except:
						status = "New"

					obj = quota_order_number(quota_order_number_id, regulation_id, method, measure_type_id, origin_string,
					origin_exclusion_string, validity_start_date, subject, status)

					self.quota_order_number_list.append(obj)


	def compare_order_number_origins(self):
		for obj in self.quota_order_number_list:
			try:
				obj.origin_list.sort(key=lambda x: x.geographical_area_id, reverse = False)
			except:
				print (obj.quota_order_number_id)
				sys.exit()

			obj.actual_origin_string = ""
			for origin in obj.origin_list:
				obj.actual_origin_string += " " + origin.geographical_area_id
			obj.actual_origin_string = obj.actual_origin_string.strip()

			match_count = 0

			my_origin = obj.origin_list
			db_match_list = []
			for db_origin in self.db_origin_list:
				if db_origin.quota_order_number_id == obj.quota_order_number_id:
					db_match_list.append (db_origin.geographical_area_id)

			db_match_list.sort()
			matched = True
			db_match_string = ""

			# First, check that all the quota order numbers match
			# We do not care about exchanging 1008 for 1011, though we shoudl stick with
			# what is already there

			# I cannot remember why this was there: now commented out
			
			"""
			if len(obj.origin_list) == 1 and obj.origin_list[0] == "1011":
				if len(db_match_list == 1):
					if db_match_list[0] == "1008":
						obj.origin_list[0] == "1008"
			"""
			
			for item in db_match_list:
				db_match_string += " " + item
				if item not in obj.actual_origin_string:
					matched = False
					match_fail = item
			

			for item in obj.origin_list:
				if item.geographical_area_id not in db_match_list:
					matched = False
					match_fail = item.geographical_area_id

			

			db_match_string = db_match_string.strip()

			if matched == False:
				if obj.quota_order_number_id[0:3] != "094":
					pass


	def get_quota_definitions_from_csv(self):
		self.d("Getting quota definitions from quota_definitions.csv", True)
		self.quota_definition_list = []
		my_file = os.path.join(self.CSV_DIR, "quota_definitions.csv")
		with open(my_file) as csv_file:
			csv_reader = csv.reader(csv_file, delimiter = ",")
			for row in csv_reader:
				if (len(row) > 0):
					quota_order_number_id			= row[0]
					measure_type					= row[1]
					quota_method					= row[2]
					validity_start_date				= row[3]
					validity_end_date				= row[4]
					length							= row[5]
					initial_volume 					= row[6]
					measurement_unit_code			= row[7]
					maximum_precision				= row[8]
					critical_state					= row[9]
					critical_threshold				= row[10]
					monetary_unit_code				= row[11]
					measurement_unit_qualifier_code	= row[12]
					blocking_period_start			= "" # row[13]
					blocking_period_end				= "" # row[14]
					origin_identifier				= row[15]

					obj = quota_definition(quota_order_number_id, measure_type, quota_method, validity_start_date, validity_end_date, length, initial_volume,
					measurement_unit_code, maximum_precision, critical_state, critical_threshold, monetary_unit_code,
					measurement_unit_qualifier_code, blocking_period_start, blocking_period_end, origin_identifier)

					self.quota_definition_list.append(obj)


	def insert_quota_order_number(self, quota_order_number_id):
		#print ("Inserting new quota order number", quota_order_number_id)
		validity_start_date = datetime.strftime(self.critical_date_plus_one, "%Y-%m-%d")
		validity_end_date		= ""
		country_name			= ""
		annual_volume			= -1
		increment				= -1
		eu_period_starts		= ""
		eu_period_ends			= ""
		interim_volume			= -1
		units					= ""
		preferential			= ""
		include_interim_period	= "N"

		obj_quota = fta_quota(country_name, quota_order_number_id, annual_volume, increment, \
		eu_period_starts, eu_period_ends, interim_volume, units, preferential, include_interim_period)
		self.quota_list.append (obj_quota)


	def get_minimum_sids(self):
		self.override_minimum_sids_node = "tariff_staging"
		with open(self.CONFIG_FILE, 'r') as f:
			my_dict = json.load(f)
		
		self.min_list = my_dict['minimum_sids'][self.override_minimum_sids_node]

		self.last_additional_code_description_period_sid	= self.larger(self.get_scalar("SELECT MAX(additional_code_description_period_sid) FROM additional_code_description_periods_oplog;"), self.min_list['additional.code.description.periods']) + 1
		self.last_additional_code_sid						= self.larger(self.get_scalar("SELECT MAX(additional_code_sid) FROM additional_codes_oplog;"), self.min_list['additional.codes']) + 1

		self.last_certificate_description_period_sid		= self.larger(self.get_scalar("SELECT MAX(certificate_description_period_sid) FROM certificate_description_periods_oplog;"), self.min_list['certificate.description.periods']) + 1
		self.last_footnote_description_period_sid			= self.larger(self.get_scalar("SELECT MAX(footnote_description_period_sid) FROM footnote_description_periods_oplog;"), self.min_list['footnote.description.periods']) + 1
		self.last_geographical_area_description_period_sid	= self.larger(self.get_scalar("SELECT MAX(geographical_area_description_period_sid) FROM geographical_area_description_periods_oplog;"), self.min_list['geographical.area.description.periods']) + 1
		self.last_geographical_area_sid						= self.larger(self.get_scalar("SELECT MAX(geographical_area_sid) FROM geographical_areas_oplog;"), self.min_list['geographical.areas']) + 1

		self.last_goods_nomenclature_sid					= self.larger(self.get_scalar("SELECT MAX(goods_nomenclature_sid) FROM goods_nomenclatures_oplog;"), self.min_list['goods.nomenclature']) + 1
		self.last_goods_nomenclature_indent_sid				= self.larger(self.get_scalar("SELECT MAX(goods_nomenclature_indent_sid) FROM goods_nomenclature_indents_oplog;"), self.min_list['goods.nomenclature.indents']) + 1
		self.last_goods_nomenclature_description_period_sid	= self.larger(self.get_scalar("SELECT MAX(goods_nomenclature_description_period_sid) FROM goods_nomenclature_description_periods_oplog;"), self.min_list['goods.nomenclature.description.periods']) + 1

		self.last_measure_sid								= self.larger(self.get_scalar("SELECT MAX(measure_sid) FROM measures_oplog;"), self.min_list['measures']) + 1
		self.last_measure_condition_sid						= self.larger(self.get_scalar("SELECT MAX(measure_condition_sid) FROM measure_conditions_oplog"), self.min_list['measure.conditions']) + 1

		self.last_quota_order_number_sid					= self.larger(self.get_scalar("SELECT MAX(quota_order_number_sid) FROM quota_order_numbers_oplog"), self.min_list['quota.order.numbers']) + 1
		self.last_quota_order_number_origin_sid				= self.larger(self.get_scalar("SELECT MAX(quota_order_number_origin_sid) FROM quota_order_number_origins_oplog"), self.min_list['quota.order.number.origins']) + 1
		self.last_quota_definition_sid						= self.larger(self.get_scalar("SELECT MAX(quota_definition_sid) FROM quota_definitions_oplog"), self.min_list['quota.definitions']) + 1
		self.last_quota_suspension_period_sid				= self.larger(self.get_scalar("SELECT MAX(quota_suspension_period_sid) FROM quota_suspension_periods_oplog"), self.min_list['quota.suspension.periods']) + 1
		self.last_quota_blocking_period_sid					= self.larger(self.get_scalar("SELECT MAX(quota_blocking_period_sid) FROM quota_blocking_periods_oplog"), self.min_list['quota.blocking.periods']) + 1

	def larger(self, a, b):
		if a > b:
			return a
		else:
			return b

	def get_scalar(self, sql):
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		#l = list(rows)
		return (rows[0][0])


	def d(self, s, include_indent = True):
		if self.debug:
			if include_indent:
				s = "- " + s
			else:
				s = "\n" + s.upper()
			print (s)

	def clear_old(self):
		# for windows
		if name == 'nt':
			_ = system('cls')
		# for mac and linux(here, os.name is 'posix')
		else:
			_ = system('clear')

	def clear(self):
		# for windows
		if name == 'nt':
			_ = system('cls')
		# for mac and linux(here, os.name is 'posix')
		else:
			#_ = system('clear')
			_ = system("printf '\33c\e[3J'")

	def connect(self):
		self.conn = psycopg2.connect("dbname=" + self.DBASE + " user=postgres password=" + self.p)


	def validate(self):
		fname = self.output_filename
		self.d("Validating the XML file against the Taric 3 schema", True)
		schema_path = os.path.join(self.SCHEMA_DIR, "envelope.xsd")
		my_schema = xmlschema.XMLSchema(schema_path)

		try:
			if my_schema.is_valid(fname):
				self.d("The file validated successfully")
				success = True
			else:
				self.d("The file did not validate")
				success = False
		except:
			self.d("The file did not validate and crashed the validator")
			success = False
		if not(success):
			my_schema.validate(fname)

	def get_all_origins_from_db(self):
		self.d("Getting origins from database", True)
		self.db_origin_list = []
		sql = """SELECT qono.quota_order_number_origin_sid, qon.quota_order_number_id,
		qon.quota_order_number_id, geographical_area_id
		FROM quota_order_number_origins qono, quota_order_numbers qon
		WHERE qon.quota_order_number_sid = qono.quota_order_number_sid
		AND qon.validity_end_date IS NULL
		AND qono.validity_end_date IS NULL ORDER BY 2, 3"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		for row in rows:
			quota_order_number_origin_sid	= row[0]
			quota_order_number_id			= row[1]
			quota_order_number_sid			= row[2]
			geographical_area_id			= row[3].strip()
			if geographical_area_id == "":
				print ("blank")
				sys.exit()

			qono = quota_order_number_origin(quota_order_number_sid, geographical_area_id, "")
			qono.quota_order_number_id = quota_order_number_id
			self.db_origin_list.append (qono)
		
		self.db_origin_exclusion_list = []
		sql = """SELECT quota_order_number_id, qon.quota_order_number_sid, excluded_geographical_area_sid,
		gad.geographical_area_id, gad.description /*, qonoe.* */
		FROM quota_order_number_origin_exclusions qonoe, quota_order_number_origins qono,
		quota_order_numbers qon, geographical_area_descriptions gad
		WHERE qono.quota_order_number_origin_sid = qonoe.quota_order_number_origin_sid
		AND qon.quota_order_number_sid = qono.quota_order_number_sid
		AND gad.geographical_area_sid = qonoe.excluded_geographical_area_sid
		ORDER BY 1, 3"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		for row in rows:
			quota_order_number_id			= row[0]
			quota_order_number_sid			= row[1]
			excluded_geographical_area_sid	= row[2]
			geographical_area_id			= row[3]
			description						= row[4]
			qonoe = quota_order_number_origin_exclusion(quota_order_number_sid, geographical_area_id) # (quota_order_number_sid, geographical_area_id, "")
			qonoe.quota_order_number_id				= quota_order_number_id
			qonoe.excluded_geographical_area_sid	= excluded_geographical_area_sid
			qonoe.description						= description
			self.db_origin_exclusion_list.append (qonoe)

	def set_config(self):
		jsonFile = open(self.CONFIG_FILE, "r")	# Open the JSON file for reading
		data = json.load(jsonFile)				# Read the JSON into the buffer
		jsonFile.close()						# Close the JSON file

		data["minimum_sids"][self.override_minimum_sids_node]["last_transaction_id"] = self.transaction_id
		data["minimum_sids"][self.override_minimum_sids_node]["measures"] = self.last_measure_sid
		data["minimum_sids"][self.override_minimum_sids_node]["measure.conditions"] = self.last_measure_condition_sid

		jsonFile = open(self.CONFIG_FILE, "w+")
		jsonFile.write(json.dumps(data, indent=4, sort_keys=True))
		jsonFile.close()

	def get_quota_associations_from_csv(self):
		csv_file	= os.path.join(self.SOURCE_DIR,	"quota_associations.csv")
		#print (csv_file)
		self.quota_associations_list = []
		with open(csv_file) as csv_file:
			csv_reader	= csv.reader(csv_file, delimiter = ",")
			for row in csv_reader:
				if (len(row) > 0):
					main_quota_order_number_id	= row[0]
					sub_quota_order_number_id	= row[1]
					relation_type				= row[2]
					coefficient					= row[3]
					qa = quota_association(main_quota_order_number_id, sub_quota_order_number_id, relation_type, coefficient)
					self.quota_associations_list.append(qa)

	def get_quota_associations(self):
		self.quota_associations_list = []
		sql = """select distinct qd_main.quota_order_number_id as main_quota_order_number_id,
		qd_sub.quota_order_number_id as sub_quota_order_number_id, 
		relation_type, coefficient
		from quota_associations qa, quota_definitions qd_main, quota_definitions qd_sub
		where qd_main.quota_definition_sid = qa.main_quota_definition_sid
		and qd_sub.quota_definition_sid = qa.sub_quota_definition_sid
		and qd_main.validity_start_date >= '2018-01-01'
		and qd_sub.validity_start_date >= '2018-01-01'
		order by 1, 2"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		for row in rows:
			main_quota_order_number_id	= row[0]
			sub_quota_order_number_id	= row[1]
			relation_type				= row[2]
			coefficient					= row[3]
			qa = quota_association(main_quota_order_number_id, sub_quota_order_number_id, relation_type, coefficient)
			self.quota_associations_list.append(qa)

		# Then add Bosnia * 2
		self.add_quota_association("092017", "092190", "EQ", 1.81)
		self.add_quota_association("092017", "092191", "EQ", 1.67)


	def add_quota_association(self, main_quota_order_number_id, sub_quota_order_number_id, relation_type, coefficient):
		qa = quota_association(main_quota_order_number_id, sub_quota_order_number_id, relation_type, coefficient)
		self.quota_associations_list.append(qa)