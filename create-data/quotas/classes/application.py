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
from classes.quota_order_number_origin				import quota_order_number_origin
from classes.quota_order_number_origin_exclusion	import quota_order_number_origin_exclusion
from classes.quota_definition						import quota_definition
from classes.quota_association						import quota_association
from classes.geographical_area						import geographical_area
from classes.measure								import measure
from classes.quota_order_number								import quota_order_number
from classes.measurement_unit						import measurement_unit

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
		self.quota_order_number_list			= []
		self.completed_quota_definition_list	= []

		self.get_config()
		self.connect()
		self.get_minimum_sids()
		self.get_templates()
		self.message_id = 1
		self.origins_added = []

		self.input_profile	= sys.argv[1]
		self.output_profile = sys.argv[2]
		self.output_file	= os.path.join(self.XML_OUT_DIR, self.output_profile)
		self.input_file		= os.path.join(self.SOURCE_DIR,  self.input_profile)

		if "safeguard" in self.input_profile:
			self.is_safeguard = True
		else:
			self.is_safeguard = False

		print ("Input profile =", self.input_profile)
		print ("Output profile =", self.output_profile)
		print ("Input file =", self.input_file)
		print ("Output file =", self.output_file)


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
		self.DBASE					= my_dict['dbase']
		self.d ("Creating quotas, using database " + self.DBASE, False)
		self.critical_date					= datetime.strptime(my_dict['critical_date'], '%Y-%m-%d')
		self.critical_date_plus_one			= self.critical_date + timedelta(days = 1)
		self.critical_date_plus_one_string	= datetime.strftime(self.critical_date_plus_one, '%Y-%m-%d')


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

		filename = os.path.join(self.TEMPLATE_DIR, "measure.condition.xml")
		file = open(filename, "r") 
		self.template_measure_condition = file.read() 

		filename = os.path.join(self.TEMPLATE_DIR, "measure.excluded.geographical.area.xml")
		file = open(filename, "r") 
		self.template_measure_excluded_geographical_area = file.read() 

		filename = os.path.join(self.TEMPLATE_DIR, "footnote.association.measure.xml")
		file = open(filename, "r") 
		self.template_footnote_association_measure = file.read() 


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




	def insert_quota_order_number(self, quota_order_number_id, measure_type_id = "122"):
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

		obj_quota = quota_order_number(country_name, measure_type_id, quota_order_number_id, annual_volume, increment, \
		eu_period_starts, eu_period_ends, interim_volume, units, preferential, include_interim_period)
		self.quota_list.append (obj_quota)


	def get_measurement_units(self):
		self.measurement_units = []
		my_file = os.path.join(self.SOURCE_DIR, "measurement_units.csv")
		with open(my_file) as csv_file:
			csv_reader = csv.reader(csv_file, delimiter = ",")
			for row in csv_reader:
				if (len(row) > 0):
					identifer						= row[0]
					measurement_unit_code			= row[1]
					measurement_unit_qualifier_code	= row[2]
					obj = measurement_unit(identifer, measurement_unit_code, measurement_unit_qualifier_code)
					self.measurement_units.append (obj)


	def get_minimum_sids(self):
		with open(self.CONFIG_FILE, 'r') as f:
			my_dict = json.load(f)
		
		self.min_list = my_dict['minimum_sids'][self.DBASE]

		self.transaction_id									= self.min_list['last_transaction_id'] + 1

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



	def set_config(self):
		jsonFile = open(self.CONFIG_FILE, "r")	# Open the JSON file for reading
		data = json.load(jsonFile)				# Read the JSON into the buffer
		jsonFile.close()						# Close the JSON file

		data["minimum_sids"][self.DBASE]["last_transaction_id"] = self.transaction_id
		data["minimum_sids"][self.DBASE]["measures"] = self.last_measure_sid
		data["minimum_sids"][self.DBASE]["measure.conditions"] = self.last_measure_condition_sid
		data["minimum_sids"][self.DBASE]["quota.definitions"] = self.last_quota_definition_sid
		data["minimum_sids"][self.DBASE]["quota.order.numbers"] = self.last_quota_order_number_sid
		data["minimum_sids"][self.DBASE]["quota.order.number.origins"] = self.last_quota_order_number_origin_sid

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
		self.add_quota_association("092017", "092018", "EQ", 1.67)
		self.add_quota_association("092017", "092020", "EQ", 1.81)

		# Then add Macedonia * 2
		self.add_quota_association("092021", "092022", "EQ", 1.67)
		self.add_quota_association("092021", "092023", "EQ", 1.81)


	def add_quota_association(self, main_quota_order_number_id, sub_quota_order_number_id, relation_type, coefficient):
		qa = quota_association(main_quota_order_number_id, sub_quota_order_number_id, relation_type, coefficient)
		self.quota_associations_list.append(qa)