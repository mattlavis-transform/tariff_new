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
import xlrd

# Custom code

from classes.condition_profile import condition_profile
from classes.progressbar import ProgressBar
from classes.quota_order_number import quota_order_number
from classes.quota_order_number_origin import quota_order_number_origin
from classes.quota_definition import quota_definition
from classes.measure import measure
from classes.geographical_area import geographical_area
from classes.goods_nomenclature import goods_nomenclature
from classes.measure_excluded_geographical_area import measure_excluded_geographical_area
from classes.eea_item import eea_item
from classes.eea_measure import eea_measure
from classes.eea_measure_component import eea_measure_component

import classes.functions as fn

class application(object):
	def __init__(self):
		self.clear()

		self.BASE_DIR				= os.path.dirname(os.path.abspath(__file__))
		self.BASE_DIR				= os.path.join(self.BASE_DIR,	"..")
		self.SCHEMA_DIR				= os.path.join(self.BASE_DIR,	"xsd")
		self.TEMPLATE_DIR			= os.path.join(self.BASE_DIR,	"templates")
		self.CSV_DIR				= os.path.join(self.BASE_DIR,	"csv")
		self.EEA_DIR				= os.path.join(self.BASE_DIR,	"eea")
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

		self.envelope_id    = "100000001"
		self.sequence_id	= 1
		self.content		= ""
		self.namespaces = {'oub': 'urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0', 'env': 'urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0', } # add more as needed

		self.measure_list				= []
		self.quota_definition_list		= []
		self.quota_order_number_list	= []

		self.get_config()
		self.connect()
		self.get_minimum_sids()
		self.get_templates()
		self.message_id = 1
		self.debug = True

		
		if len(sys.argv) > 1:
			self.output_profile = sys.argv[1]
			self.output_filename = os.path.join(self.XML_OUT_DIR, self.output_profile.strip() + ".xml")
		else:
			print ("No profile specified")
			sys.exit()
		
	def get_config(self):
		with open(self.CONFIG_FILE, 'r') as f:
			my_dict = json.load(f)

		critical_date 						= my_dict['critical_date']
		self.critical_date					= datetime.strptime(critical_date, '%Y-%m-%d')
		self.critical_date_plus_one			= self.critical_date + timedelta(days = 1)
		self.critical_date_plus_one_string	= datetime.strftime(self.critical_date_plus_one, '%Y-%m-%d')

		self.DBASE			= my_dict['dbase']
		self.p				= my_dict['p']
		self.transaction_id	= my_dict["minimum_sids"][self.DBASE]["last_transaction_id"]

	def get_templates(self):
		filename = os.path.join(self.TEMPLATE_DIR, "quota.order.number.xml")
		file = open(filename, "r") 
		self.template_quota_order_number = file.read() 

		filename = os.path.join(self.TEMPLATE_DIR, "quota.definition.xml")
		file = open(filename, "r") 
		self.template_quota_definition = file.read() 

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

		filename = os.path.join(self.TEMPLATE_DIR, "envelope.xml")
		file = open(filename, "r") 
		self.template_envelope = file.read() 


	def write_xml(self):
		file = open(self.output_filename, "w+")
		print ("Writing XML")
		xml = '<?xml version="1.0" encoding="UTF-8"?>\n'
		xml += '<env:envelope xmlns="urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0" xmlns:env="urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0" id="ENV">\n'
		file.write(xml)
		# Write the measures
		for m in self.measure_list:
			if m.goods_nomenclature_item_id in self.valid_commodity_list:
				xml = m.xml()
				file.write(xml)

		xml = '</env:envelope>'
		file.write(xml)
		file.close() 
		print ("XML Write Complete")


	def get_measures_from_csv(self):
		self.measure_list = []
		self.commodity_list = []
		my_file = os.path.join(self.CSV_DIR, self.output_profile + ".csv")
		with open(my_file) as csv_file:
			csv_reader = csv.reader(csv_file, delimiter = ",")
			next(csv_reader)
			for row in csv_reader:
				if (len(row) > 0):
					goods_nomenclature_item_id	= row[0]
					regulation_id				= row[1]
					geographical_area_id		= row[2]
					measure_type_id				= row[3]
					ad_valorem					= row[4]
					specific1					= row[5]
					ceiling						= row[6]
					minimum						= row[7]
					specific2					= row[8]
					date_from					= row[9]
					date_to						= row[10]
					try:
						exclusions				= row[11]
					except:
						exclusions				= ""

					if (goods_nomenclature_item_id != "goods nomenclature") and (goods_nomenclature_item_id != ""):
						obj = measure(goods_nomenclature_item_id, regulation_id, geographical_area_id, measure_type_id, ad_valorem, \
						specific1, ceiling, minimum, specific2, date_from, date_to, exclusions)
						self.measure_list.append(obj)
						self.commodity_list.append(goods_nomenclature_item_id)

	def associate_exclusions(self):
		if self.output_profile == "2020":
			for m in self.measure_list:
				for x in self.exclusions_list:
					if x.goods_nomenclature_item_id == m.goods_nomenclature_item_id:
						m.measure_excluded_geographical_area_list.append (x)

	def get_quota_order_numbers_from_csv(self):
		self.quota_order_number_list = []
		my_file = os.path.join(self.CSV_DIR, "quota_order_numbers.csv")
		with open(my_file) as csv_file:
			csv_reader = csv.reader(csv_file, delimiter = ",")
			for row in csv_reader:
				if (len(row) > 0):
					quota_order_number_id	= row[0]
					regulation_id 			= row[1]
					measure_type_id	 		= row[2]
					origin_string			= row[3]
					origin_exclusion_string = row[4]
					validity_start_date		= row[5]
					subject					= row[6]
					try:
						status = row[7]
					except:
						status = "New"

					obj = quota_order_number(quota_order_number_id, regulation_id, measure_type_id, origin_string,
					origin_exclusion_string, validity_start_date, subject, status)

					self.quota_order_number_list.append(obj)

	def get_quota_definitions_from_csv(self):
		self.quota_definition_list = []
		my_file = os.path.join(self.CSV_DIR, "quota_definitions.csv")
		with open(my_file) as csv_file:
			csv_reader = csv.reader(csv_file, delimiter = ",")
			for row in csv_reader:
				if (len(row) > 0):
					quota_order_number_id			= row[0]
					validity_start_date				= row[1]
					validity_end_date				= row[2]
					initial_volume 					= row[3]
					measurement_unit_code			= row[4]
					maximum_precision				= row[5]
					critical_state					= row[6]
					critical_threshold				= row[7]
					monetary_unit_code				= row[8]
					measurement_unit_qualifier_code	= row[9]
					blocking_period_start			= row[10]
					blocking_period_end				= row[11]

					obj = quota_definition(quota_order_number_id, validity_start_date, validity_end_date, initial_volume,
					measurement_unit_code, maximum_precision, critical_state, critical_threshold, monetary_unit_code,
					measurement_unit_qualifier_code, blocking_period_start, blocking_period_end)

					self.quota_definition_list.append(obj)

	def get_geographical_areas(self):
		self.d("Getting geographical areas", False)
		sql = """SELECT geographical_area_id, geographical_area_sid FROM geographical_areas;"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		if len(rows) > 0:
			self.geographical_area_list = []
			for rw in rows:
				geographical_area_id	= rw[0]
				geographical_area_sid	= rw[1]
				g = geographical_area(geographical_area_id, geographical_area_sid)
				self.geographical_area_list.append (g)


	def get_minimum_sids(self):
		with open(self.CONFIG_FILE, 'r') as f:
			my_dict = json.load(f)
		
		self.min_list = my_dict['minimum_sids'][self.DBASE]

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

		#self.transaction_id									= self.get_scalar("SELECT MAX(last_transaction_id) FROM ml.config") + 1


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


	def get_master_nomenclature_list(self):
		print ("Simplifying data for " + self.output_profile.title())
		print ("Reading nomenclature data")
		self.commodity_list = []
		my_file = os.path.join(self.EEA_DIR, "all_nomenclature_for_eea_calculations.csv")
		with open(my_file) as csv_file:
			csv_reader = csv.reader(csv_file, delimiter = ",")
			next (csv_reader)
			count = 0
			for row in csv_reader:
				if (len(row) > 0):
					count += 1
					goods_nomenclature_item_id	= row[0]
					productline_suffix			= row[1]
					gn = goods_nomenclature(goods_nomenclature_item_id, productline_suffix, "", "")
					gn.number_indents 			= int(row[2])
					gn.significant_digits 		= row[3]
					gn.leaf						= row[4]
					gn.description				= ""

					gn.eea_measure			= None
					gn.iceland_measure		= None
					gn.norway_measure		= None
					gn.eea_iceland_measure	= None

					gn.duty_list = []

					self.commodity_list.append(gn)
					
		print ("Reading nomenclature data - complete")

		
		# Run through the list and get parent codes for each code 
		goods_nomenclature_count = len(self.commodity_list)

		print ("Working out nomenclature parent / child relationships")
		for loop1 in range(0, goods_nomenclature_count):
			my_commodity = self.commodity_list[loop1]
			if my_commodity.significant_digits == 2:
				pass
			else:
				if my_commodity.number_indents == 0:
					for loop2 in range(loop1 - 1, -1, -1):
						prior_commodity = self.commodity_list[loop2]
						if prior_commodity.significant_digits == 2:
							my_commodity.parent_goods_nomenclature_item_id = prior_commodity.goods_nomenclature_item_id
							my_commodity.parent_productline_suffix = prior_commodity.productline_suffix
							break
				else:
					for loop2 in range(loop1 - 1, -1, -1):
						prior_commodity = self.commodity_list[loop2]
						if prior_commodity.number_indents == (my_commodity.number_indents - 1):
							my_commodity.parent_goods_nomenclature_item_id = prior_commodity.goods_nomenclature_item_id
							my_commodity.parent_productline_suffix = prior_commodity.productline_suffix
							break
		print ("Working out nomenclature parent / child relationships - complete")




		out = "comm code,PLS,Indents,EEA (2012),EEA Iceland (2014),Iceland,Norway,Prevailing,Active count,Conflicts,Parent ID, Parent PLS,Description\n"
		list_count = len(self.commodity_list)

		if self.output_profile.lower() == "norway":
			print ("Getting preferences for Norway and EEA")
			self.duties_2012 = self.get_preferences("2012")
			self.duties_NO = self.get_preferences("NO")
		else:
			print ("Getting preferences for Iceland, Iceland-EEA and EEA")
			self.duties_2012 = self.get_preferences("2012")
			self.duties_2014 = self.get_preferences("2014")
			self.duties_IS = self.get_preferences("IS")
		

		print ("Getting preferences - complete")
		print ("Working out lower preference")
		for loop1 in range(0, list_count):
			g = self.commodity_list[loop1]
			if self.output_profile.lower() == "norway":
				g.get_lower_norway()
			else:
				g.get_lower_iceland()


		p = ProgressBar(list_count, sys.stdout)
		print ("Getting ME32 conflicts in dataset")
		for loop1 in range(0, list_count):
			g = self.commodity_list[loop1]
			p.print_progress(loop1)

			if g.prevailing_measure != None:
				g.get_conflicts(loop1, g, "both")

		print ("\n\n")
		print ("Resolving ME32 conflicts in dataset")
		for loop1 in range(0, list_count):
			g = self.commodity_list[loop1]
			
			if g.active_measure_count > 1:
				a = 1
				g.resolve_conflicts()
			

			out += '"' + g.goods_nomenclature_item_id + '",'
			out += g.productline_suffix + ","
			out += str(g.number_indents) + ","
			if g.eea_measure != None:
				out += str(g.eea_measure.combined_duty) + ","
			else:
				out += ","

			if g.eea_iceland_measure != None:
				out += str(g.eea_iceland_measure.combined_duty) + ","
			else:
				out += ","

			if g.iceland_measure != None:
				out += str(g.iceland_measure.combined_duty) + ","
			else:
				out += ","

			if g.norway_measure != None:
				out += str(g.norway_measure.combined_duty) + ","
			else:
				out += ","

			if g.prevailing_measure != None:
				out += str(g.prevailing_measure.combined_duty) + ","
			else:
				out += ","

			# Print the number of conflicts (including self)
			out += str(g.active_measure_count) + ","

			# Print the actual conflicts
			if len(g.conflict_list) > 0:
				s = ""
				for item in g.conflict_list:
					s += item.goods_nomenclature_item_id + " | "
				s = s.strip(" | ")
				out += '"' + s + '",'
			else:
				out += ","

			out += '"' + g.parent_goods_nomenclature_item_id + '",'
			out += '"' + g.parent_productline_suffix + '",'

			out += '"' + g.description.strip() + '"\n'

		print ("\n")

		filename = my_file = os.path.join(self.CSV_DIR, "data_" + self.output_profile + ".csv")
		f = open(filename, "w+")
		f.write(out)
		f.close()


		# Get the SIDs for the relevant nomenclatures
		commodity_string = ""
		for g in self.commodity_list:
			if g.prevailing_measure != None:
				commodity_string += "'" + g.goods_nomenclature_item_id + "', "
		commodity_string = commodity_string.strip(", ")
		#commodity_string = set(commodity_string)
		#commodity_string = sorted(commodity_string)
		sql = """
		select goods_nomenclature_sid, goods_nomenclature_item_id, producline_suffix
		from goods_nomenclatures
		where goods_nomenclature_item_id in (""" + commodity_string + """)
		and (validity_end_date >= '""" + self.critical_date_plus_one_string + """' or validity_end_date is null)
		"""
		cur = self.conn.cursor()
		cur.execute(sql)
		self.goods_nomenclature_sid_lookup = cur.fetchall()
		

		# now write the XML
		out = ""
		xml_envelope = self.template_envelope
		for g in self.commodity_list:
			if g.prevailing_measure != None:
				if g.goods_nomenclature_item_id not in ("0304750000", "1905401000", "1905409000"):
					m = g.prevailing_measure
					out += m.xml()

		xml_envelope = xml_envelope.replace("[CONTENT]", out)
				
		self.output_filename = my_file = os.path.join(self.XML_OUT_DIR, "november_duties_" + self.output_profile + ".xml")
		print ("Writing to file " + self.output_filename)
		f = open(self.output_filename, "w+")
		f.write(xml_envelope)


		self.validate()



	def get_preferences(self, geographical_area_id):
		master_measure_list = []
		# OLD
		sql = """
		select m.measure_sid, m.goods_nomenclature_item_id, m.validity_start_date, m.validity_end_date,
		mc.duty_expression_id, mc.duty_amount, mc.monetary_unit_code, mc.measurement_unit_code, mc.measurement_unit_qualifier_code
		from ml.v5_2019 m left outer join measure_components mc
		on m.measure_sid = mc.measure_sid
		where geographical_area_id = '""" + geographical_area_id + """'
		and measure_type_id in ('142', '145')
		order by goods_nomenclature_item_id, duty_expression_id
		"""
		# NEW
		sql = """
		select m.measure_sid, m.goods_nomenclature_item_id, m.validity_start_date, m.validity_end_date,
		mc.duty_expression_id, mc.duty_amount, mc.monetary_unit_code, mc.measurement_unit_code, mc.measurement_unit_qualifier_code
		from ml.measures_real_end_dates m left outer join measure_components mc
		on m.measure_sid = mc.measure_sid
		where geographical_area_id = '""" + geographical_area_id + """'
		and measure_type_id in ('142', '145')
		and validity_start_date <= '2019-12-31'
		and validity_end_date >= '2019-01-01'
		order by goods_nomenclature_item_id, duty_expression_id
		"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()

		measure_sid_string = "" # This is used to see which of these measures should be excluded

		if len(rows) > 0:
			for rw in rows:
				measure_sid						= rw[0]
				goods_nomenclature_item_id		= rw[1]
				validity_start_date				= rw[2]
				validity_end_date				= rw[3]
				duty_expression_id 				= rw[4]
				duty_amount 					= rw[5]
				monetary_unit_code 				= rw[6]
				measurement_unit_code 			= rw[7]
				measurement_unit_qualifier_code = rw[8]

				if str(str(measure_sid)) not in measure_sid_string:
					measure_sid_string += str(measure_sid) + ", " # Required for exclusion

				# A bit of hygiene first - if the duty expression is 0, then we must set to '01' and the duty_amount to 0.
				# This deals with EPS values where the rates are in measure condition components, not measures
				if duty_expression_id == None:
					duty_expression_id = "01"
					duty_amount = 0

				item = eea_item(geographical_area_id, measure_sid, goods_nomenclature_item_id, validity_start_date,
    			validity_end_date, duty_expression_id, duty_amount, monetary_unit_code, measurement_unit_code, measurement_unit_qualifier_code)

				master_measure_list.append(item)

			measure_sid_string = measure_sid_string.strip(", ") # Clean the exclusion string

			# Now, using this string, get the list of exclusions
			if geographical_area_id in ('2012', '2014'):
				if self.output_profile == "iceland":
					self.country_string = "IS"
				else:
					self.country_string = "NO"
				sql = """select distinct measure_sid from measure_excluded_geographical_areas
				where excluded_geographical_area = '""" + self.country_string + """'
				and measure_sid in (""" + measure_sid_string + """)
				order by measure_sid
				"""
				cur = self.conn.cursor()
				cur.execute(sql)
				rows = cur.fetchall()

				excluded_measure_list = []

				if len(rows) > 0:
					for rw in rows:
						excluded_measure_list.append (rw[0])
			else:
				excluded_measure_list = []
			
			# This process takes the 'items' loaded from the database and assigns them to
			# individual measures with measure components
			# We need to keep the Meursing duties in, as they all seem to be related to Max / Mins as well
			measure_sid_list = []
			measure_list = []

			#for ex in excluded_measure_list:
			#	print ("xx: ", ex)

			for item in master_measure_list:
				if item.measure_sid in excluded_measure_list:
					pass
				else:
					if item.measure_sid in measure_sid_list:
						# Find the item, then assign the measure component only
						for m in measure_list:
							if m.measure_sid == item.measure_sid:
								mc = eea_measure_component(item.measure_sid, item.duty_expression_id, item.duty_amount, item.monetary_unit_code,
								item.measurement_unit_code, item.measurement_unit_qualifier_code)
								m.measure_component_list.append (mc)
					else:
						# Create a new measure & assign measure components to it
						m = eea_measure(item.geographical_area_id, item.measure_sid, item.goods_nomenclature_item_id, item.validity_start_date, item.validity_end_date)
						mc = eea_measure_component(item.measure_sid, item.duty_expression_id, item.duty_amount, item.monetary_unit_code, item.measurement_unit_code, item.measurement_unit_qualifier_code)
						m.measure_component_list.append (mc)
						measure_list.append (m)

					measure_sid_list.append(item.measure_sid)

			# This process selectively strips Meursing codes, as they are unnecessary,
			# but it also needs to take the max / mins with it
			
			for m in measure_list:
				for mc in m.measure_component_list:
					if mc.duty_expression_id in ('12', '14', '21', '25', '27', '29'):
						m.mark_for_measure_trimming = True
						break
			
			for m in measure_list:
				if m.mark_for_measure_trimming == True:
					count = len(m.measure_component_list)
					if count > 1:
						for i in range(count-1, 0, -1):
							m.measure_component_list.pop(i)
			

			# This process goes through all of the measures and assigns a value against them
			"""
			for m in measure_list:
				print (m.measure_sid, m.goods_nomenclature_item_id, m.geographical_area_id, len(m.measure_component_list))
			"""

		
			for m in measure_list:
				my_value = 0
				for mc in m.measure_component_list:
					if mc.monetary_unit_code == None:
						my_value += float(mc.duty_amount) * 1000
					else:
						if mc.duty_expression_id in ("01", "04"):
							my_value += float(mc.duty_amount)

				m.value = my_value
				m.combine_duties()
			
			
			# Assign the measures to the commodities
			for m in measure_list:
				for g in self.commodity_list:
					if m.goods_nomenclature_item_id == g.goods_nomenclature_item_id:
						if g.productline_suffix == "80":
							if geographical_area_id == "2012":
								g.eea_measure = m
							elif geographical_area_id == "2014":
								g.eea_iceland_measure = m
							elif geographical_area_id == "IS":
								g.iceland_measure = m
							elif geographical_area_id == "NO":
								g.norway_measure = m

		return (master_measure_list)	

	def validate(self):
		fname = self.output_filename
		msg = "Validating the XML file against the Taric 3 schema"
		self.d(msg, False)
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

	def get_exclusions(self):
		self.exclusions_list = []
		if self.output_profile != "2020":
			return

		my_file = os.path.join(self.CSV_DIR, "gsp_2020_exclusions.csv")
		with open(my_file) as csv_file:
			csv_reader = csv.reader(csv_file, delimiter = ",")
			for row in csv_reader:
				if (len(row) > 0):
					geographical_area_id_group			= row[0]
					goods_nomenclature_item_id			= row[1]
					geographical_area_id_group_excluded	= row[2]

					if (geographical_area_id_group != ""):
						obj = measure_excluded_geographical_area(geographical_area_id_group, goods_nomenclature_item_id, geographical_area_id_group_excluded)
						self.exclusions_list.append(obj)
		

	def set_config(self):
		jsonFile = open(self.CONFIG_FILE, "r")	# Open the JSON file for reading
		data = json.load(jsonFile)				# Read the JSON into the buffer
		jsonFile.close()						# Close the JSON file

		data["minimum_sids"][self.DBASE]["last_transaction_id"] = self.transaction_id
		data["minimum_sids"][self.DBASE]["measures"] = self.last_measure_sid
		data["minimum_sids"][self.DBASE]["measure.conditions"] = self.last_measure_condition_sid

		print (self.last_measure_sid)

		jsonFile = open(self.CONFIG_FILE, "w+")
		jsonFile.write(json.dumps(data, indent=4, sort_keys=True))
		jsonFile.close()


	def get_nomenclature_dates(self):
		clause = ""
		for m in self.measure_list:
			clause += "'" + m.goods_nomenclature_item_id + "', "

		clause = clause.strip()
		clause = clause.strip(",")
		sql = """SELECT goods_nomenclature_item_id, validity_start_date, validity_end_date FROM goods_nomenclatures
		WHERE goods_nomenclature_item_id IN (""" + clause + """) AND validity_end_date IS NULL ORDER BY 1"""
		#print (sql)
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		if len(rows) > 0:
			self.commodity_list		= []
			self.valid_commodity_list	= []
			for rw in rows:
				goods_nomenclature_item_id	= rw[0]
				validity_start_date			= rw[1]
				validity_end_date			= rw[2]
				g = goods_nomenclature(goods_nomenclature_item_id, "80", validity_start_date, validity_end_date)
				self.commodity_list.append (g)
				self.valid_commodity_list.append (goods_nomenclature_item_id)


	def get_export_controls(self):
		self.get_geographical_areas()
		self.get_export_profiles()
		self.get_export_measures()


	def get_export_profiles(self):
		self.d("Getting export profiles", False)
		file = os.path.join(self.SOURCE_DIR, self.output_profile + ".xlsx")
		workbook	= xlrd.open_workbook(file)
		wb			= workbook.sheet_by_name("profiles")
		row_count	= wb.nrows
		self.condition_profiles = []
		
		for row in range(1, row_count):
			profile		= wb.cell(row, 0).value
			condition1	= wb.cell(row, 1).value
			condition2	= wb.cell(row, 2).value
			condition3	= wb.cell(row, 3).value

			my_condition_profile			= condition_profile()
			my_condition_profile.profile	= profile

			my_condition_profile.create_condition(1, condition1)
			my_condition_profile.create_condition(2, condition2)
			my_condition_profile.create_condition(3, condition3)

			self.condition_profiles.append(my_condition_profile)


	def get_export_measures(self):
		self.d("Getting export measures", False)
		file = os.path.join(self.SOURCE_DIR, self.output_profile + ".xlsx")
		workbook				= xlrd.open_workbook(file)
		wb						= workbook.sheet_by_name("measures")
		row_count				= wb.nrows
		self.export_measures	= []
		
		for row in range(1, row_count):
			category					= wb.cell(row, 0).value
			subcategory					= wb.cell(row, 1).value
			goods_nomenclature_item_id	= self.convert_to_10_digit(wb.cell(row, 2).value)
			footnote					= wb.cell(row, 3).value
			measure_type_id				= str(wb.cell(row, 4).value)
			geographical_area_id		= str(wb.cell(row, 5).value)
			regulation_id				= wb.cell(row, 6).value
			omit						= wb.cell(row, 7).value
			condition_profile			= wb.cell(row, 8).value
			validity_start_date			= str(wb.cell(row, 9).value)

			if omit != "Y":
				my_measure = measure(goods_nomenclature_item_id, regulation_id, geographical_area_id, measure_type_id, "", "", "", "", "", validity_start_date, "", "")
				my_measure.apply_footnote(footnote)
				my_condition = self.get_condition(condition_profile)
				my_measure.apply_conditions(my_condition)

			self.export_measures.append (my_measure)


	def convert_to_10_digit(self, s):
		s = str(s)
		s = s + ("0" * (10-len(s)))
		return (s)


	def get_condition(self, condition_profile):
		out = None
		for item in self.condition_profiles:
			if condition_profile == item.profile:
				out = item
				break

		if out == None:
			print ("No profile found")
			sys.exit()
		else:
			return (out)