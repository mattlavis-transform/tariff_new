import psycopg2
import sys
import os
from os import system, name
import csv
import re
import codecs
import json
import xmlschema

import common.functions as fn
from common.master_commodity import master_commodity
from common.measure import measure
from common.goods_nomenclature import goods_nomenclature
from progressbar import ProgressBar
from datetime import datetime
from datetime import timedelta

class application(object):
	def __init__(self):
		self.clear()

		self.BASE_DIR				= os.path.join(os.path.dirname( __file__ ), '..')
		temp						= os.path.join(self.BASE_DIR, '..')
		temp						= os.path.join(temp, 'convert_and_import_taric')
		self.OUT_DIR				= os.path.join(self.BASE_DIR, "xml_out")
		self.LOG_DIR				= os.path.join(self.BASE_DIR, "log")
		self.TEMPLATE_DIR			= os.path.join(self.BASE_DIR, "templates")
		self.SOURCE_DIR				= os.path.join(self.BASE_DIR, "source")
		self.CONFIG_DIR				= os.path.join(self.BASE_DIR, "..")
		self.CONFIG_DIR				= os.path.join(self.CONFIG_DIR, "config")
		self.CONFIG_FILE			= os.path.join(self.CONFIG_DIR, "config_common.json")
		self.CONFIG_FILE_LOCAL		= os.path.join(self.CONFIG_DIR, "config_create_reference_data.json")

		self.SCHEMA_DIR				= os.path.join(self.BASE_DIR, "..")
		self.SCHEMA_DIR				= os.path.join(self.SCHEMA_DIR, "xsd")

		self.output_filename		= os.path.join(self.OUT_DIR, "uk_mfn_data.xml")
		self.verification_filename	= os.path.join(self.LOG_DIR, "verify_mfn_data.csv")
		self.fname_mfn_csv			= os.path.join(self.SOURCE_DIR, "mfns_nov_19.csv")

		# The XML export directory
		temp				= os.path.join(self.BASE_DIR, '..')
		temp				= os.path.join(temp, 'convert_and_import_taric')
		self.XML_DIR		= os.path.join(temp, "xml_in")
		self.XML_DIR		= os.path.join(self.XML_DIR, "custom")

		self.footnotes_list				= []
		self.footnote_type_list			= []
		self.measure_type_list			= []
		self.additional_code_type_list	= []
		self.goods_nomenclature_list	= []
		self.measure_105_list			= []
		self.certificate_type_list		= []
		self.geographical_area_list		= []
		self.membership_list			= []
		self.certificates_list			= []
		self.base_regulations_list		= []
		self.regulation_groups_list		= []
		self.measure_list				= []
		self.cnt = 0

		self.base_footnote_description_period_sid			= 200000
		self.base_certificate_description_period_sid 		= 10000
		self.base_geographical_area_description_period_sid	= 10000

		self.base_envelope_id = 5000
		self.message_id = 1
		self.fname = ""

		self.get_config()
		self.get_minimum_sids()
		self.transaction_id = self.last_transaction_id


	def protect_commodities(self):
		# Now comes the fun stuff - go through the entire nomenclature base
		# and protect codes that cannot have duties applied to them without triggering ME32
		print ("Protecting commodities from ME32")
		list_count = len(self.goods_nomenclature_list)
		p = ProgressBar(list_count, sys.stdout)
		for loop1 in range(0, list_count):
			g1 = self.goods_nomenclature_list[loop1]
			if g1.status in ("Applied", "Liberalise"):
				self.protect_code(loop1, g1, "both")
			p.print_progress(loop1)

		print ("\n")
	

	def create_liberalised_duties(self):
		# Anything that is left is liberalised at the highest level possible and then "Protected" at lower levels
		list_count = len(self.goods_nomenclature_list)
		print ("Creating liberalised tariff duties")
		p = ProgressBar(list_count, sys.stdout)
		for loop1 in range(0, list_count):
			g1 = self.goods_nomenclature_list[loop1]
			if g1.status == "":
				g1.status			= "Liberalise"
				g1.measure_type_id	= "103"
				g1.advalorem		= "0"
				g1.specific			= ""
				g1.minimum			= ""
				self.protect_code(loop1, g1, "down")
				p.print_progress(loop1)

		print ("\n")


	def write_xml(self):
		# Write the XML
		self.measure_sid = self.last_measure_sid + 1
		xml_envelope = self.envelope_XML
		xml_string = ""
		for g in self.goods_nomenclature_list:
			if g.status in ("Applied", "Liberalise"):
				xml_string += g.xml()

		xml_envelope = xml_envelope.replace("[CONTENT]", xml_string)

		file = open(self.output_filename, "w")
		file.write(xml_envelope)
		file.close()

	def write_verification_csv(self):
		# Write the CSV
		with open(self.verification_filename, 'w') as csvfile:
			csv_writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)

			for g in self.goods_nomenclature_list:
				csv_writer.writerow([g.goods_nomenclature_item_id, g.productline_suffix, g.leaf, g.print_measure_type_id(), g.print_status(), g.print_advalorem(), g.print_specific(), g.print_minimum(), g.description])



	def get_nomenclature_sids(self):
		commodity_string = ""
		for g in self.goods_nomenclature_list:
			if g.status in ("Applied", "Liberalise"):
				commodity_string += "'" + g.goods_nomenclature_item_id + "', "
		commodity_string = commodity_string.strip()
		commodity_string = commodity_string.strip(",")

		sql = """select goods_nomenclature_item_id, goods_nomenclature_sid from goods_nomenclatures where producline_suffix = '80'
		and goods_nomenclature_item_id in (""" + commodity_string + """) order by goods_nomenclature_item_id, validity_start_date DESC"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		self.goods_nomenclature_list_for_sids = []
		if len(rows) > 0:
			for row in rows:
				goods_nomenclature_item_id	= row[0]
				goods_nomenclature_sid		= row[1]
				obj = goods_nomenclature(goods_nomenclature_item_id, goods_nomenclature_sid)
				self.goods_nomenclature_list_for_sids.append (obj)


	def perform_me32_checks(self):
		list_count = len(self.goods_nomenclature_list)
		for loop1 in range(0, list_count):
			g1 = o.app.goods_nomenclature_list[loop1]
			if g1.measure_type_id != "":
				for loop2 in range(loop1 + 1, list_count):
					g2 = o.app.goods_nomenclature_list[loop2]
					if g2.parent_goods_nomenclature_item_id == g1.goods_nomenclature_item_id and \
						g2.parent_productline_suffix == g1.productline_suffix:
						
						if g2.measure_type_id == "":
							print ("Inheriting")
							g2.measure_type_id = g1.measure_type_id
							g2.advalorem = g1.advalorem
							g2.specific = g1.specific
							g2.minimum = g1.minimum
							g2.status = "INHERIT"
						else:
							print ("Inheritance error")
							g2.status = "ERROR"
					elif g2.number_indents == 0:
						break


		for g in self.goods_nomenclature_list:
			if g.advalorem == "" and g.specific == "" and g.minimum == "":
				if g.significant_digits > 4:
					if int(g.leaf) == 1:
						g.status = "Missing"



	def write_measure_count(self):
		liberal_count = 0
		applied_count = 0
		protect_count = 0
		barred_count  = 0
		other_count   = 0

		for g in self.goods_nomenclature_list:
			if g.status == "Liberalise":
				liberal_count += 1
			elif g.status == "Applied":
				applied_count += 1
			elif g.status == "Barred":
				barred_count += 1
			elif g.status == "Protect":
				protect_count += 1
			else:
				other_count += 1

		print ("\nResults")
		print ("=======")
		print ("Number of liberalised duties", str(liberal_count))
		print ("Number of applied duties", str(applied_count))
		print ("Number of barred duties", str(barred_count))
		print ("Number of protected duties", str(protect_count))
		print ("Number of other duties", str(other_count))



	def get_measure_type_105(self):
		self.measure_105_list = []
		fname_csv	= os.path.join(self.SOURCE_DIR, "measure_type_105.csv")
		print ("Getting commodities that have 105 measures")
		with open(fname_csv, 'r') as csvfile:
			csv_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
			next (csv_reader)
			for row in csv_reader:
				goods_nomenclature_item_id	= row[0]
				self.measure_105_list.append(goods_nomenclature_item_id)
		

	def get_uk_mfns(self):
		with open(self.fname_mfn_csv, 'r') as csvfile:
			csv_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
			next (csv_reader)
			for row in csv_reader:
				goods_nomenclature_item_id	= row[0]
				advalorem     				= row[1]
				specific     				= row[2]
				minimum						= row[3]

				obj = measure(goods_nomenclature_item_id, advalorem, specific, minimum)
				obj.get_indents_and_description()
				self.measure_list.append(obj)

		self.measure_list = sorted(self.measure_list, key = lambda x: x.goods_nomenclature_item_id, reverse = False)
		a = 1


	def get_nomenclature(self):
		# Open up the full list of commodity codes as it currently stands
		# Read the commodity codes into the goods_nomenclature_list array
		fname_csv	= os.path.join(self.SOURCE_DIR, "complete_nomenclature.csv")
		print ("Getting full nomenclature list")
		with open(fname_csv, 'r') as csvfile:
			csv_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
			next (csv_reader)
			for row in csv_reader:
				goods_nomenclature_item_id	= row[1]
				productline_suffix     		= row[2]
				number_indents     			= int(row[3])
				description     			= ""
				significant_digits     		= row[5]
				leaf						= row[6]

				obj = master_commodity(goods_nomenclature_item_id, productline_suffix, number_indents, significant_digits, description, leaf)
				self.goods_nomenclature_list.append(obj)

		# Get parent goods_nomenclature_item_id and productline_suffix for each commodity
		goods_nomenclature_count = len(self.goods_nomenclature_list)
		for loop1 in range(0, goods_nomenclature_count):
			my_commodity = self.goods_nomenclature_list[loop1]
			if my_commodity.significant_digits == 2:
				pass
			else:
				if my_commodity.number_indents == 0:
					for loop2 in range(loop1 - 1, -1, -1):
						prior_commodity = self.goods_nomenclature_list[loop2]
						if prior_commodity.significant_digits == 2:
							my_commodity.parent_goods_nomenclature_item_id = prior_commodity.goods_nomenclature_item_id
							my_commodity.parent_productline_suffix = prior_commodity.productline_suffix
							break
				else:
					for loop2 in range(loop1 - 1, -1, -1):
						prior_commodity = self.goods_nomenclature_list[loop2]
						if prior_commodity.number_indents == (my_commodity.number_indents - 1):
							my_commodity.parent_goods_nomenclature_item_id = prior_commodity.goods_nomenclature_item_id
							my_commodity.parent_productline_suffix = prior_commodity.productline_suffix
							break

	def protect_code(self, my_index, my_commodity, direction):
		# Search up the tree for parentage
		if direction != "down":
			for loop2 in range(my_index - 1, -1, -1):
				prior_commodity = self.goods_nomenclature_list[loop2]
				if prior_commodity.goods_nomenclature_item_id == my_commodity.parent_goods_nomenclature_item_id \
					and prior_commodity.productline_suffix == my_commodity.parent_productline_suffix:
					if prior_commodity.status not in ("Applied", "Protect", "Liberalise"):
						prior_commodity.status = "Protect"
						self.protect_code(loop2, prior_commodity, "up")
					if prior_commodity.significant_digits == 4:
						break

		# Now search down the tree for children
		if direction != "up":
			for loop2 in range(my_index + 1, len(self.goods_nomenclature_list)):
				next_commodity = self.goods_nomenclature_list[loop2]
				if next_commodity.parent_goods_nomenclature_item_id == my_commodity.goods_nomenclature_item_id \
					and next_commodity.parent_productline_suffix == my_commodity.productline_suffix:
					if next_commodity.status not in ("Applied", "Protect", "Liberalise"):
						next_commodity.status = "Protect"
						self.protect_code(loop2, next_commodity, "down")
					if next_commodity.significant_digits == 4 or loop2 == len(self.goods_nomenclature_list):
						break
		pass		

	def get_minimum_sids(self):
		with open(self.CONFIG_FILE, 'r') as f:
			my_dict = json.load(f)
		
		min_list = my_dict['minimum_sids'][self.DBASE]

		self.last_additional_code_description_period_sid	= self.larger(self.get_scalar("SELECT MAX(additional_code_description_period_sid) FROM additional_code_description_periods_oplog;"), min_list['additional.code.description.periods']) + 1
		self.last_additional_code_sid						= self.larger(self.get_scalar("SELECT MAX(additional_code_sid) FROM additional_codes_oplog;"), min_list['additional.codes']) + 1

		self.last_certificate_description_period_sid		= self.larger(self.get_scalar("SELECT MAX(certificate_description_period_sid) FROM certificate_description_periods_oplog;"), min_list['certificate.description.periods']) + 1
		self.last_footnote_description_period_sid			= self.larger(self.get_scalar("SELECT MAX(footnote_description_period_sid) FROM footnote_description_periods_oplog;"), min_list['footnote.description.periods']) + 1
		self.last_geographical_area_description_period_sid	= self.larger(self.get_scalar("SELECT MAX(geographical_area_description_period_sid) FROM geographical_area_description_periods_oplog;"), min_list['geographical.area.description.periods']) + 1
		self.last_geographical_area_sid						= self.larger(self.get_scalar("SELECT MAX(geographical_area_sid) FROM geographical_areas_oplog;"), min_list['geographical.areas']) + 1

		self.last_goods_nomenclature_sid					= self.larger(self.get_scalar("SELECT MAX(goods_nomenclature_sid) FROM goods_nomenclatures_oplog;"), min_list['goods.nomenclature']) + 1
		self.last_goods_nomenclature_indent_sid				= self.larger(self.get_scalar("SELECT MAX(goods_nomenclature_indent_sid) FROM goods_nomenclature_indents_oplog;"), min_list['goods.nomenclature.indents']) + 1
		self.last_goods_nomenclature_description_period_sid	= self.larger(self.get_scalar("SELECT MAX(goods_nomenclature_description_period_sid) FROM goods_nomenclature_description_periods_oplog;"), min_list['goods.nomenclature.description.periods']) + 1

		self.last_measure_sid								= self.larger(self.get_scalar("SELECT MAX(measure_sid) FROM measures_oplog;"), min_list['measures']) + 1
		self.last_measure_condition_sid						= self.larger(self.get_scalar("SELECT MAX(measure_condition_sid) FROM measure_conditions_oplog"), min_list['measure.conditions']) + 1

		self.last_quota_order_number_sid					= self.larger(self.get_scalar("SELECT MAX(quota_order_number_sid) FROM quota_order_numbers_oplog"), min_list['quota.order.numbers']) + 1
		self.last_quota_order_number_origin_sid				= self.larger(self.get_scalar("SELECT MAX(quota_order_number_origin_sid) FROM quota_order_number_origins_oplog"), min_list['quota.order.number.origins']) + 1
		self.last_quota_definition_sid						= self.larger(self.get_scalar("SELECT MAX(quota_definition_sid) FROM quota_definitions_oplog"), min_list['quota.definitions']) + 1
		self.last_quota_suspension_period_sid				= self.larger(self.get_scalar("SELECT MAX(quota_suspension_period_sid) FROM quota_suspension_periods_oplog"), min_list['quota.suspension.periods']) + 1
		self.last_quota_blocking_period_sid					= self.larger(self.get_scalar("SELECT MAX(quota_blocking_period_sid) FROM quota_blocking_periods_oplog"), min_list['quota.blocking.periods']) + 1

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


	def get_profile(self, profile_type):
		if profile_type == "regulations":
			self.d("Getting regulation profile")
			if (len(sys.argv) > 1):
				profile = sys.argv[1]
				self.d("Using regulation profile: " + profile)
				try:
					with open(self.CONFIG_FILE_LOCAL, 'r') as f:
						my_dict = json.load(f)

					self.excel_source	= os.path.join(self.SOURCE_DIR, my_dict["regulation_profiles"][profile]["source"])
					self.xml_file		= my_dict["regulation_profiles"][profile]["dest"]

				except:
					self.d("No profile specified or profile not found. Please supply correct profile name after the script name.", False)
					sys.exit()


	def set_config(self):
		jsonFile = open(self.CONFIG_FILE, "r")	# Open the JSON file for reading
		data = json.load(jsonFile)				# Read the JSON into the buffer
		jsonFile.close()						# Close the JSON file

		data["minimum_sids"][self.DBASE]["last_transaction_id"] = self.transaction_id
		data["minimum_sids"][self.DBASE]["measures"] = self.last_measure_sid

		print (self.last_measure_sid)

		jsonFile = open(self.CONFIG_FILE, "w+")
		jsonFile.write(json.dumps(data, indent=4, sort_keys=True))
		jsonFile.close()


	def get_config(self):
		with open(self.CONFIG_FILE, 'r') as f:
			my_dict = json.load(f)

		self.DBASE					= my_dict['dbase']
		self.p						= my_dict['p']
		self.last_transaction_id	= my_dict["minimum_sids"][self.DBASE]["last_transaction_id"]

		critical_date 						= my_dict['critical_date']
		self.critical_date					= datetime.strptime(critical_date, '%Y-%m-%d')
		self.critical_date_plus_one			= self.critical_date + timedelta(days = 1)
		self.critical_date_plus_one_string	= datetime.strftime(self.critical_date_plus_one, '%Y-%m-%d')

		self.debug					= my_dict['debug']
		self.connect()

	def d(self, s, include_indent = True):
		if self.debug:
			if include_indent:
				s = "- " + s
			else:
				s = "\n" + s.upper()
			#print (s + "\n")
			print (s)

	def clear(self):
		# for windows
		if name == 'nt':
			_ = system('cls')
		# for mac and linux(here, os.name is 'posix')
		else:
			_ = system('clear')

	def connect(self):
		self.conn = psycopg2.connect("dbname=" + self.DBASE + " user=postgres password=" + self.p)

	def get_templates(self):
		print ("Reading XML templates")
		# Get envelope XML
		filename = os.path.join(self.TEMPLATE_DIR, "envelope.xml")
		handle = open(filename, "r")
		self.envelope_XML = handle.read()

		# Get measure XML
		filename = os.path.join(self.TEMPLATE_DIR, "measure.xml")
		handle = open(filename, "r")
		self.measure_XML = handle.read()

		# Get measure component XML
		filename = os.path.join(self.TEMPLATE_DIR, "measure.component.xml")
		handle = open(filename, "r")
		self.measure_component_XML = handle.read()
