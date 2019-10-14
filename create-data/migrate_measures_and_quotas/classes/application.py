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
from classes.quota_order_number import quota_order_number
from classes.quota_definition import quota_definition
from classes.quota_association import quota_association
from classes.quota_suspension_period import quota_suspension_period
from classes.quota_blocking_period import quota_blocking_period
from classes.measure import measure
from classes.measure_component import measure_component
from classes.measure_condition import measure_condition
from classes.measure_condition_component import measure_condition_component
from classes.measure_excluded_geographical_area import measure_excluded_geographical_area
from classes.measure_footnote import measure_footnote
from classes.measure_partial_temporary_stop import measure_partial_temporary_stop
from classes.base_regulation import base_regulation
from classes.business_rules import business_rules
from classes.quota_balance import quota_balance
from classes.quota_description import quota_description

from progressbar import ProgressBar
import classes.functions as fn

class application(object):
	def __init__(self):
		self.clear()

		self.BASE_DIR				= os.path.dirname(os.path.abspath(__file__))
		if "/classes" in self.BASE_DIR:
			self.BASE_DIR = self.BASE_DIR.replace("/classes", "")
		else:
			self.BASE_DIR = self.BASE_DIR.replace("\classes", "")
		self.TEMPLATE_DIR			= os.path.join(self.BASE_DIR,	"templates")
		self.CSV_DIR				= os.path.join(self.BASE_DIR,	"csv")
		self.SOURCE_DIR 			= os.path.join(self.BASE_DIR,	"source")
		self.MIGRATION_PROFILE_DIR 	= os.path.join(self.BASE_DIR,	"migration_profiles")
		
		self.XML_OUT_DIR			= os.path.join(self.BASE_DIR,	"xml_out")
		self.XML_REPORT_DIR			= os.path.join(self.BASE_DIR,	"xml_report")

		self.TEMP_DIR 				= os.path.join(self.BASE_DIR,	"temp")
		self.BULK_LOG_FILE			= os.path.join(self.TEMP_DIR,	"bulk_log.csv")

		self.CONFIG_DIR				= os.path.join(self.BASE_DIR, "..")
		self.CONFIG_DIR				= os.path.join(self.CONFIG_DIR, "config")
		self.CONFIG_FILE			= os.path.join(self.CONFIG_DIR, "config_common.json")
		self.CONFIG_FILE_LOCAL		= os.path.join(self.CONFIG_DIR, "config_migrate_measures_and_quotas.json")

		# Used for validating the produced XML
		self.SCHEMA_DIR				= os.path.join(self.BASE_DIR, "..")
		self.SCHEMA_DIR				= os.path.join(self.SCHEMA_DIR, "xsd")

		self.QUOTA_DIR				= os.path.join(self.SOURCE_DIR, "quotas")
		self.BALANCE_FILE			= os.path.join(self.QUOTA_DIR, "quota_volume_master.csv")
		self.QUOTA_DESCRIPTION_FILE	= os.path.join(self.QUOTA_DIR, "quota definitions.csv")
		self.MFN_COMPONENTS_FILE	= os.path.join(self.SOURCE_DIR, "mfn_components.csv")

		self.envelope_id            = "100000001"
		self.definition_sid_start   = 20000
		self.sequence_id            = 1
		self.content				= ""
		self.namespaces = {'oub': 'urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0', 'env': 'urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0', } # add more as needed

		self.regulation_list			= []
		self.measures_with_sivs_list	= []
		self.quota_definition_sid_mapping_list = []
		self.quota_description_list = []
		self.override_prompt = True
		self.destination_geographical_area_id	= ""
		self.destination_geographical_area_sid	= ""
		
		self.get_config()
		self.get_arguments()
		self.get_minimum_sids()

	def get_all_quota_order_numbers(self):
        # Get the detail of all quota order numbers
		self.quota_order_number_list = []
		sql = """SELECT quota_order_number_sid, quota_order_number_id, validity_start_date, validity_end_date FROM quota_order_numbers
		WHERE validity_end_date IS NULL OR validity_end_date  >= '""" + self.critical_date_string + """'
		ORDER BY 2, 3"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()

		for rw in rows:
			quota_order_number_sid	= rw[0]
			quota_order_number_id	= rw[1]
			validity_start_date		= rw[2]
			validity_end_date		= rw[3]
			q = quota_order_number(quota_order_number_sid, quota_order_number_id, validity_start_date, validity_end_date)
			self.quota_order_number_list.append(q)



	def get_quota_order_numbers(self):
        # Get the detail of all quota order numbers
		self.quota_order_number_list = []
		if self.regulation_string == "":
			sql = """SELECT quota_order_number_sid, quota_order_number_id, validity_start_date, validity_end_date FROM quota_order_numbers
			ORDER BY 1, 2 DESC"""
		else:
			sql = """SELECT DISTINCT qon.quota_order_number_sid, qon.quota_order_number_id, qon.validity_start_date, qon.validity_end_date FROM quota_order_numbers qon, measures m
			WHERE m.ordernumber = qon.quota_order_number_id
			AND LEFT(m.measure_generating_regulation_id, 7) IN (""" + self.regulation_string + """)
			ORDER BY 1, 2 DESC"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()

		for rw in rows:
			q = quota_order_number(rw[0], rw[1], rw[2], rw[3])
			self.quota_order_number_list.append(q)

	def get_quota_descriptions(self):
		self.d("Getting UK quota descriptions")
		self.quota_description_list = []
		with open(self.QUOTA_DESCRIPTION_FILE) as csv_file:
			csv_reader = csv.reader(csv_file, delimiter = ",")
			next(csv_reader, None)
			for row in csv_reader:
				quota_order_number_id 	= row[0]
				description 			= row[1]

				obj = quota_description(quota_order_number_id, description)
				self.quota_description_list.append (obj)

	def get_quota_balances(self):
		self.d("Getting UK quota balances")
		self.quota_balance_list = []
		with open(self.BALANCE_FILE) as csv_file:
			csv_reader = csv.reader(csv_file, delimiter = ",")
			next(csv_reader, None)
			for row in csv_reader:
				quota_order_number_id 	= row[0]
				country				 	= row[1]
				method				 	= row[2]
				y1_balance			 	= row[9]
				yx_balance			 	= row[10]
				yx_start			 	= row[11]

				obj = quota_balance(quota_order_number_id, country, method, y1_balance, yx_balance, yx_start)
				self.quota_balance_list.append (obj)

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

	def get_mfns(self):
		self.mfn_master_list = []
		with open(self.MFN_COMPONENTS_FILE) as csv_file:
			csv_reader = csv.reader(csv_file, delimiter = ",", quoting = csv.QUOTE_ALL)
			for row in csv_reader:
				if len(row) > 0:
					goods_nomenclature_item_id		= row[0]
					duty_expression_id				= row[1]
					duty_amount						= float(row[2])
					monetary_unit_code				= row[3]
					measurement_unit_code			= row[4]
					measurement_unit_qualifier_code	= row[5]
					measure_sid						= -1

					obj = measure_component(measure_sid, duty_expression_id, duty_amount, monetary_unit_code,
					measurement_unit_code, measurement_unit_qualifier_code, goods_nomenclature_item_id)
					self.mfn_master_list.append (obj)


	def get_config(self):
		# Get global config items
		with open(self.CONFIG_FILE, 'r') as f:
			my_dict = json.load(f)

		self.remove_SIVs 			= fn.mbool2(my_dict['remove_SIVs'])
		self.remove_Meursing 		= fn.mbool2(my_dict['remove_Meursing'])
		self.critical_date			= datetime.strptime(my_dict['critical_date'], '%Y-%m-%d')
		self.critical_date_plus_one	= self.critical_date + timedelta(days = 1)

		self.critical_date_string			= datetime.strftime(self.critical_date, '%Y-%m-%d')
		self.critical_date_plus_one_string	= datetime.strftime(self.critical_date_plus_one, '%Y-%m-%d')

		"""
		print (self.critical_date)
		print (self.critical_date_plus_one)
		print (type(self.critical_date))
		print (type(self.critical_date_plus_one))

		print (self.critical_date_string)
		print (self.critical_date_plus_one_string)
		print (type(self.critical_date_string))
		print (type(self.critical_date_plus_one_string))
		sys.exit()
		"""
		
		self.DBASE					= my_dict['dbase']
		
		self.p						= my_dict['p']
		self.DBASE_MIGRATE_MEASURES = my_dict['dbase_migrate_measures']
		
		self.connect()
		self.debug              	= fn.mbool2(my_dict['debug'])
		self.last_transaction_id	= my_dict["minimum_sids"][self.DBASE]["last_transaction_id"]
		self.transaction_id			= self.last_transaction_id + 1
		self.meursing_list			= my_dict['meursing_list'].split(", ")

		# Get local config items
		with open(self.CONFIG_FILE_LOCAL, 'r') as f:
			my_dict = json.load(f)
		self.bulk_migrations_list	= my_dict['bulk_migrations']

		self.preferential_measure_list = my_dict['measure_types']['preferential']
		self.all_country_profiles = my_dict['country_profiles']

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

	def get_arguments(self):
		# Argument 0 is the script
		# Argument 1 is scope type (measuretype, regulation)
		# Argument 2 is action type (terminate or split)
		# Argument 3 is measure type(s) or regulation ID, dependent on argument 2
		# Argument 4 is future regulation ID
		# Argument 5 is the country limiter (if required)

		# This function also determines the output filename

		# Initialise
		self.future_regulation_id	= ""
		self.scope					= ""
		self.action_string			= ""
		self.measure_type_list		= []
		self.regulation_string		= ""
		self.country_profile		= ""
		self.output_filename		= ""

		# These are special cases that allow for shortcuts across multiple measure types
		self.credibility_list			= ['430', '431', '485', '481', '482', '483']
		self.mfn_list					= ['103', '105']
		self.quota_list					= ['122', '123', '143', '146']
		self.supplementary_list			= ['109', '110']
		self.suspension_list			= ['112', '115', '117', '119', '141']
		self.surveillance_list			= ['442', '447']
		self.agri_list					= ['489', '490', '651', '652', '653', '654', '672', '673', '674']
		self.remedies_list				= ['570', '566', '565', '564', '561', '555', '553', '551']
		self.omit_measure_types_list	= self.credibility_list + self.supplementary_list + self.suspension_list + self.quota_list

		if "quota" in sys.argv[0]:	# quotas
			self.get_quota_parameters()

		elif sys.argv[0] == "migrate_measures.py": # We are migrating measures
			# Get scope type - this is specified in the 1st argument and could be "measure types", "regulation" or "country"
			if (len(sys.argv) > 1):
				if sys.argv[1] in ("measure_types", "measuretypes", "measuretype", "measure_type", "m"):	# measures
					self.scope = "measuretypes"
				elif sys.argv[1] in ("regulations", "regulation", "r", "rb"):		# regulations
					self.scope = "regulation"
				elif sys.argv[1] in ("country", "preferential", "c"):		# by country's preferential agreement
					self.scope = "country"

			# Get action type - this can be one of two things, either terminate (t) or restart (r)
			# In the case of terminate, the measures will be stopped, but not restarted after Brexit
			# in the case opf restart, the measures will be stopped and then restatred after Brexit

			self.action_verbatim = ""
			if (len(sys.argv) > 2):	# action
				if sys.argv[2] in ("terminate", "term", "t"):
					self.action_string = "terminate"
				elif sys.argv[2] in ("split", "continue", "restart", "r", "s", "c", "n", "newstart"):
					self.action_string = "restart"
				self.action_verbatim = sys.argv[2]

			if self.scope == "country":
				self.get_migrate_country_parameters()
			elif self.scope == "measuretypes":
				self.get_migrate_measure_type_parameters()
			elif self.scope == "regulation":
				self.get_migrate_regulation_parameters()


		elif sys.argv[0] == "bulk_migrate.py": # We are migrating regulations en masse
			# This is suitable for things like Trade Remedies and Import and Export Control
			self.scope = "bulk_migration"
			if (len(sys.argv) > 1):
				self.bulk_migration_profile = sys.argv[1]
			else:
				print ("No bulk migration profile found")
				sys.exit()

		# print the parameters to the screen
		if self.scope != "bulk_migration":
			self.d("Parameters", False)
			self.d("Using database: "			+ self.DBASE)
			self.d("Scope string: "				+ self.scope)
			self.d("Action string: "			+ self.action_string)
			self.d("Country string: "			+ self.country_profile)
			self.d("Regulation string: "		+ self.regulation_string)
			self.d("Measure type list: "		+ str(self.measure_type_list))
			self.d("Future regulation: "		+ self.future_regulation_id)
			self.d("Output filename: "			+ self.output_filename)
			self.d("Output folder: "			+ self.XML_OUT_DIR)
			print ("\n")

			if self.override_prompt == False:
				ret = fn.yes_or_no("Do you want to continue?")
				if not (ret):
					sys.exit()

	def get_migrate_regulation_parameters(self):
		# Get today's date - this will be appended to the output filename
		d = datetime.now()
		d2 = d.strftime("%Y-%m-%d")
		self.regulation_string = sys.argv[3]
		self.measure_type_string = ""

		# Get the future regulation ID
		if (len(sys.argv) > 4):
			self.future_regulation_id = sys.argv[4].strip()
			if len(self.future_regulation_id) != 8:
				print ("Erroneous future regulation string - please fix")
				sys.exit()

		# Get the optional final parameter (measure type - needed for Trade Remedies)
		regulation_string2 = self.regulation_string
		if (len(sys.argv) > 5):
			self.measure_type_string = sys.argv[5].strip()
			regulation_string2 += "_mt_" + self.measure_type_string

		# Set the filename for measure exports
		if self.action_string == "terminate":
			self.output_filename = self.scope + "_end_" + regulation_string2 + ".xml"
		else:
			self.output_filename = self.scope + "_end_" + regulation_string2 + "_start_" + self.future_regulation_id + ".xml"

		self.country_profile = ""
		

		# If this script is being run from the bulk migration script,
		# then it needs to be silent, i.e. suppress the "Are you sure?" prompt.
		# We also need to be able to recompile the individual files back into a
		# single XML file at the end, therefore we need to log the filenames that are created.
		if sys.argv[1] == "rb":
			self.override_prompt = True
			self.bulk_log()

	def bulk_log_delete(self):
		try:
			os.remove(self.BULK_LOG_FILE)
		except:
			pass
		f = open(self.BULK_LOG_FILE, "w+")
		
	
	def bulk_log(self):
		f = open(self.BULK_LOG_FILE, "a+")
		f.write (self.output_filename + "\n")
		f.close()

	def bulk_recompile(self):
		contents = ""
		print ("Recompiling individual files into single file")
		print ("Reading logs from file", self.BULK_LOG_FILE)
		merged_filename = "bulk_migration_" + self.bulk_migration_profile + ".xml"
		merged_filename2	= os.path.join(self.XML_OUT_DIR, merged_filename)
		files = []
		f = open(self.BULK_LOG_FILE, "r")
		lines = f.readlines()
		for line in lines:
			files.append (line.replace("\n", ""))
		f.close()

		# Business rules for recombining a file
		# If there is only a single file, then just use the file in its entirety, do not strip any part
		
		buffer = ""
		tally = -1
		file_count = len(files)

		# Need to go through every file in the list and strip out those that do not exist
		for i in range(file_count -1, -1, -1):
			file = files[i]
			file2	= os.path.join(self.XML_OUT_DIR, file)
			if not os.path.isfile(file2):
				files.pop(i)

		file_count = len(files)
		for file in files:
			file2	= os.path.join(self.XML_OUT_DIR, file)
			if os.path.isfile(file2):
				tally += 1
				f = open(file2, "r")
				contents = f.read()
				if tally == 0:
					if file_count > 1:
						contents = contents.replace("</env:envelope>", "")
				elif tally == file_count - 1:
					contents = contents.replace('<?xml version="1.0" encoding="UTF-8"?>', "")
					contents = contents.replace('<env:envelope xmlns="urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0" xmlns:env="urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0" id="[ID]">', "")
				else:
					contents = contents.replace("</env:envelope>", "")
					contents = contents.replace('<?xml version="1.0" encoding="UTF-8"?>', "")
					contents = contents.replace('<env:envelope xmlns="urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0" xmlns:env="urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0" id="[ID]">', "")
			
				buffer += contents
		if contents.find("</env:envelope>") == -1:
			contents += "\n</env:envelope>"

		f = open(merged_filename2, "w+")
		f.write (buffer)
		f.close()

		for file in files:
			file2 = os.path.join(self.XML_OUT_DIR, file)
			if os.path.isfile(file2):
				os.remove(file2)
		print ("Merged file written to", merged_filename)
		self.validate(merged_filename)
		self.output_filename = os.path.join(self.XML_OUT_DIR,	merged_filename)
		self.copy_to_custom_import_folder()
		sys.exit()


	def get_migrate_measure_type_parameters(self):
		# Get the date - this will be appended to the output filename 
		d = datetime.now()
		d2 = d.strftime("%Y-%m-%d")

		# Get the measure types that have been specified, or more likely the shortcut
		if (len(sys.argv) > 3):
			self.measure_type_string = sys.argv[3].strip()
			if len(self.measure_type_string) == 0:
				print ("You have opted to migrate by measure type, but not specified the measure type")
				sys.exit()

			if self.measure_type_string in ("credibility", "credibilitychecks", "cred"):	# credibility checks
				self.measure_type_list = self.credibility_list

			elif self.measure_type_string in ("supp", "supplementaryunits", "sup"):			# supplementary units
				self.measure_type_list = self.supplementary_list

			elif self.measure_type_string in ("quota", "quotas", "wto_quotas", "wto", "q"):	# quotas
				self.measure_type_list = self.quota_list

			elif self.measure_type_string in ("mfn"):										# MFNs
				self.measure_type_list = self.mfn_list

			elif self.measure_type_string in ("suspension", "suspensions", "susp"):			# suspensions
				self.measure_type_list = self.suspension_list

			elif self.measure_type_string in ("surveillance", "surv"):						# surveillance
				self.measure_type_list = self.surveillance_list

			elif self.measure_type_string in ("agri"):										# agricultural safeguards
				self.measure_type_list = self.agri_list

			elif self.measure_type_string in ("remedies"):									# trade remedies unwanted measures
				self.measure_type_list = self.remedies_list

			else:
				self.measure_type_list = []
				self.measure_type_list.append(self.measure_type_string)

		# Get the future regulation ID
		if (len(sys.argv) > 4):
			self.future_regulation_id = sys.argv[4].strip()
			if len(self.future_regulation_id) != 8:
				print ("Erroneous future regulation string - please fix")
				sys.exit()

		# Set the filename for measure exports
		if self.action_string == "terminate":
			#self.output_filename = self.scope + "_end_" + self.measure_type_string + "_" + d2 + ".xml"
			self.output_filename = self.scope + "_end_" + self.measure_type_string + ".xml"
		else:
			#self.output_filename = self.scope + "_end_" + self.measure_type_string + "_new_regulation_" + self.future_regulation_id + "_" + d2 + ".xml"
			self.output_filename = self.scope + "_end_" + self.measure_type_string + "_new_regulation_" + self.future_regulation_id + ".xml"


	def get_migrate_country_parameters(self):
		if (len(sys.argv) > 3):
			self.country_profile = sys.argv[3].strip()
			self.get_country_list()
		else:
			print ("No country specified - quitting")
			sys.exit()

		self.output_filename = self.scope + "_" + self.action_string + "_" + self.country_profile

		if (len(sys.argv) > 4):
			self.future_regulation_id = sys.argv[4].strip()
			self.output_filename += "_" + self.future_regulation_id
			if len(self.future_regulation_id) != 8:
				print ("Erroneous future regulation string - please fix")
				sys.exit()
		else:
			if self.action_string != "terminate":
				print ("No 'to-be' regulation specified - quitting")
				sys.exit()
		
		if (len(sys.argv) > 5):
			if self.action_verbatim not in ("n", "newstart"):
				print ("Error - cannot terminate or migrate measures when a destination country is specified in parameter 5")
				sys.exit()
			self.destination_geographical_area_id	= sys.argv[5].strip()
			self.get_geographical_area_sid()
			self.output_filename += "_" + self.destination_geographical_area_id


		self.output_filename += ".xml"


	def get_geographical_area_sid(self):
		sql = """
		select geographical_area_sid
		from geographical_area_descriptions where geographical_area_id = 'LI'
		order by geographical_area_description_period_sid desc limit 1
		"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		if len(rows) > 0:
			rw = rows[0]
			self.destination_geographical_area_sid	= rw[0]


	def get_quota_parameters(self):
		self.scope = "quotas"
		if sys.argv[0] == "terminate_quota_definitions.py":
			self.action_string = "terminate"
			self.output_filename = "terminate_quota_definitions.xml"
		elif sys.argv[0] == "create_new_fcfs_quotas.py":
			self.action_string = "create"
			self.output_filename = "create_quota_definitions.xml"
		
		self.action_string = "create"
		self.country_profile = ""
		self.regulation_string = ""
		self.measure_type_list = ""
		self.future_regulation_id = ""
	
	def get_country_list(self):
		try:
			self.country_codes = self.all_country_profiles[self.country_profile]["country_codes"]
		except:
			self.country_codes = []
			self.country_codes.append (self.country_profile)


	def get_templates(self):
		# Get template - envelope
		filename = os.path.join(self.TEMPLATE_DIR,	"envelope.xml")
		file = open(filename, mode = 'r')
		self.template_envelope = file.read()
		file.close()

		# Get template - transaction
		filename = os.path.join(self.TEMPLATE_DIR,	"transaction.xml")
		file = open(filename, mode = 'r')
		self.template_transaction = file.read()
		file.close()

		# Get template - measure
		filename = os.path.join(self.TEMPLATE_DIR,	"measure.xml")
		file = open(filename, mode = 'r')
		self.template_measure = file.read()
		file.close()

		# Get template - measure.component
		filename = os.path.join(self.TEMPLATE_DIR,	"measure.component.xml")
		file = open(filename, mode = 'r')
		self.template_measure_component = file.read()
		file.close()

		# Get template - measure.condition
		filename = os.path.join(self.TEMPLATE_DIR,	"measure.condition.xml")
		file = open(filename, mode = 'r')
		self.template_measure_condition = file.read()
		file.close()

		# Get template - measure.condition.component
		filename = os.path.join(self.TEMPLATE_DIR,	"measure.condition.component.xml")
		file = open(filename, mode = 'r')
		self.template_measure_condition_component = file.read()
		file.close()

		# Get template - quota.definition
		filename = os.path.join(self.TEMPLATE_DIR,	"quota.definition.xml")
		file = open(filename, mode = 'r')
		self.template_quota_definition = file.read()
		file.close()

		# Get template - quota.association
		filename = os.path.join(self.TEMPLATE_DIR,	"quota.association.xml")
		file = open(filename, mode = 'r')
		self.template_quota_association = file.read()
		file.close()

		# Get template - quota.association.insert
		filename = os.path.join(self.TEMPLATE_DIR,	"quota.association.insert.xml")
		file = open(filename, mode = 'r')
		self.template_quota_association_insert = file.read()
		file.close()

		# Get template - quota.suspension.period
		filename = os.path.join(self.TEMPLATE_DIR,	"quota.suspension.period.xml")
		file = open(filename, mode = 'r')
		self.template_quota_suspension_period = file.read()
		file.close()

		# Get template - quota.blocking.period
		filename = os.path.join(self.TEMPLATE_DIR,	"quota.blocking.period.xml")
		file = open(filename, mode = 'r')
		self.template_quota_blocking_period = file.read()
		file.close()

		# Get template - measure.excluded.geographical.area
		filename = os.path.join(self.TEMPLATE_DIR,	"measure.excluded.geographical.area.xml")
		file = open(filename, mode = 'r')
		self.template_measure_excluded_geographical_area = file.read()
		file.close()

		# Get template - footnote.association.measure
		filename = os.path.join(self.TEMPLATE_DIR,	"footnote.association.measure.xml")
		file = open(filename, mode = 'r')
		self.template_footnote_association_measure = file.read()
		file.close()

		# Get template - measure_partial_temporary_stop
		filename = os.path.join(self.TEMPLATE_DIR,	"measure.partial.temporary.stop.xml")
		file = open(filename, mode = 'r')
		self.template_measure_partial_temporary_stop = file.read()
		file.close()

	def get_future_quota_definitions(self):
		# Get all future definitions, which will be killed later
		sql = """SELECT quota_definition_sid, quota_order_number_id, validity_start_date, validity_end_date, quota_order_number_sid, volume,
		initial_volume, measurement_unit_code, maximum_precision, critical_state, critical_threshold, monetary_unit_code, 
		measurement_unit_qualifier_code, description
		FROM quota_definitions WHERE validity_start_date >= '""" + self.critical_date_string + """'
		ORDER BY quota_order_number_id"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		self.quota_definition_list = []
		for row in rows:
			quota_definition_sid			= row[0]
			quota_order_number_id			= row[1]
			validity_start_date				= row[2]
			validity_end_date				= row[3]
			quota_order_number_sid			= row[4]
			volume							= row[5]
			initial_volume					= row[6]
			measurement_unit_code			= row[7]
			maximum_precision				= row[8]
			critical_state					= row[9]
			critical_threshold				= row[10]
			monetary_unit_code				= row[11]
			measurement_unit_qualifier_code	= row[12]
			description						= row[13]

			q = quota_definition(quota_definition_sid, quota_order_number_id, validity_start_date, validity_end_date, quota_order_number_sid, volume,
		initial_volume, measurement_unit_code, maximum_precision, critical_state, critical_threshold, monetary_unit_code, 
		measurement_unit_qualifier_code, description, "delete")
			self.quota_definition_list.append(q)

	def get_current_quota_definitions(self):
		# Get all current quota definitions, which we will work out if they need to be
		# recreated in other scripts
		# we are using all definitions that started on 1st Jan 2018 or later as examples
		# The presumption will be that there will have been a truncation and deltion run prior to this
		# of any quota definitions that areeither start after brexit or straddle brexit

		# When looking ot create future quota definitions, don't forget to check what quota definitions
		# may not have been created by the EU yet - it's conceivable that we are still waiting
		# for definitions to come through, so we will be creating some from extrapolation of old patterns

		self.quota_definition_sid_mapping_list = []

		sql = """SELECT quota_definition_sid, quota_order_number_id, validity_start_date, validity_end_date, quota_order_number_sid, volume,
		initial_volume, measurement_unit_code, maximum_precision, critical_state, critical_threshold, monetary_unit_code, 
		measurement_unit_qualifier_code, description
		FROM quota_definitions WHERE validity_start_date >= '2018-01-01'
		ORDER BY quota_order_number_id, validity_start_date;"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		self.quota_definition_list = []
		for row in rows:
			quota_definition_sid			= row[0]
			quota_order_number_id			= row[1]
			validity_start_date				= row[2]
			validity_end_date				= row[3]
			quota_order_number_sid			= row[4]
			volume							= row[5]
			initial_volume					= row[6]
			measurement_unit_code			= row[7]
			maximum_precision				= row[8]
			critical_state					= row[9]
			critical_threshold				= row[10]
			monetary_unit_code				= row[11]
			measurement_unit_qualifier_code	= row[12]
			description						= row[13]

			q = quota_definition(quota_definition_sid, quota_order_number_id, validity_start_date, validity_end_date, quota_order_number_sid, volume,
		initial_volume, measurement_unit_code, maximum_precision, critical_state, critical_threshold, monetary_unit_code, 
		measurement_unit_qualifier_code, description, "insert")
			self.quota_definition_list.append(q)

		# Assign the definitions to the quota order numbers, such that can then go round the data once more
		# and work out what the future definitions are meant to be - in most cases it will be simple
		# but there will be complex cases, where we need to extrapolate from old data

		self.d("Assigning quota definitions to quota order numbers")
		p = ProgressBar(len(self.quota_order_number_list), sys.stdout)
		cnt = 1
		start = 0
		for qon in self.quota_order_number_list:
			for i in range(start, len(self.quota_definition_list)):
				qd = self.quota_definition_list[i]
				if (qon.quota_order_number_id == qd.quota_order_number_id):
					qon.quota_definition_list.append(qd)
					start = i

			p.print_progress(cnt)
			cnt +=1

	def define_future_quota_definitions(self):
		# Check if there are any items with a missing quota_order_number_sid
		# as these break the extract when the data is missing
		# Insert from the quota order number table, if found
		for qon in self.quota_order_number_list:
			for qd in qon.quota_definition_list:
				if qd.quota_order_number_sid is None:
					qd.quota_order_number_sid = qon.quota_order_number_sid

		# Then work out what sort of period types we have and how many definitions to create.
		# Only after having done that should we look at creating the data
		for qon in self.quota_order_number_list:
			if len(qon.quota_definition_list) == 1:
				qon.single_definition_in_year = True
			else:
				# If the period type of any of the definitions is Annual, then there is only one in a year
				for qd in qon.quota_definition_list:
					if qd.period_type == "Annual":
						qon.single_definition_in_year = True
						break

				if qon.single_definition_in_year == False:
					if len(qon.quota_definition_list) == 2:
						day1	= qon.quota_definition_list[0].validity_start_day
						day2	= qon.quota_definition_list[1].validity_start_day
						month1	= qon.quota_definition_list[0].validity_start_month
						month2	= qon.quota_definition_list[1].validity_start_month
						if (day1 == day2) and (month1 == month2):
							qon.single_definition_in_year = True
					elif len(qon.quota_definition_list) == 0:
						qon.omit = True
					else:
						# print ("More than 2 definitions", qon.quota_order_number_id)
						# There only actually seem to be three of these, as follows
						# 091104 = tomatoes
						# 091193 = beef
						# 092202 = beef
						# All the future data is there, so there is no work to do
						pass

		# Check if the quota is an annual quota: if so, then there can only be one definition in a year
		# If so, then delete all the other definitions from the future association list

		for qon in self.quota_order_number_list:
			if qon.single_definition_in_year == True:
				if len(qon.quota_definition_list) > 1:
					# Delete all but the latest quota definition from the quota order number
					# If we have discerned already that this is a quota with a single definition
					# in a year
					for i in range(0, len(qon.quota_definition_list) - 1):
						del qon.quota_definition_list[i]
				else:
					pass

				
		for qon in self.quota_order_number_list:
			if len(qon.quota_definition_list) == 1:
				qd = qon.quota_definition_list[0]
				if qd.validity_start_date >= self.critical_date_plus_one:
					# If the start date of the definition is in the future after Brexit, then leave it
					# and its end date alone without modification
					#print (qon.quota_order_number_id, "Found a definition that is in the future, so leaving")
					pass
				elif (qd.validity_start_date < self.critical_date_plus_one) and (qd.validity_end_date > self.critical_date_plus_one):
					#print (qon.quota_order_number_id, "Found a definition that straddles Brexit, so amending")
					qd.validity_start_date = self.critical_date_plus_one
				elif qd.validity_start_date < self.critical_date_plus_one:
					#print (qon.quota_order_number_id, qd.validity_start_date, qd.validity_end_date)
					#print (qon.quota_order_number_id, "Found a definition that fully precedes Brexit, so deleting")
					qon.omit = True
					#qd.validity_start_date = self.critical_date_plus_one
			else:
				for qd in qon.quota_definition_list:
					if qd.validity_start_date >= self.critical_date_plus_one:
						# If the start date of the definition is in the future after Brexit, then leave it
						# and its end date alone without modification
						#print (qon.quota_order_number_id, "Found a definition that is in the future, so leaving")
						pass
					elif (qd.validity_start_date < self.critical_date_plus_one) and (qd.validity_end_date > self.critical_date_plus_one):
						#print (qon.quota_order_number_id, "Found a definition that straddles Brexit, so amending")
						qd.validity_start_date = self.critical_date_plus_one
					elif qd.validity_start_date < self.critical_date_plus_one:
						#print (qon.quota_order_number_id, qd.validity_start_date, qd.validity_end_date)
						#print (qon.quota_order_number_id, "Found a definition that fully precedes Brexit, so deleting")
						qd.omit = True
						#qd.validity_start_date = self.critical_date_plus_one


		# Now get the balances from the new balance file
		print ("\n\n")
		self.d("Inserting UK quota balances")
		p = ProgressBar(len(self.quota_order_number_list), sys.stdout)
		cnt = 1
		for qon in self.quota_order_number_list:
			if qon.omit == False:
				for qd in qon.quota_definition_list:
					if qd.omit == False:
						qd.definition = "DIT will insert this here - need to get a list somewhere, innit"
						#print (qon.quota_order_number_id, qd.validity_start_date, qd.validity_end_date)
						for bal in self.quota_balance_list:
							d2 = datetime.strptime(str(qd.validity_start_date), '%Y-%m-%d %H:%M:%S')
							if (bal.quota_order_number_id == qon.quota_order_number_id):
								qd.initial_volume = bal.y1_balance
								qd.volume = bal.y1_balance
								qd.critical_state = "Y"
								qd.action = "insert"
								break
			p.print_progress(cnt)
			cnt +=1

	def get_straddling_quota_definitions(self, action):
		# Get all future definitions, which will be killed later
		sql = """SELECT quota_definition_sid, quota_order_number_id, validity_start_date, validity_end_date, quota_order_number_sid, volume,
		initial_volume, measurement_unit_code, maximum_precision, critical_state, critical_threshold, monetary_unit_code, 
		measurement_unit_qualifier_code, description
		FROM quota_definitions WHERE validity_start_date <= '""" + self.critical_date_string + """' AND validity_end_date >= '""" + self.critical_date_plus_one_string + """'
		ORDER BY quota_order_number_id"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		self.quota_definition_list = []
		for row in rows:
			quota_definition_sid			= row[0]
			quota_order_number_id			= row[1]
			validity_start_date				= row[2]
			validity_end_date				= row[3]
			quota_order_number_sid			= row[4]
			volume							= row[5]
			initial_volume					= row[6]
			measurement_unit_code			= row[7]
			maximum_precision				= row[8]
			critical_state					= row[9]
			critical_threshold				= row[10]
			monetary_unit_code				= row[11]
			measurement_unit_qualifier_code	= row[12]
			description						= row[13]

			q = quota_definition(quota_definition_sid, quota_order_number_id, validity_start_date, validity_end_date, quota_order_number_sid, volume,
		initial_volume, measurement_unit_code, maximum_precision, critical_state, critical_threshold, monetary_unit_code, 
		measurement_unit_qualifier_code, description, action)
			self.quota_definition_list.append(q)

	def kill_future_quota_definitions(self):
		for obj in self.quota_definition_list:
			self.content += obj.xml()

	def truncate_straddling_quota_definitions(self):
		for obj in self.quota_definition_list:
			self.content += obj.xml()

	def insert_straddling_quota_definitions(self):
		for obj in self.quota_definition_list:
			obj.validity_start_date = g.app.critical_date_plus_one
			self.content += obj.xml()

	def write_uk_future_quota_definitions(self):
		for qon in self.quota_order_number_list:
			if qon.omit == False:
				for obj in qon.quota_definition_list:
					if obj.omit == False:
						self.content += obj.xml()

	def write_uk_future_quota_associations(self):
		for qon in self.quota_order_number_list:
			if qon.omit == False:
				for qd in qon.quota_definition_list:
					if qd.omit == False:
						for assoc in qd.quota_association_list:
							self.content += assoc.xml()


	def get_associations(self):
		self.d("Getting quota associations")
		sql = """SELECT main_quota_definition_sid, sub_quota_definition_sid, relation_type, coefficient FROM
		quota_associations WHERE main_quota_definition_sid IN (""" + self.definition_clause + """)
		OR sub_quota_definition_sid IN (""" + self.definition_clause + """)
		ORDER BY 1, 2;"""

		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()

		self.quota_association_list = []
		for item in rows:
			main_quota_definition_sid	= item[0]
			sub_quota_definition_sid	= item[1]
			relation_type				= item[2]
			coefficient					= item[3]

			obj = quota_association(main_quota_definition_sid, sub_quota_definition_sid, relation_type, coefficient)
			self.quota_association_list.append(obj)

		self.d("Assigning quota associations to quota definitions")
		p = ProgressBar(len(self.quota_association_list), sys.stdout)
		cnt = 1
		start = 0
		for a in self.quota_association_list:
			for i in range(start, len(self.quota_definition_list_combined)):
				q = self.quota_definition_list_combined[i]
				if (a.main_quota_definition_sid == q.quota_definition_sid) or (a.sub_quota_definition_sid == q.quota_definition_sid):
					q.quota_association_list.append(a)
					start = i
					break
			p.print_progress(cnt)
			cnt +=1
		print ("\n")

	def get_current_quota_associations(self):
		self.d("Getting future quota associations, to be deleted")
		sql = """SELECT DISTINCT main_quota_definition_sid, sub_quota_definition_sid, relation_type, coefficient FROM quota_associations
		WHERE main_quota_definition_sid IN (SELECT quota_definition_sid FROM quota_definitions WHERE validity_start_date >= '2018-01-01')
		OR sub_quota_definition_sid IN (SELECT quota_definition_sid FROM quota_definitions WHERE validity_start_date >= '2018-01-01')"""

		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()

		self.quota_association_list = []
		for item in rows:
			main_quota_definition_sid	= item[0]
			sub_quota_definition_sid	= item[1]
			relation_type				= item[2]
			coefficient					= item[3]

			obj = quota_association(main_quota_definition_sid, sub_quota_definition_sid, relation_type, coefficient, "insert")
			self.quota_association_list.append(obj)

		self.d("Assigning quota associations to quota definitions")
		p = ProgressBar(len(self.quota_association_list), sys.stdout)
		cnt = 1
		start = 0
		for assoc in self.quota_association_list:
			for i in range(start, len(self.quota_definition_list)):
				qd = self.quota_definition_list[i]
				if (assoc.main_quota_definition_sid == qd.quota_definition_sid) or (assoc.sub_quota_definition_sid == qd.quota_definition_sid):
					qd.quota_association_list.append(assoc)
					start = i
			p.print_progress(cnt)
			cnt +=1
		print ("\n")


	def get_future_quota_associations(self):
		self.d("Getting future quota associations, to be deleted")
		sql = """SELECT DISTINCT main_quota_definition_sid, sub_quota_definition_sid, relation_type, coefficient FROM quota_associations
		WHERE main_quota_definition_sid IN (SELECT quota_definition_sid FROM quota_definitions WHERE validity_start_date >= '""" + self.critical_date_string + """')
		OR sub_quota_definition_sid IN (SELECT quota_definition_sid FROM quota_definitions WHERE validity_start_date >= '""" + self.critical_date_string + """')"""

		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()

		self.quota_association_list = []
		for item in rows:
			main_quota_definition_sid	= item[0]
			sub_quota_definition_sid	= item[1]
			relation_type				= item[2]
			coefficient					= item[3]

			obj = quota_association(main_quota_definition_sid, sub_quota_definition_sid, relation_type, coefficient, "delete")
			self.quota_association_list.append(obj)

		self.d("Assigning quota associations to quota definitions")
		p = ProgressBar(len(self.quota_association_list), sys.stdout)
		cnt = 1
		start = 0
		for a in self.quota_association_list:
			for i in range(start, len(self.quota_definition_list)):
				q = self.quota_definition_list[i]
				if (a.main_quota_definition_sid == q.quota_definition_sid) or (a.sub_quota_definition_sid == q.quota_definition_sid):
					q.quota_association_list.append(a)
					start = i
					break
			p.print_progress(cnt)
			cnt +=1
		print ("\n")


	def get_future_quota_suspension_periods(self):
		self.d("Getting future quota suspensions, to be deleted")
		sql = """SELECT quota_suspension_period_sid, quota_definition_sid, suspension_start_date, suspension_end_date, description
		FROM quota_suspension_periods
		WHERE quota_definition_sid IN (SELECT quota_definition_sid FROM quota_definitions WHERE validity_start_date >= '""" + self.critical_date_string + """')"""

		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()

		self.quota_suspension_period_list = []
		for item in rows:
			quota_suspension_period_sid	= item[0]
			quota_definition_sid		= item[1]
			suspension_start_date		= item[2]
			suspension_end_date			= item[3]
			description					= item[4]

			obj = quota_suspension_period(quota_suspension_period_sid, quota_definition_sid, suspension_start_date, suspension_end_date, description)
			self.quota_suspension_period_list.append(obj)

		self.d("Assigning quota suspension periods to quota definitions")

		p = ProgressBar(len(self.quota_suspension_period_list), sys.stdout)
		cnt = 1
		start = 0
		for obj in self.quota_suspension_period_list:
			for i in range(start, len(self.quota_definition_list)):
				q = self.quota_definition_list[i]
				if obj.quota_definition_sid == q.quota_definition_sid:
					q.quota_suspension_period_list.append(obj)
					start = i
					break
			p.print_progress(cnt)
			cnt +=1
		print ("\n")


	def get_future_quota_blocking_periods(self):
		self.d("Getting future quota blockings, to be deleted")
		sql = """SELECT quota_blocking_period_sid, quota_definition_sid, blocking_start_date, blocking_end_date, blocking_period_type, description
		FROM quota_blocking_periods
		WHERE quota_definition_sid IN (SELECT quota_definition_sid FROM quota_definitions WHERE validity_start_date >= '""" + self.critical_date_string + """')"""

		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()

		self.quota_blocking_period_list = []
		for item in rows:
			quota_blocking_period_sid	= item[0]
			quota_definition_sid		= item[1]
			blocking_start_date			= item[2]
			blocking_end_date			= item[3]
			blocking_period_type		= item[4]
			description					= item[5]

			obj = quota_blocking_period(quota_blocking_period_sid, quota_definition_sid, blocking_start_date, blocking_end_date, blocking_period_type, description)
			self.quota_blocking_period_list.append(obj)

		self.d("Assigning quota blocking periods to quota definitions")

		p = ProgressBar(len(self.quota_blocking_period_list), sys.stdout)
		cnt = 1
		start = 0
		for obj in self.quota_blocking_period_list:
			for i in range(start, len(self.quota_definition_list)):
				q = self.quota_definition_list[i]
				q.quota_blocking_period_list = []
				if obj.quota_definition_sid == q.quota_definition_sid:
					q.quota_blocking_period_list.append(obj)
					start = i
					break
			p.print_progress(cnt)
			cnt +=1
		print ("\n")

	def list_to_string(self, lst, data_type="string"):
		s = ""
		if data_type == "string":
			for item in lst:
				s += "'" + item + "', "
		elif data_type == "number":
			for item in lst:
				s += item + ", "
		s = s.strip()
		s = s.strip(",")
		return (s)

	def list_to_sql(self, my_list):
		s = ""
		if my_list != "":
			for o in my_list:
				s += "'" + o + "', "
			s = s.strip()
			s = s.strip(",")
		return (s)

	def get_siv_measures(self):
		pass

	def get_measures(self):
		# Depending on the scope, this function retrieves all matching measures from the database
		# For quotas, it pulls back literally all quota measures (not sure this will stay)
		# for measure type, it retrieves all active measures of the specified type (e.g. usage in supp, cred)
		# for regulations, it brings back all active measures belonging to the regulation
		self.d("Deriving measures from database", False)
		# Get the matching measures
		# dq (self.scope)
		country_clause = ""

		#if self.country_limit != "":
		#	country_clause = " AND geographical_area_id = '" + self.country_limit + "' "

		# print ("function get_measures", self.scope)
		#sys.exit()

		
		if self.scope == "country":
			my_geo_ids = self.list_to_sql(self.country_codes)

			my_measure_types = self.list_to_sql(self.preferential_measure_list)

			sql = """SELECT measure_sid, ordernumber, measure_type_id, validity_start_date, validity_end_date,
			geographical_area_id, goods_nomenclature_item_id, additional_code_type_id,
			additional_code_id, reduction_indicator, measure_generating_regulation_role,
			measure_generating_regulation_id, justification_regulation_role, justification_regulation_id,
			stopped_flag, geographical_area_sid, goods_nomenclature_sid,
			additional_code_sid, export_refund_nomenclature_sid
			FROM ml.get_current_measures m WHERE geographical_area_id IN (""" + my_geo_ids + """)
			AND measure_type_id IN (""" + my_measure_types + """) ORDER BY measure_sid
			"""

			sql = """SELECT measure_sid, ordernumber, measure_type_id, m.validity_start_date, m.validity_end_date,
			geographical_area_id, m.goods_nomenclature_item_id, additional_code_type_id,
			additional_code_id, reduction_indicator, measure_generating_regulation_role,
			measure_generating_regulation_id, justification_regulation_role, justification_regulation_id,
			stopped_flag, geographical_area_sid, m.goods_nomenclature_sid,
			additional_code_sid, export_refund_nomenclature_sid
			FROM ml.measures_real_end_dates m, goods_nomenclatures g
			WHERE geographical_area_id IN (""" + my_geo_ids + """)
			and m.goods_nomenclature_item_id = g.goods_nomenclature_item_id
			and g.producline_suffix = '80'
			AND measure_type_id IN (""" + my_measure_types + """)
			and (m.validity_end_date is null or m.validity_end_date > '""" + self.critical_date.strftime("%Y-%m-%d") + """')
			and g.validity_end_date is null
			ORDER BY measure_sid
			"""

		elif self.scope == "quotas":
			sql = """SELECT measure_sid, ordernumber, measure_type_id, validity_start_date, validity_end_date,
			geographical_area_id, goods_nomenclature_item_id, additional_code_type_id,
			additional_code_id, reduction_indicator, measure_generating_regulation_role,
			measure_generating_regulation_id, justification_regulation_role, justification_regulation_id,
			stopped_flag, geographical_area_sid, goods_nomenclature_sid,
			additional_code_sid, export_refund_nomenclature_sid
			FROM ml.measures_real_end_dates m WHERE ordernumber IS NOT NULL
			ORDER BY measure_sid
			"""

		elif self.scope == "measuretypes":
			if self.measure_type_string in ("agri"):
				sql = """
				SELECT measure_sid, ordernumber, measure_type_id, m.validity_start_date, m.validity_end_date,
				geographical_area_id, m.goods_nomenclature_item_id, additional_code_type_id,
				additional_code_id, reduction_indicator, measure_generating_regulation_role,
				measure_generating_regulation_id, justification_regulation_role, justification_regulation_id,
				stopped_flag, geographical_area_sid, m.goods_nomenclature_sid,
				additional_code_sid, export_refund_nomenclature_sid
				FROM ml.measures_real_end_dates m left outer join goods_nomenclatures g
				on m.goods_nomenclature_item_id = g.goods_nomenclature_item_id
				WHERE measure_type_id IN (""" + self.list_to_string(self.measure_type_list) + """)
				and (g.producline_suffix = '80' or g.producline_suffix is null)
				and (g.validity_end_date > '""" + self.critical_date.strftime("%Y-%m-%d") + """' or g.validity_end_date is null)
				AND (m.validity_end_date is null or m.validity_end_date > '""" + self.critical_date.strftime("%Y-%m-%d") + """')
				ORDER BY measure_sid
				"""
			else:
				sql = """
				SELECT measure_sid, ordernumber, measure_type_id, m.validity_start_date, m.validity_end_date,
				geographical_area_id, m.goods_nomenclature_item_id, additional_code_type_id,
				additional_code_id, reduction_indicator, measure_generating_regulation_role,
				measure_generating_regulation_id, justification_regulation_role, justification_regulation_id,
				stopped_flag, geographical_area_sid, m.goods_nomenclature_sid,
				additional_code_sid, export_refund_nomenclature_sid
				FROM ml.measures_real_end_dates m, goods_nomenclatures g
				WHERE measure_type_id IN (""" + self.list_to_string(self.measure_type_list) + """)
				""" + country_clause + """
				and m.goods_nomenclature_item_id = g.goods_nomenclature_item_id
				and g.producline_suffix = '80' and
				(g.validity_end_date > '""" + self.critical_date.strftime("%Y-%m-%d") + """' or g.validity_end_date is null)
				AND (m.validity_end_date is null or m.validity_end_date > '""" + self.critical_date.strftime("%Y-%m-%d") + """')
				ORDER BY measure_sid
				"""

		elif self.scope == "regulation":
			if "," in self.regulation_string:
				clause = "AND measure_generating_regulation_id IN (" + self.regulation_string + ") "
			else:
				if len(self.regulation_string) == 7:
					clause = "AND measure_generating_regulation_id like '" + self.regulation_string + "%' "
				else:
					clause = "AND measure_generating_regulation_id = '" + self.regulation_string + "' "

			if self.measure_type_string != "":
				clause += " AND measure_type_id = '" + self.measure_type_string + "'"

			omit_measure_type_clause = ""
			for obj in self.omit_measure_types_list:
				omit_measure_type_clause += "'" + obj + "', "
			omit_measure_type_clause = omit_measure_type_clause.strip()
			omit_measure_type_clause = omit_measure_type_clause.strip(",")
			clause += " AND measure_type_id NOT IN (" + omit_measure_type_clause + ") "

			sql = """
			SELECT measure_sid, ordernumber, measure_type_id, m.validity_start_date, m.validity_end_date,
			geographical_area_id, m.goods_nomenclature_item_id, additional_code_type_id,
			additional_code_id, reduction_indicator, measure_generating_regulation_role,
			measure_generating_regulation_id, justification_regulation_role, justification_regulation_id,
			stopped_flag, geographical_area_sid, m.goods_nomenclature_sid,
			additional_code_sid, export_refund_nomenclature_sid
			FROM ml.get_current_measures m, ml.goods_nomenclatures gn
			WHERE m.goods_nomenclature_item_id = gn.goods_nomenclature_item_id
			AND (gn.validity_end_date IS NULL OR gn.validity_end_date > CURRENT_DATE) """ + clause + country_clause + """
			ORDER BY measure_sid
			"""


		# Write the data retrieved to a CSV - just for debug purposes, this is actually not required
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		m = list(rows)
		filename = os.path.join(self.CSV_DIR, "measures.csv")
		with open(filename, mode='w', newline='', encoding="utf-8") as file:
			writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
			writer.writerow(["Measure SID", "Order number", "Measure type", "Start date", "End date",
			"Geo", "Commodity", "additional_code_type_id",
			"additional_code_id", "reduction_indicator", "measure_generating_regulation_role",
			"measure_generating_regulation_id", "justification_regulation_role", "justification_regulation_id",
			"stopped_flag", "geographical_area_sid", "goods_nomenclature_sid",
			"additional_code_sid", "export_refund_nomenclature_sid"])

			for item in m:
				writer.writerow(item)
		file.close()

		# Transfer the items retrieved from the database into a Python list entitles "measure_list"
		self.d("Creating list of measures")
		self.measure_list = []
		
		self.ignore_count	= 0
		self.enddate_count	= 0
		self.delete_count	= 0
		self.recreate_count	= 0

		for item in m:
			measure_sid 						= item[0]
			ordernumber							= item[1]
			measure_type_id						= item[2]
			validity_start_date					= item[3]
			validity_end_date					= item[4]
			geographical_area_id				= item[5]
			goods_nomenclature_item_id			= item[6]
			additional_code_type_id				= item[7]
			additional_code_id					= item[8]
			reduction_indicator					= item[9]
			measure_generating_regulation_role	= item[10]
			measure_generating_regulation_id	= item[11]
			justification_regulation_role		= item[12]
			justification_regulation_id			= item[13]
			stopped_flag						= item[14]
			geographical_area_sid				= item[15]
			goods_nomenclature_sid				= item[16]
			additional_code_sid					= item[17]
			export_refund_nomenclature_sid		= item[18]

			measure_object = measure(measure_sid, ordernumber, measure_type_id,
			validity_start_date, validity_end_date, geographical_area_id, goods_nomenclature_item_id,
			additional_code_type_id, additional_code_id, reduction_indicator, measure_generating_regulation_role,
			measure_generating_regulation_id, justification_regulation_role, justification_regulation_id,
			stopped_flag, geographical_area_sid, goods_nomenclature_sid,
			additional_code_sid, export_refund_nomenclature_sid)

			self.measure_list.append(measure_object)

		if len(self.measure_list) == 0:
			self.d("No matching measures found - exiting", False)
			sys.exit()

		# Write the state of play to the screen
		self.d("Found " + str(len(self.measure_list)) + " measures in total")
		if self.action_string == "terminate":
			self.d("Found " + str(self.enddate_count) + " measures to end date (straddle Brexit)")
			self.d("Found " + str(self.ignore_count) + " measures to ignore (end before Brexit)")
			self.d("Found " + str(self.delete_count) + " measures to delete (start after Brexit)\n")
		else:
			self.d("Found " + str(self.enddate_count) + " measures to end date and recreate (straddle Brexit)")
			self.d("Found " + str(self.ignore_count) + " measures to ignore (end before Brexit)")
			self.d("Found " + str(self.recreate_count) + " measures to delete and recreate (start after Brexit)\n")

		# Very dull, but we need to check if the measures still exist on the staging database
		# If they do not, then we need to omit them completely from processing
		for m in self.measure_list:
			sql = "select * from measures where measure_sid = " + str(m.measure_sid)
			cur = self.conn_staging.cursor()
			cur.execute(sql)
			rows = cur.fetchall()
			if len(rows) == 0:
				m.omit = True
				print ("Missing measure on ", str(m.measure_sid), m.goods_nomenclature_item_id, m.geographical_area_id)
		
		# Get a list of all measures into a string for use in the SQL statements
		self.measure_clause = ""
		for m in self.measure_list:
			self.measure_clause += str(m.measure_sid) + ", "
		self.measure_clause = self.measure_clause.strip()
		self.measure_clause = self.measure_clause.strip(",")

		# So this function has literally just got hold of the measures and that's it
		# no actions, and no presumptions based on terminate vs. restart

	def get_measure_components(self):
		# Get components related to all these measures
		self.d("Getting measure components")

		sql = """SELECT measure_sid, duty_expression_id, duty_amount, monetary_unit_code,
		measurement_unit_code, measurement_unit_qualifier_code FROM measure_components
		WHERE measure_sid IN (""" + self.measure_clause + """)
		ORDER BY measure_sid, duty_expression_id"""

		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()

		self.measure_component_list = []
		for component in rows:
			measure_sid						= component[0]
			duty_expression_id				= component[1]
			duty_amount						= component[2]
			monetary_unit_code				= component[3]
			measurement_unit_code			= component[4]
			measurement_unit_qualifier_code	= component[5]

			measure_component_object = measure_component(measure_sid, duty_expression_id, duty_amount,
			monetary_unit_code, measurement_unit_code, measurement_unit_qualifier_code)
			self.measure_component_list.append(measure_component_object)

		
		# If we are removing SIVs, then we need to add back in the simple ad valorem duty
		# which is always going to be "0.0%"
		if self.remove_SIVs:
			for siv_measure_sid in self.measures_with_sivs_list:
				measure_sid						= siv_measure_sid
				duty_expression_id				= "01"
				duty_amount						= 0
				monetary_unit_code				= ""
				measurement_unit_code			= ""
				measurement_unit_qualifier_code	= ""

				measure_component_object = measure_component(measure_sid, duty_expression_id, duty_amount,
				monetary_unit_code, measurement_unit_code, measurement_unit_qualifier_code)
				#self.measure_component_list.append(measure_component_object)

		
		self.d("Assigning measure components to measures")
		p = ProgressBar(len(self.measure_component_list), sys.stdout)
		cnt = 1
		start = 0
		for c in self.measure_component_list:
			found = False
			for i in range(start, len(self.measure_list)):
				m = self.measure_list[i]
				if c.measure_sid == m.measure_sid:
					found = True
					m.measure_component_list.append(c)
					start = i
				else:
					if found == True:
						break
			p.print_progress(cnt)
			cnt +=1
		print ("\n")

	def get_measure_conditions(self):
		# Get conditions related to all these measures
		self.d("Getting measure conditions")

		sql = """SELECT measure_condition_sid, measure_sid, condition_code, component_sequence_number,
		condition_duty_amount, condition_monetary_unit_code, condition_measurement_unit_code,
		condition_measurement_unit_qualifier_code, action_code, certificate_type_code,
		certificate_code FROM measure_conditions
		WHERE measure_sid IN (""" + self.measure_clause + """)
		ORDER BY measure_sid, component_sequence_number"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()

		self.measure_condition_list = []
		for condition in rows:
			measure_condition_sid						= condition[0]
			measure_sid									= condition[1]
			condition_code								= condition[2]
			component_sequence_number					= condition[3]
			condition_duty_amount						= condition[4]
			condition_monetary_unit_code				= condition[5]
			condition_measurement_unit_code				= condition[6]
			condition_measurement_unit_qualifier_code	= condition[7]
			action_code									= condition[8]
			certificate_type_code						= condition[9]
			certificate_code							= condition[10]

			measure_condition_object = measure_condition(measure_condition_sid, measure_sid, condition_code,
			component_sequence_number, condition_duty_amount, condition_monetary_unit_code,
			condition_measurement_unit_code, condition_measurement_unit_qualifier_code,
			action_code, certificate_type_code, certificate_code)
			self.measure_condition_list.append(measure_condition_object)

		self.d("Assigning measure conditions to measures")
		p = ProgressBar(len(self.measure_condition_list), sys.stdout)
		cnt = 1
		start = 0
		for c in self.measure_condition_list:
			found = False
			for i in range(start, len(self.measure_list)):
			#for m in self.measure_list:
				m = self.measure_list[i]
				if c.measure_sid == m.measure_sid:
					found = True
					m.measure_condition_list.append(c)
					start = i
				else:
					if found == True:
						break
			p.print_progress(cnt)
			cnt +=1
		print ("\n")


	def get_measure_condition_components(self):
		# Get conditions related to all these measures
		self.d("Getting measure condition components")

		sql = """SELECT mc.measure_sid, mcc.measure_condition_sid, mcc.duty_expression_id,
		mcc.duty_amount, mcc.monetary_unit_code,
		mcc.measurement_unit_code, mcc.measurement_unit_qualifier_code, mc.condition_code
		FROM measure_conditions mc, measure_condition_components mcc
		WHERE mc.measure_condition_sid = mcc.measure_condition_sid
		AND mc.measure_sid IN (""" + self.measure_clause + """)
		ORDER BY measure_sid, duty_expression_id"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()

		self.measure_condition_component_list = []
		for component in rows:
			measure_sid						= component[0]
			measure_condition_sid			= component[1]
			duty_expression_id				= component[2]
			duty_amount						= component[3]
			monetary_unit_code				= component[4]
			measurement_unit_code			= component[5]
			measurement_unit_qualifier_code	= component[6]
			condition_code					= component[7]

			measure_condition_component_object = measure_condition_component(measure_sid, measure_condition_sid,
			duty_expression_id, duty_amount, monetary_unit_code, measurement_unit_code,
			measurement_unit_qualifier_code, condition_code)
			self.measure_condition_component_list.append(measure_condition_component_object)

		self.d("Assigning measure condition components to measures")
		p = ProgressBar(len(self.measure_condition_component_list), sys.stdout)
		cnt = 1
		start = 0
		for c in self.measure_condition_component_list:
			found = False
			for i in range(start, len(self.measure_list)):
				m = self.measure_list[i]
				if c.measure_sid == m.measure_sid:
					found = True
					m.measure_condition_component_list.append(c)
					start = i
				else:
					if found == True:
						break
			p.print_progress(cnt)
			cnt +=1
		print ("\n")


	def get_measure_geographical_exclusions(self):
		# Get geographical area exclusions related to all these measures
		self.d("Getting geographical area exclusions")

		sql = """SELECT measure_sid, excluded_geographical_area, geographical_area_sid
		FROM measure_excluded_geographical_areas WHERE measure_sid IN (""" + self.measure_clause + """)
		ORDER BY measure_sid, excluded_geographical_area"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()

		self.measure_excluded_geographical_area_list = []
		for exclusion in rows:
			measure_sid					= exclusion[0]
			excluded_geographical_area	= exclusion[1]
			geographical_area_sid		= exclusion[2]

			exclusion_object = measure_excluded_geographical_area(measure_sid, excluded_geographical_area, geographical_area_sid)
			self.measure_excluded_geographical_area_list.append(exclusion_object)

		self.d("Assigning measure excluded geographical areas to measures")
		p = ProgressBar(len(self.measure_excluded_geographical_area_list), sys.stdout)
		cnt = 1
		start = 0
		for c in self.measure_excluded_geographical_area_list:
			found = False
			for i in range(start, len(self.measure_list)):
				m = self.measure_list[i]
				if c.measure_sid == m.measure_sid:
					found = True
					m.measure_excluded_geographical_area_list.append(c)
					start = i
				else:
					if found == True:
						break
			p.print_progress(cnt)
			cnt +=1
		print ("\n")

	def get_measure_footnotes(self):
		self.d("Getting measure footnote associations")

		sql = """SELECT measure_sid, footnote_type_id, footnote_id
		FROM footnote_association_measures WHERE measure_sid IN (""" + self.measure_clause + """)
		ORDER BY measure_sid, footnote_type_id, footnote_id"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()

		self.measure_footnote_list = []
		for footnote in rows:
			measure_sid			= footnote[0]
			footnote_type_id	= footnote[1]
			footnote_id			= footnote[2]

			footnote_object = measure_footnote(measure_sid, footnote_type_id, footnote_id)
			self.measure_footnote_list.append(footnote_object)

		self.d("Assigning footnotes to measures")
		p = ProgressBar(len(self.measure_footnote_list), sys.stdout)
		cnt = 1
		start = 0
		for c in self.measure_footnote_list:
			found = False
			for i in range(start, len(self.measure_list)):
				m = self.measure_list[i]
				if c.measure_sid == m.measure_sid:
					found = True
					m.measure_footnote_list.append(c)
					start = i
				else:
					if found == True:
						break
			p.print_progress(cnt)
			cnt +=1
		print ("\n")

	def get_measure_partial_temporary_stops(self):
		self.d("Getting measure partial temporary stops")

		sql = """SELECT measure_sid, validity_start_date, validity_end_date, partial_temporary_stop_regulation_id,
		partial_temporary_stop_regulation_officialjournal_number, partial_temporary_stop_regulation_officialjournal_page,
		abrogation_regulation_id, abrogation_regulation_officialjournal_number, abrogation_regulation_officialjournal_page
		FROM measure_partial_temporary_stops WHERE measure_sid IN (""" + self.measure_clause + """)
		ORDER BY measure_sid, validity_start_date;"""
		#dq (sql)
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()

		self.measure_partial_temporary_stop_list = []
		for pts in rows:
			measure_sid													= pts[0]
			validity_start_date											= pts[1]
			validity_end_date											= pts[2]
			partial_temporary_stop_regulation_id						= pts[3]
			partial_temporary_stop_regulation_officialjournal_number	= pts[4]
			partial_temporary_stop_regulation_officialjournal_page		= pts[5]
			abrogation_regulation_id									= pts[6]
			abrogation_regulation_officialjournal_number				= pts[7]
			abrogation_regulation_officialjournal_page					= pts[8]

			obj = measure_partial_temporary_stop(measure_sid, validity_start_date, validity_end_date, partial_temporary_stop_regulation_id,
			partial_temporary_stop_regulation_officialjournal_number, partial_temporary_stop_regulation_officialjournal_page,
			abrogation_regulation_id, abrogation_regulation_officialjournal_number, abrogation_regulation_officialjournal_page)
			self.measure_partial_temporary_stop_list.append(obj)

		self.d("Assigning partial temporary stops to measures")
		p = ProgressBar(len(self.measure_partial_temporary_stop_list), sys.stdout)
		cnt = 1
		start = 0

		for c in self.measure_partial_temporary_stop_list:
			found = False
			for i in range(start, len(self.measure_list)):
				m = self.measure_list[i]
				if c.measure_sid == m.measure_sid:
					found = True
					m.measure_partial_temporary_stop_list.append(c)
					start = i
				else:
					if found == True:
						break
					
			p.print_progress(cnt)
			cnt +=1
		print ("\n")


	def terminate_measures(self):
		self.d("Writing script to terminate / split measures", False)
		
		self.deleted_measure_list	= []
		self.enddated_measure_list	= []
		self.recreated_measure_list	= []

		p = ProgressBar(len(self.measure_list), sys.stdout)
		cnt = 1

		# The following code hunts for SIVs and remove the threshold-based values / replace with standard component
		#self.d("Checking for SIVs")
		for obj in self.measure_list:
			if obj.omit == False:
				my_duty_list = []
				# Search through the measure conditions and if there is a "V" type condition code (an SIV)
				# remove the condition and then add a measure component equivalent to the consensus
				# ad valorem measure condition component's ad valorem component duty amount

				# Here we capture all of the measure condition components (V type)
				# and compile a "duty list", from which we find the ad valorem that we are interested in
				# For MFNs, these should just be deleted, not replaced - all products with SIVs on them are "0"-rated
				# This script is no longer used on MFNs

				for mc in obj.measure_condition_list:
					if mc.condition_code == "V":
						for mcc in obj.measure_condition_component_list:
							if mc.measure_sid == mcc.measure_sid:
								if mcc.duty_expression_id == "01":
									my_duty_list.append (mcc.duty_amount)

				p.print_progress(cnt)
				cnt += 1
				if obj.action == "delete":
					self.deleted_measure_list.append(obj)
					if self.action_verbatim not in ("n", "newstart"):
						self.content += obj.xml()
						self.transaction_id += 1
				
				elif obj.action == "recreate":
					self.recreated_measure_list.append(obj)
					obj.action = "delete"
					if self.action_verbatim not in ("n", "newstart"):
						self.content += obj.xml()
						self.transaction_id += 1
				
				elif obj.action == "enddate":
					self.enddated_measure_list.append(obj)
					if self.action_verbatim not in ("n", "newstart"):
						self.content += obj.xml()
						self.transaction_id += 1

				
				if (self.scope != "measuretypes") or ("103" not in self.measure_type_list):
					#print ("Reviewing SIVs and removing")
					if len(my_duty_list) > 0:
						duty_amount_last = -1
						all_match = True
						for da in my_duty_list:
							if duty_amount_last != -1:
								if da != duty_amount_last:
									all_match = False
									break

							duty_amount_last = da

						if all_match == True:
							#print ("Creating a new component")
							new_component = measure_component(obj.measure_sid, "01", duty_amount_last, "", "", "", obj.goods_nomenclature_item_id)
							mc_found = False
							for mc in obj.measure_component_list:
								if mc.duty_expression_id == "01":
									mc_found = True
									break
							if mc_found == False:
								obj.measure_component_list.append (new_component)
								#print ("Adding an SIV replacement measure for: ", obj.goods_nomenclature_item_id, obj.validity_start_date, obj.measure_sid)

		print ("\n")



	def get_goods_nomenclature_sid(self, goods_nomenclature_item_id):
		return (1)

	def restart_measures(self):
		# These are the new measures which did not previously exist
		# These need to 
		# - Create measures
		# - Create measure components
		
		dealt_with_list = []
		for obj in self.enddated_measure_list:
			dealt_with_list.append (obj.goods_nomenclature_item_id)
		
		
		for obj in self.recreated_measure_list:
			dealt_with_list.append (obj.goods_nomenclature_item_id)
		
		#self.content = ""

		if self.action_string == "restart":
			self.content += "\n<!-- RESTARTS //-->\n\n"
			self.d("Writing script to restart end-dated measures", False)
			p = ProgressBar(len(self.enddated_measure_list), sys.stdout)
			cnt = 1
			for obj in self.enddated_measure_list:
				proceed = False
				if self.scope == "country":
					if obj.measure_type_id in ["142", "145"]:
						proceed = True
				else:
					proceed = True
				if proceed == True:
					p.print_progress(cnt)
					cnt += 1
					obj.action = "restart"
					obj.measure_generating_regulation_id = self.future_regulation_id
					obj.measure_generating_regulation_role = "1"
					self.content += obj.xml()
					self.transaction_id += 1
			print ("\n")
			self.d("Writing script to reinstitute deleted future measures", False)
			p = ProgressBar(len(self.recreated_measure_list), sys.stdout)
			cnt = 1
			for obj in self.recreated_measure_list:
				p.print_progress(cnt)
				cnt += 1
				obj.action = "restart"
				obj.measure_generating_regulation_id = self.future_regulation_id
				obj.measure_generating_regulation_role = "1"
				self.content += obj.xml()
				self.transaction_id += 1
			print ("\n")
		else:
			self.d("No new measures to write - terminate only", False)

	def terminate_definitions(self):
		self.d("Writing script to terminate quota definitions", False)

		#self.deleted_measure_list = []
		p = ProgressBar(len(self.quota_definition_list_combined), sys.stdout)
		cnt = 1
		for obj in self.quota_definition_list_combined:
			p.print_progress(cnt)
			cnt += 1
			if obj.action == "delete":
				#self.deleted_measure_list.append(obj)
				self.content += obj.xml()
				self.transaction_id += 1
			elif obj.action == "split":
				#self.deleted_measure_list.append(obj)
				self.content += obj.xml()
				self.transaction_id += 1
			else:
				pass
		print ("\n")

	def compare(self, a, b):
		mapped = False
		if a.quota_order_number_id == b.quota_order_number_id:
			if a.validity_start_day == b.validity_start_day:
				if a.validity_end_month == b.validity_end_month:
					mapped = True
		return (mapped)

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
			_ = system('clear')

	def connect(self):
		self.conn			= psycopg2.connect("dbname=" + self.DBASE_MIGRATE_MEASURES + " user=postgres password=" + self.p)
		self.conn_staging	= psycopg2.connect("dbname=" + self.DBASE + " user=postgres password=" + self.p)

	def generate_xml_report(self):
		self.d("Generating an XML report", False)
		ET.register_namespace('oub', 'urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0')
		ET.register_namespace('env', 'urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0')
		fname = os.path.join(self.XML_OUT_DIR,	self.output_filename)
		tree = ET.parse(fname)
		root = tree.getroot()
		
		# Get the envelope ID
		out = ""
		out += "envelope id = " + root.get("id") + "\n\n"

		out += "Overall stats\n".upper()
		out += "=============\n"


		# Get number of transactions
		transaction_count = len(root.findall('.//env:transaction', self.namespaces))
		out += "Transaction count = " + str(transaction_count) + "\n"
		
		# Get number of records
		record_count = len(root.findall('.//oub:record', self.namespaces))
		out += "Record count = " + str(record_count) + "\n"
		
		# Get number of measures
		measure_count = len(root.findall('.//oub:measure', self.namespaces))
		out += "Measure count = " + str(measure_count) + "\n"

		# Get number of measure components
		measure_component_count = len(root.findall('.//oub:measure.component', self.namespaces))
		out += "Measure component count = " + str(measure_component_count) + "\n"

		# Get number of measure conditions
		measure_condition_count = len(root.findall('.//oub:measure.condition', self.namespaces))
		out += "Measure condition count = " + str(measure_condition_count) + "\n"

		# Get number of measure condition components
		measure_condition_component_count = len(root.findall('.//oub:measure.condition.component', self.namespaces))
		out += "Measure condition component count = " + str(measure_condition_component_count) + "\n"


		out += "\nInserts\n".upper()
		out += "=======\n"

		# Get number of inserted measures
		measure_count = len(root.findall('.//oub:measure/../../[oub:update.type=3]', self.namespaces))
		out += "Inserted measure count = " + str(measure_count) + "\n"


		fname = self.output_filename.replace(".xml", ".txt")
		fname = os.path.join(self.XML_REPORT_DIR,	fname)
		f = open(fname, "w+", encoding="utf-8")
		f.write(out)
		f.close()

	def validate(self, filename):
		fname = os.path.join(self.XML_OUT_DIR,	filename)

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

	def get_envelope(self):
		# Open the envelope template file
		filename = os.path.join(self.TEMPLATE_DIR,	"envelope.xml")
		file = open(filename, mode = 'r')
		self.envelope_string = file.read()
		self.envelope_string = self.envelope_string.replace("[ID]", self.envelope_id)
		file.close()

	def write_content(self):
		if len(self.content) == 0:
			print ("No matching measures found")
			sys.exit()
		else:
			self.d("Writing file", False)
			self.d("Writing file '" + self.output_filename + "'")
			self.d("to folder " + self.XML_OUT_DIR)
			xml_string = self.template_envelope.replace("[CONTENT]", self.content)
			filename = os.path.join(self.XML_OUT_DIR,	self.output_filename)
			f = open(filename, "w+", encoding="utf-8")
			f.write(xml_string)
			f.close()

	def check_business_rules(self):
		self.d("Validating against business rules", False)
		fname = os.path.join(self.XML_OUT_DIR,	self.output_filename)
		br = business_rules(fname)
		

	def copy_to_custom_import_folder(self):
		if self.scope == "bulk_migration":
			return
		self.d("Copying file to custom import directory", False)
		self.CUSTOM_DIR				= os.path.join(self.BASE_DIR,	"..")
		self.CUSTOM_DIR				= os.path.join(self.CUSTOM_DIR,	"convert_and_import_taric")
		self.CUSTOM_DIR				= os.path.join(self.CUSTOM_DIR,	"xml_in")
		self.CUSTOM_DIR				= os.path.join(self.CUSTOM_DIR,	"custom")
		# Copy from self.output_filename to dest
		file_from	= os.path.join(self.XML_OUT_DIR,	self.output_filename)
		file_to		= os.path.join(self.CUSTOM_DIR,	self.output_filename)
		#print (file_from)
		#print (file_to)
		shutil.copy (file_from, file_to)