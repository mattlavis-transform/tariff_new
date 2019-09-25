import xml.etree.ElementTree as ET
import xmlschema
import psycopg2
import sys
import os
from os import system, name
import csv
import re
import json
import codecs
import uuid
import hashlib
import subprocess
import xml.dom.minidom
from shutil import copyfile
import os.path
import shutil


from datetime import datetime
from log import log

from common.regulation import regulation
from common.classification import classification

from profile.profile_10000_footnote_type							import profile_10000_footnote_type
from profile.profile_10005_footnote_type_description				import profile_10005_footnote_type_description

from profile.profile_11000_certificate_type							import profile_11000_certificate_type
from profile.profile_11005_certificate_type_description				import profile_11005_certificate_type_description

from profile.profile_12000_additional_code_type						import profile_12000_additional_code_type
from profile.profile_12005_additional_code_type_description			import profile_12005_additional_code_type_description

from profile.profile_13000_language									import profile_13000_language
from profile.profile_13005_language_description						import profile_13005_language_description

from profile.profile_14000_measure_type_series						import profile_14000_measure_type_series
from profile.profile_14005_measure_type_series_description			import profile_14005_measure_type_series_description

from profile.profile_15000_regulation_group							import profile_15000_regulation_group
from profile.profile_15005_regulation_group_description				import profile_15005_regulation_group_description

from profile.profile_16000_regulation_role_type						import profile_16000_regulation_role_type
from profile.profile_16005_regulation_role_type_description			import profile_16005_regulation_role_type_description

from profile.profile_17000_publication_sigle						import profile_17000_publication_sigle

from profile.profile_20000_footnote									import profile_20000_footnote
from profile.profile_20005_footnote_description_period				import profile_20005_footnote_description_period
from profile.profile_20010_footnote_description						import profile_20010_footnote_description

from profile.profile_20500_certificate								import profile_20500_certificate
from profile.profile_20505_certificate_description_period			import profile_20505_certificate_description_period
from profile.profile_20510_certificate_description					import profile_20510_certificate_description

from profile.profile_21000_measurement_unit							import profile_21000_measurement_unit
from profile.profile_21005_measurement_unit_description				import profile_21005_measurement_unit_description
from profile.profile_21500_measurement_unit_qualifier				import profile_21500_measurement_unit_qualifier
from profile.profile_21505_measurement_unit_qualifier_description	import profile_21505_measurement_unit_qualifier_description
from profile.profile_22000_measurement								import profile_22000_measurement

from profile.profile_22500_monetary_unit							import profile_22500_monetary_unit
from profile.profile_22505_monetary_unit_description				import profile_22505_monetary_unit_description

from profile.profile_23000_duty_expression							import profile_23000_duty_expression
from profile.profile_23005_duty_expression_description				import profile_23005_duty_expression_description

from profile.profile_23500_measure_type								import profile_23500_measure_type
from profile.profile_23505_measure_type_description					import profile_23505_measure_type_description

from profile.profile_24000_additional_code_type_measure_type		import profile_24000_additional_code_type_measure_type
from profile.profile_24500_additional_code							import profile_24500_additional_code
from profile.profile_24505_additional_code_description_period		import profile_24505_additional_code_description_period
from profile.profile_24510_additional_code_description				import profile_24510_additional_code_description

from profile.profile_25000_geographical_area						import profile_25000_geographical_area
from profile.profile_25005_geographical_area_description_period		import profile_25005_geographical_area_description_period
from profile.profile_25010_geographical_area_description			import profile_25010_geographical_area_description
from profile.profile_25015_geographical_area_membership				import profile_25015_geographical_area_membership


from profile.profile_27500_complete_abrogation_regulation			import profile_27500_complete_abrogation_regulation
from profile.profile_28000_explicit_abrogation_regulation			import profile_28000_explicit_abrogation_regulation
from profile.profile_28500_base_regulation							import profile_28500_base_regulation
from profile.profile_29000_modification_regulation					import profile_29000_modification_regulation
from profile.profile_29500_prorogation_regulation					import profile_29500_prorogation_regulation
from profile.profile_29505_prorogation_regulation_action			import profile_29505_prorogation_regulation_action

from profile.profile_30000_full_temporary_stop_regulation			import profile_30000_full_temporary_stop_regulation
from profile.profile_30005_fts_regulation_action					import profile_30005_fts_regulation_action

from profile.profile_30500_regulation_replacement					import profile_30500_regulation_replacement



from profile.profile_35000_measure_condition_code					import profile_35000_measure_condition_code
from profile.profile_35005_measure_condition_code_description		import profile_35005_measure_condition_code_description
from profile.profile_35500_measure_action							import profile_35500_measure_action
from profile.profile_35505_measure_action_description				import profile_35505_measure_action_description

from profile.profile_36000_quota_order_number						import profile_36000_quota_order_number
from profile.profile_36010_quota_order_number_origin				import profile_36010_quota_order_number_origin
from profile.profile_36015_quota_order_number_origin_exclusion		import profile_36015_quota_order_number_origin_exclusion

from profile.profile_37000_quota_definition							import profile_37000_quota_definition
from profile.profile_37005_quota_association						import profile_37005_quota_association
from profile.profile_37010_quota_blocking_period					import profile_37010_quota_blocking_period
from profile.profile_37015_quota_suspension_period					import profile_37015_quota_suspension_period

from profile.profile_37500_quota_balance_event						import profile_37500_quota_balance_event
from profile.profile_37505_quota_unblocking_event					import profile_37505_quota_unblocking_event
from profile.profile_37510_quota_critical_event						import profile_37510_quota_critical_event
from profile.profile_37515_quota_exhaustion_event					import profile_37515_quota_exhaustion_event
from profile.profile_37520_quota_reopening_event					import profile_37520_quota_reopening_event
from profile.profile_37525_quota_unsuspension_event					import profile_37525_quota_unsuspension_event
from profile.profile_37530_quota_closed_and_balance_transferred_event	import profile_37530_quota_closed_and_balance_transferred_event

from profile.profile_40000_goods_nomenclature						import profile_40000_goods_nomenclature
from profile.profile_40005_goods_nomenclature_indent				import profile_40005_goods_nomenclature_indent
from profile.profile_40010_goods_nomenclature_description_period	import profile_40010_goods_nomenclature_description_period
from profile.profile_40015_goods_nomenclature_description			import profile_40015_goods_nomenclature_description
from profile.profile_40020_footnote_association_goods_nomenclature	import profile_40020_footnote_association_goods_nomenclature
from profile.profile_40035_goods_nomenclature_origin				import profile_40035_goods_nomenclature_origin
from profile.profile_40040_goods_nomenclature_successor				import profile_40040_goods_nomenclature_successor

from profile.profile_43000_measure									import profile_43000_measure
from profile.profile_43005_measure_component						import profile_43005_measure_component
from profile.profile_43010_measure_condition						import profile_43010_measure_condition
from profile.profile_43011_measure_condition_component				import profile_43011_measure_condition_component
from profile.profile_43015_measure_excluded_geographical_area		import profile_43015_measure_excluded_geographical_area
from profile.profile_43020_footnote_association_measure				import profile_43020_footnote_association_measure
from profile.profile_43025_measure_partial_temporary_stop			import profile_43025_measure_partial_temporary_stop

from profile.profile_44000_monetary_exchange_period					import profile_44000_monetary_exchange_period
from profile.profile_44005_monetary_exchange_rate					import profile_44005_monetary_exchange_rate

class application(object):
	def __init__(self):
		self.reg_count = 0
		self.clear()

		self.perform_taric_validation = True
		#self.perform_taric_validation = False
		self.show_progress = True
		#self.show_progress = False

		self.BASE_DIR			= os.path.dirname(os.path.abspath(__file__))
		self.TEMPLATE_DIR		= os.path.join(self.BASE_DIR,	"templates")
		self.CSV_DIR			= os.path.join(self.BASE_DIR,	"csv")
		self.XML_IN_DIR			= os.path.join(self.BASE_DIR,	"xml_in")
		self.UNLINTED_DIR		= os.path.join(self.XML_IN_DIR,	"unlinted")
		self.IMPORT_DIR			= os.path.join(self.BASE_DIR,	"import")
		self.XML_OUT_DIR		= os.path.join(self.BASE_DIR,	"xml_out")
		self.TEMP_DIR			= os.path.join(self.BASE_DIR,	"temp")
		self.TEMP_FILE			= os.path.join(self.TEMP_DIR,	"temp.xml")
		self.LOG_DIR			= os.path.join(self.BASE_DIR,	"log")
		self.IMPORT_LOG_DIR		= os.path.join(self.LOG_DIR,	"import")
		self.ERROR_LOG_DIR		= os.path.join(self.LOG_DIR,	"errors")
		self.REG_LOG_DIR		= os.path.join(self.LOG_DIR,	"regulation")

		self.LOG_FILE								= os.path.join(self.LOG_DIR,	"log.csv")
		self.LOG_FILE_MEASURE						= os.path.join(self.LOG_DIR,	"log_measure_deleted.csv")
		self.LOG_FILE_MEASURE_COMPONENT				= os.path.join(self.LOG_DIR,	"log_measure_component_deleted.csv")
		self.LOG_FILE_MEASURE_CONDITION				= os.path.join(self.LOG_DIR,	"log_measure_condition_deleted.csv")
		self.LOG_FILE_MEASURE_CONDITION_COMPONENT	= os.path.join(self.LOG_DIR,	"log_measure_condition_component_deleted.csv")

		self.MERGE_DIR			= os.path.join(self.XML_IN_DIR,	"custom")
		self.DUMP_DIR			= os.path.join(self.BASE_DIR,	"dump")
		self.CONFIG_DIR			= os.path.join(self.BASE_DIR, 	"..")
		self.CONFIG_DIR			= os.path.join(self.CONFIG_DIR, "config")
		self.CONFIG_FILE		= os.path.join(self.CONFIG_DIR, "config_common.json")
		self.CONFIG_FILE_LOCAL	= os.path.join(self.CONFIG_DIR, "config_convert_and_import_taric_files.json")

		self.SCHEMA_DIR				= os.path.join(self.BASE_DIR, "..")
		self.SCHEMA_DIR				= os.path.join(self.SCHEMA_DIR, "xsd")

		self.IMPORT_PROFILE_DIR	= os.path.join(self.CONFIG_DIR, "import_profile")

		self.namespaces 				= {'oub': 'urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0', 'env': 'urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0', } # add more as needed
		self.envelope_id				= ""
		self.sDivider					= ""
		self.message_id					= 1
		self.debug		    			= True
		self.simple_filenames			= True
		self.abrogation_regulation_id	= "I1900030" # Used for abrogating the PTS records that need truncating on Brexit day

		self.correlation_id				= ""
		self.checksum					= ""
		self.filesize					= ""
		self.source_file_name			= ""

		self.log_list_string			= []
		self.log_list					= []

		self.load_errors				= []

		my_script = sys.argv[0]
		self.get_config()
		self.get_minimum_sids()

		if self.DBASE in ("tariff_eu", "tariff_fta"):
			self.IMPORT_DIR			= self.XML_IN_DIR
		else:
			self.IMPORT_DIR			= self.IMPORT_DIR

		# Read in the whole of the log file, so that we can compare
		# current actions against past actions that have already been made
		with open(self.LOG_FILE) as csv_file:
			csv_reader = csv.reader(csv_file, delimiter = ",")
			for row in csv_reader:
				s = log(row[0], row[1], row[2], row[3], row[4], row[5])
				self.log_list.append (s)
				s2 = row[0] + "," + row[1] + "," + row[2] + "," + row[3] + "," + row[4] + "," + row[5]
				self.log_list_string.append (s2)


	def get_config(self):
		# Get global config items
		with open(self.CONFIG_FILE, 'r') as f:
			my_dict = json.load(f)

		critical_date			= my_dict['critical_date']
		nomenclature_date		= my_dict['nomenclature_date']

		self.critical_date		= datetime.strptime(critical_date, '%Y-%m-%d')
		self.nomenclature_date	= datetime.strptime(nomenclature_date, '%Y-%m-%d')
		self.DBASE				= my_dict['dbase']
		self.p					= my_dict['p']

		my_script = sys.argv[0]
		my_script = os.path.basename(my_script)

		if my_script == "import_dev.py":
			self.DBASE = "tariff_dev"
		elif my_script == "import_staging.py":
			self.DBASE = "tariff_staging"
		elif my_script == "import_fta.py":
			self.DBASE = "tariff_fta"
		elif my_script == "import_cds.py":
			self.DBASE = "tariff_cds"
		elif my_script == "import_eu.py":
			self.DBASE = "tariff_eu"
		elif my_script == "import_uk.py":
			self.DBASE = "tariff_uk"
		elif my_script == "import_shell.py":
			self.DBASE = "tariff_shell"
		elif my_script == "import_national.py":
			self.DBASE = "tariff_national"
		elif my_script == "import_load.py":
			self.DBASE = "tariff_load"

		# Get local config items
		with open(self.CONFIG_FILE_LOCAL, 'r') as f2:
			my_dict = json.load(f2)
		self.import_file_list = my_dict["import_files"]

		# Connect to the database
		self.connect()


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


	def pretty_print(self, xml_file):
		# Pretty print the XML file, making a copy of the original in the "unlinted" subfolder
		# Using this awful DOM thing, it will continue to insert line after line after line
		# if you pretty print multiple times, so there are two strategies for avoiding this
		# a) if the backup file exists, then abort and b) if the 1st line is short, then abort

		self.backup_file = os.path.join(self.UNLINTED_DIR, xml_file)
		if os.path.isfile(self.backup_file):
			return

		# If the file has short lines, this is a pretty good sign that it has already been pretty-printed
		my_file = open(self.xml_file_In, "r")
		line = my_file.readline()
		my_file.close()
		if len(line) < 255:
			return

		copyfile(self.xml_file_In, self.backup_file)

		# Now pretty print the original
		dom = xml.dom.minidom.parse(self.xml_file_In)
		pretty_xml_as_string = dom.toprettyxml()

		my_file = open(self.xml_file_In, "w")
		my_file.write (pretty_xml_as_string)

	def get_deleted_goods_nomenclatures(self):
		self.deleted_goods_nomenclatures = []
		sql = """select distinct goods_nomenclature_sid, goods_nomenclature_item_id, productline_suffix
		from ml.deleted_goods_nomenclatures
		order by goods_nomenclature_item_id, productline_suffix"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		for row in rows:
			obj = [row[0], row[1], row[2]]
			self.deleted_goods_nomenclatures.append (obj)


	def end_date_EU_measures(self, files):
		if len(files) == 0:
			sys.exit()
		else:
			xml_file = files[0]

		self.convertFilename(xml_file)
		self.d("Creating converted file for " + self.output_filename, False)

		self.xml_file_In	= os.path.join(self.XML_IN_DIR,  xml_file)
		self.xml_file_out	= os.path.join(self.XML_OUT_DIR, self.output_filename)

		self.pretty_print(xml_file)
		s = "What's in the input file" # (" + self.xml_file_In + ")"
		self.document_report = s + "\n".upper()
		self.document_report += ("=" * len(s)) + "\n"
		self.document_report += self.document_xml(self.xml_file_In)

		self.get_deleted_goods_nomenclatures()


		# Load file
		ET.register_namespace('oub', 'urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0')
		ET.register_namespace('env', 'urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0')
		tree = ET.parse(self.xml_file_In)
		root = tree.getroot()

		# Get the envelope ID
		self.envelope_id = root.get("id")
		if (len(self.envelope_id)) == 5:
			self.envelope_id = self.envelope_id[0:2] + "0" + self.envelope_id[2:]
			root.set("id", self.envelope_id)

		print ("Envelope ID of source file", self.envelope_id)

		# Loop through the transactions, looking for items to delete or amend
		quota_order_number_origin_list	= []
		quota_order_definition_list		= []
		measure_list					= []
		measure_condition_list			= []
		action_list = ["update", "delete", "insert"]
		self.regulation_log_filename	= os.path.join(self.REG_LOG_DIR, "reg_log_" + xml_file.replace("xml", "csv"))
		self.regulation_list = []
		self.new_regulation_list = []

		self.footnote_description_period_list = []
		self.certificate_description_period_list = []
		self.geographical_area_description_period_list = []

		for oTransaction in root.findall('.//env:transaction', self.namespaces):
			for oMessage in oTransaction.findall('.//env:app.message', self.namespaces):
				record_code			= oMessage.find(".//oub:record.code", self.namespaces).text
				sub_record_code		= oMessage.find(".//oub:subrecord.code", self.namespaces).text
				update_type			= oMessage.find(".//oub:update.type", self.namespaces).text
				update_type_string	= action_list[int(update_type) - 1]

				# 20005 FOOTNOTE DESCRIPTION PERIOD
				if record_code == "200" and sub_record_code == "05" and update_type in ("1", "3"):
					validity_start_date				= self.getDateValue(oMessage, ".//oub:validity.start.date")
					footnote_description_period_sid	= self.getDateValue(oMessage, ".//oub:footnote.description.period.sid")
					if validity_start_date >= self.critical_date:
						footnote_description_period_list.append(footnote_description_period_sid)
						oTransaction.remove (oMessage)
						self.register_update("200", "05", "delete", update_type_string, footnote_description_period_sid, xml_file, "Delete instruction to footnote description period " + footnote_description_period_sid)

				# 20010 FOOTNOTE DESCRIPTION
				if record_code == "200" and sub_record_code == "10" and update_type in ("1", "3"):
					footnote_description_period_sid	= self.getDateValue(oMessage, ".//oub:footnote.description.period.sid")
					if footnote_description_period_sid in self.footnote_description_period_list:
						oTransaction.remove (oMessage)
						self.register_update("200", "10", "delete", update_type_string, footnote_description_period_sid, xml_file, "Delete instruction to footnote description " + footnote_description_period_sid)

				# 20505 CERTIFICATE DESCRIPTION PERIOD
				if record_code == "205" and sub_record_code == "05" and update_type in ("1", "3"):
					validity_start_date				= self.getDateValue(oMessage, ".//oub:validity.start.date")
					certificate_description_period_sid	= self.getDateValue(oMessage, ".//oub:certificate.description.period.sid")
					if validity_start_date >= self.critical_date:
						certificate_description_period_list.append(certificate_description_period_sid)
						oTransaction.remove (oMessage)
						self.register_update("205", "05", "delete", update_type_string, certificate_description_period_sid, xml_file, "Delete instruction to certificate description period " + certificate_description_period_sid)

				# 20510 CERTIFICATE DESCRIPTION
				if record_code == "205" and sub_record_code == "10" and update_type in ("1", "3"):
					certificate_description_period_sid	= self.getDateValue(oMessage, ".//oub:certificate.description.period.sid")
					if certificate_description_period_sid in self.certificate_description_period_list:
						oTransaction.remove (oMessage)
						self.register_update("205", "10", "delete", update_type_string, certificate_description_period_sid, xml_file, "Delete instruction to certificate description " + certificate_description_period_sid)

				# 25005 GEOGRAPHICAL AREA DESCRIPTION PERIOD
				if record_code == "250" and sub_record_code == "05" and update_type in ("1", "3"):
					validity_start_date				= self.getDateValue(oMessage, ".//oub:validity.start.date")
					geographical_area_description_period_sid	= self.getDateValue(oMessage, ".//oub:geographical_area.description.period.sid")
					if validity_start_date >= self.critical_date:
						geographical_area_description_period_list.append(geographical_area_description_period_sid)
						oTransaction.remove (oMessage)
						self.register_update("250", "05", "delete", update_type_string, geographical_area_description_period_sid, xml_file, "Delete instruction to geographical_area description period " + geographical_area_description_period_sid)

				# 25010 GEOGRAPHICAL AREA DESCRIPTION
				if record_code == "250" and sub_record_code == "10" and update_type in ("1", "3"):
					geographical_area_description_period_sid	= self.getDateValue(oMessage, ".//oub:geographical_area.description.period.sid")
					if geographical_area_description_period_sid in self.geographical_area_description_period_list:
						oTransaction.remove (oMessage)
						self.register_update("250", "10", "delete", update_type_string, geographical_area_description_period_sid, xml_file, "Delete instruction to geographical_area description " + geographical_area_description_period_sid)

				# 37000	QUOTA DEFINITION
				# We actually do want to delete quota definitions
				if record_code == "370" and sub_record_code == "00" and update_type in ("1", "3"):
					validity_start_date		= self.getDateValue(oMessage, ".//oub:validity.start.date")
					validity_end_date		= self.getDateValue(oMessage, ".//oub:validity.end.date")
					quota_definition_sid	=  self.getValue(oMessage, ".//oub:quota.definition.sid")

					# Action - remove the quota definition message node if the quota definition does not start
					# until after the critical date
					if validity_start_date >= self.critical_date:
						quota_order_definition_list.append (quota_definition_sid)
						oTransaction.remove (oMessage)
						self.register_update("370", "00", "delete", update_type_string, quota_definition_sid, xml_file, "Delete instruction to create quota order definition " + quota_definition_sid)
					else:
						# Action - insert the end date on the quota definition, if the end date is blank
						# Not sure if this is possible, but here for completion
						if validity_end_date == "":
							oElement = self.getNode(oMessage, ".//oub:quota.definition")
							self.add_edit_node(oElement, "oub:validity.end.date", "oub:validity.start.date", datetime.strftime(self.critical_date, "%Y-%m-%d"))
							self.register_update("370", "00", "update", update_type_string, quota_definition_sid, xml_file, "Insert an end date for a quota definition which was otherwise un-end dated for definition " + quota_definition_sid)

						# Action - update the end date, if the end date is later than the critical date
						# and the start date is before the critical date, i.e. straddles
						elif validity_end_date >= self.critical_date:
							oElement = self.getNode(oMessage, ".//oub:quota.definition")
							self.setNode(oElement, "oub:validity.end.date", datetime.strftime(self.critical_date, "%Y-%m-%d"))
							self.register_update("370", "00", "update", update_type_string, quota_definition_sid, xml_file, "Update an explicit quota definition end date to the critical date for quota definition " + quota_definition_sid)

				# 40000 GOODS NOMENCLATURE - Inserts
				if record_code == "400" and sub_record_code == "00" and update_type == "3":
					goods_nomenclature_sid		= self.getNumberValue(oMessage, ".//oub:goods.nomenclature.sid")
					goods_nomenclature_item_id	= self.getValue(oMessage, ".//oub:goods.nomenclature.item.id")
					productline_suffix			= self.getValue(oMessage, ".//oub:producline.suffix")
					validity_start_date			= self.getDateValue(oMessage, ".//oub:validity.start.date")
					if validity_start_date > self.nomenclature_date:
						found = False
						for item in self.deleted_goods_nomenclatures:
							if item[0] == goods_nomenclature_sid:
								found = True
								break
						if found == False:
							obj = [goods_nomenclature_sid, goods_nomenclature_item_id, productline_suffix]
							self.deleted_goods_nomenclatures.append (obj)
						oTransaction.remove (oMessage)
						self.register_update("400", "00", "delete", update_type_string, goods_nomenclature_item_id, xml_file, "Delete instruction to create goods.nomenclature.item for commodity " + goods_nomenclature_item_id)

				# 40000 GOODS NOMENCLATURE - Updates
				if record_code == "400" and sub_record_code == "00" and update_type == "1":
					goods_nomenclature_sid		= self.getNumberValue(oMessage, ".//oub:goods.nomenclature.sid")
					goods_nomenclature_item_id	= self.getValue(oMessage, ".//oub:goods.nomenclature.item.id")
					productline_suffix			= self.getValue(oMessage, ".//oub:producline.suffix")
					validity_end_date			= self.getDateValue(oMessage, ".//oub:validity.end.date")

					if validity_end_date != "":
						dx = datetime.strftime(self.nomenclature_date, "%Y-%m-%d 00:00:00")
						if validity_end_date > self.nomenclature_date:
							found = False
							for item in self.deleted_goods_nomenclatures:
								if item[0] == goods_nomenclature_sid:
									found = True
									break
							if found == False:
								obj = [goods_nomenclature_sid, goods_nomenclature_item_id, productline_suffix]
								self.deleted_goods_nomenclatures.append (obj)
							oTransaction.remove (oMessage)
							self.register_update("400", "00", "delete", update_type_string, goods_nomenclature_item_id, xml_file, "Delete instruction to update goods.nomenclature.item for commodity " + goods_nomenclature_item_id)

				# 43000	MEASURE
				if record_code == "430" and sub_record_code == "00":
					validity_start_date					= self.getDateValue(oMessage, ".//oub:validity.start.date")
					validity_end_date					= self.getDateValue(oMessage, ".//oub:validity.end.date")
					measure_generating_regulation_role	= self.getValue(oMessage, ".//oub:measure.generating.regulation.role")
					measure_generating_regulation_id	= self.getValue(oMessage, ".//oub:measure.generating.regulation.id")
					measure_type_id						= self.getValue(oMessage, ".//oub:measure.type")
					measure_sid							= self.getValue(oMessage, ".//oub:measure.sid")
					self.regulation_list.append (measure_generating_regulation_id)

					# Action - remove the message node if the measure does not start until after the critical date
					if update_type in ("1", "3"):
						if validity_start_date > self.critical_date:
							oTransaction.remove (oMessage)
							measure_list.append(measure_sid)
							self.register_update("430", "00", "delete", update_type_string, measure_sid, xml_file, "Delete instruction for measure that would have started after EU Exit with measure.sid of " + measure_sid)

						# Action - if the measure begins before EU Exit, but the end date is empty,
						# then insert an end date (i.e the critical date - to be determined)
						# This also requires a justification regulation ID and role to be added
						elif validity_end_date == "":
							oElement = self.getNode(oMessage, ".//oub:measure")

							self.add_edit_node(oElement, "oub:justification.regulation.id", "oub:measure.generating.regulation.id", measure_generating_regulation_id)
							self.add_edit_node(oElement, "oub:justification.regulation.role", "oub:measure.generating.regulation.id", measure_generating_regulation_role)
							self.add_edit_node(oElement, "oub:validity.end.date", "oub:measure.generating.regulation.id", datetime.strftime(self.critical_date, "%Y-%m-%d"))

							self.register_update("430", "00", "update", update_type_string, measure_sid, xml_file, "Update a measure with no end date to end on the critical date - measure_sid " + measure_sid)

						# Action - if the measure begins before EU Exit, but the end date is after EU Exit and is fixed,
						# then alter the end date to be the critical date - to be determined)
						elif validity_end_date >= self.critical_date:
							oElement = self.getNode(oMessage, ".//oub:measure")
							self.setNode(oElement, "oub:validity.end.date", datetime.strftime(self.critical_date, "%Y-%m-%d"))
							self.add_edit_node(oElement, "oub:justification.regulation.id",   "oub:validity.end.date", measure_generating_regulation_id)
							self.add_edit_node(oElement, "oub:justification.regulation.role", "oub:validity.end.date", measure_generating_regulation_role)

							self.register_update("430", "00", "update", update_type_string, measure_sid, xml_file, "Update a measure that starts before EU Exit and ends after EU Exit to end on the critical date - measure_sid: " + measure_sid)



		########################################## PHASE 2 ########################################
		# Second loop through the transactions = needed once the list variables have been populated
		# Loop through the transactions, looking for items to delete or amend
		for oTransaction in root.findall('.//env:transaction', self.namespaces):
			for oMessage in oTransaction.findall('.//env:app.message', self.namespaces):
				record_code			= oMessage.find(".//oub:record.code", self.namespaces).text
				sub_record_code		= oMessage.find(".//oub:subrecord.code", self.namespaces).text
				update_type			= oMessage.find(".//oub:update.type", self.namespaces).text
				update_type_string	= action_list[int(update_type) - 1]

				# 40005 GOODS NOMENCLATURE INDENT
				if record_code == "400" and sub_record_code == "05" and update_type == "3":
					goods_nomenclature_sid		= self.getNumberValue(oMessage, ".//oub:goods.nomenclature.sid")
					goods_nomenclature_item_id	= self.getValue(oMessage, ".//oub:goods.nomenclature.item.id")
					productline_suffix			= self.getValue(oMessage, ".//oub:producline.suffix")
					validity_start_date			= self.getDateValue(oMessage, ".//oub:validity.start.date")
					if validity_start_date > self.nomenclature_date:
						oTransaction.remove (oMessage)
						self.register_update("400", "05", "delete", update_type_string, goods_nomenclature_item_id, xml_file, "Delete instruction to create / update goods.nomenclature.indent for commodity " + goods_nomenclature_item_id)

				# 40010 GOODS NOMENCLATURE DESCRIPTION PERIOD
				if record_code == "400" and sub_record_code == "10" and update_type == "3":
					goods_nomenclature_sid						= self.getNumberValue(oMessage, ".//oub:goods.nomenclature.sid")
					goods_nomenclature_description_period_sid	= self.getNumberValue(oMessage, ".//oub:goods.nomenclature.description.period.sid")
					goods_nomenclature_item_id					= self.getValue(oMessage, ".//oub:goods.nomenclature.item.id")
					productline_suffix							= self.getValue(oMessage, ".//oub:productline.suffix")
					validity_start_date							= self.getDateValue(oMessage, ".//oub:validity.start.date")
					if validity_start_date > self.nomenclature_date:
						oTransaction.remove (oMessage)
						self.register_update("400", "10", "delete", update_type_string, goods_nomenclature_item_id, xml_file, "Delete instruction to create goods.nomenclature.description.period for commodity " + goods_nomenclature_item_id)

				# 40015 GOODS NOMENCLATURE DESCRIPTION
				if record_code == "400" and sub_record_code == "15" and update_type == "3":
					goods_nomenclature_sid						= self.getNumberValue(oMessage, ".//oub:goods.nomenclature.sid")
					goods_nomenclature_description_period_sid	= self.getNumberValue(oMessage, ".//oub:goods.nomenclature.description.period.sid")
					goods_nomenclature_item_id					= self.getValue(oMessage, ".//oub:goods.nomenclature.item.id")
					productline_suffix							= self.getValue(oMessage, ".//oub:productline.suffix")
					if self.goods_nomenclature_deleted(goods_nomenclature_sid, goods_nomenclature_item_id, productline_suffix):
					#if goods_nomenclature_sid in self.deleted_goods_nomenclatures:
						oTransaction.remove (oMessage)
						self.register_update("400", "15", "delete", update_type_string, goods_nomenclature_item_id, xml_file, "Delete instruction to create goods.nomenclature.description for commodity " + goods_nomenclature_item_id)

				# 40020 GOODS NOMENCLATURE - FOOTNOTE ASSOCIATION
				if record_code == "400" and sub_record_code == "20" and update_type == "3":
					goods_nomenclature_sid						= self.getNumberValue(oMessage, ".//oub:goods.nomenclature.sid")
					goods_nomenclature_description_period_sid	= self.getNumberValue(oMessage, ".//oub:goods.nomenclature.description.period.sid")
					goods_nomenclature_item_id					= self.getValue(oMessage, ".//oub:goods.nomenclature.item.id")
					productline_suffix							= self.getValue(oMessage, ".//oub:productline.suffix")
					if self.goods_nomenclature_deleted(goods_nomenclature_sid, goods_nomenclature_item_id, productline_suffix):
						oTransaction.remove (oMessage)
						self.register_update("400", "20", "delete", update_type_string, goods_nomenclature_item_id, xml_file, "Delete instruction to create goods.nomenclature.footnote.association for commodity " + goods_nomenclature_item_id)

				# 40035 GOODS NOMENCLATURE ORIGIN
				if record_code == "400" and sub_record_code == "35" and update_type == "3":
					goods_nomenclature_sid						= self.getNumberValue(oMessage, ".//oub:goods.nomenclature.sid")
					goods_nomenclature_item_id					= self.getValue(oMessage, ".//oub:goods.nomenclature.item.id")
					productline_suffix							= self.getValue(oMessage, ".//oub:productline.suffix")
					if self.goods_nomenclature_deleted(goods_nomenclature_sid, goods_nomenclature_item_id, productline_suffix):
						oTransaction.remove (oMessage)
						self.register_update("400", "35", "delete", update_type_string, goods_nomenclature_item_id, xml_file, "Delete instruction to create goods.nomenclature.origin for commodity " + goods_nomenclature_item_id)

				# 40040 GOODS NOMENCLATURE SUCCESSOR
				if record_code == "400" and sub_record_code == "40" and update_type == "3":
					goods_nomenclature_item_id					= self.getValue(oMessage, ".//oub:absorbed.goods.nomenclature.item.id")
					productline_suffix							= self.getValue(oMessage, ".//oub:absorbed.productline.suffix")
					#if self.goods_nomenclature_deleted(None, goods_nomenclature_item_id, productline_suffix):
					oTransaction.remove (oMessage)
					self.register_update("400", "40", "delete", update_type_string, goods_nomenclature_item_id, xml_file, "Delete instruction to create goods.nomenclature.successor for commodity " + goods_nomenclature_item_id)


				if 1 > 0:
					# Additional action needed - will need to look for any quota associations and events for any definitions
					# that have been stripped, and get rid of them too
					# 37005	QUOTA ASSOCIATION
					# There are no associations left - yay
					if record_code == "370" and sub_record_code == "05" and update_type in ("1", "3"):
						main_quota_definition_sid	= self.getValue(oMessage, ".//oub:main.quota.definition.sid")
						sub_quota_definition_sid	= self.getValue(oMessage, ".//oub:sub.quota.definition.sid")
						for sid in quota_order_definition_list:
							if (main_quota_definition_sid == sid) or (sub_quota_definition_sid == sid):
								oTransaction.remove (oMessage)
								self.register_update("370", "05", "delete", update_type_string, sid, xml_file, "Delete quota association with quota definition: " + sid)
								break

					# 37010	QUOTA BLOCKING PERIOD
					if record_code == "370" and sub_record_code == "10" and update_type in ("1", "3"):
						quota_blocking_period_sid	= self.getValue(oMessage, ".//oub:quota.blocking.period.sid")
						quota_definition_sid		= self.getValue(oMessage, ".//oub:quota.definition.sid")
						blocking_start_date			= self.getDateValue(oMessage, ".//oub:blocking.start.date")
						blocking_end_date			= self.getDateValue(oMessage, ".//oub:blocking.end.date")

						# Action - Delete a blocking period if the blocking period starts after the critical date
						if blocking_start_date > self.critical_date:
							oTransaction.remove (oMessage)
							self.register_update("370", "10", "delete", update_type_string, quota_blocking_period_sid, xml_file, "Delete quota blocking period with quota definition " + quota_definition_sid + " and blocking period SID of " + quota_blocking_period_sid)

						# Action - If the blocking period starts before the critical date, but ends after the critical date
						# then end date the blocking period on the critical date
						# Please note - blocking periods must have an end date, so there is no condition where end date is blank
						elif blocking_start_date < self.critical_date and blocking_end_date > self.critical_date:
							oElement = self.getNode(oMessage, ".//oub:quota.blocking.period")
							self.register_update("370", "10", "update", update_type_string, quota_blocking_period_sid, xml_file, "Update an existing blocking period end date to the critical date")
							self.setNode(oElement, "oub:blocking.end.date", datetime.strftime(self.critical_date, "%Y-%m-%d"))

						# Action - search through this file's quota definitions and look for any items that have been deleted
						# If the quota definition has been deleted, then delete the blocking period too
						for sid in quota_order_definition_list:
							if (quota_definition_sid == sid):
								try:
									oTransaction.remove (oMessage)
									self.register_update("370", "10", "delete", update_type_string, quota_blocking_period_sid, xml_file, "Delete quota blocking period " + update_type_string + " instruction")
								except:
									pass
								break

					# 37015	QUOTA SUSPENSION PERIOD
					# There are none of these left, yay
					if record_code == "370" and sub_record_code == "15" and update_type in ("1", "3"):
						quota_suspension_period_sid	= self.getValue(oMessage, ".//oub:quota.suspension.period.sid")
						quota_definition_sid		= self.getValue(oMessage, ".//oub:quota.definition.sid")
						suspension_start_date		= self.getDateValue(oMessage, ".//oub:suspension.start.date")
						suspension_end_date			= self.getDateValue(oMessage, ".//oub:suspension.end.date")

						# Action - Delete a suspension period if the suspension period starts after the critical date
						if suspension_start_date > self.critical_date:
							oTransaction.remove (oMessage)
							self.register_update("370", "15", "delete", update_type_string, quota_suspension_period_sid, xml_file, "Delete quota suspension period with quota definition " + quota_definition_sid + " and suspension period SID of " + quota_suspension_period_sid)

						# Action - If the suspension period starts before the critical date, but ends after the critical date
						# then end date the suspension period on the critical date
						# Please note - suspension periods must have an end date, so there is no condition where end date is blank
						elif suspension_start_date < self.critical_date and suspension_end_date > self.critical_date:
							oElement = self.getNode(oMessage, ".//oub:quota.suspension.period")
							self.d("Update an existing suspension period end date to the critical date")
							self.setNode(oElement, "oub:suspension.end.date", datetime.strftime(self.critical_date, "%Y-%m-%d"))
							self.register_update("370", "15", "update", update_type_string, quota_suspension_period_sid, xml_file, "Update an existing suspension period end date to the critical date")

						# Action - search through this file's quota definitions and look for any items that have been deleted
						# If the quota definition has been deleted, then delete the suspension period too
						for sid in quota_order_definition_list:
							if (quota_definition_sid == sid):
								try:
									oTransaction.remove (oMessage)
									self.register_update("370", "15", "delete", update_type_string, quota_suspension_period_sid, xml_file, "Delete quota suspension period with quota definition: " + sid)
								except:
									pass
								break

					# 37505	QUOTA UNBLOCKING EVENT
					if record_code == "375" and sub_record_code == "05" and update_type in ("1", "3"):
						quota_definition_sid		= self.getValue(oMessage, ".//oub:quota.definition.sid")
						unblocking_date				= self.getDateValue(oMessage, ".//oub:unblocking.date")

						# Action - Delete an unblocking event if it takes place after the critical date
						if unblocking_date > self.critical_date:
							oTransaction.remove (oMessage)
							self.register_update("375", "05", "delete", update_type_string, quota_definition_sid, xml_file, "Delete quota unblocking event with quota definition " + quota_definition_sid)

						# Action - search through this file's quota definitions and look for any items that have been deleted
						# If the quota definition has been deleted, then delete the suspension period too
						for sid in quota_order_definition_list:
							if (quota_definition_sid == sid):
								try:
									self.register_update("375", "05", "delete", update_type_string, quota_definition_sid, xml_file, "Delete quota unblocking event with quota definition: " + sid)
									oTransaction.remove (oMessage)
								except:
									pass
								break

					# 37510	QUOTA CRITICAL EVENT
					if record_code == "375" and sub_record_code == "10" and update_type in ("1", "3"):
						quota_definition_sid		= self.getValue(oMessage, ".//oub:quota.definition.sid")
						critical_state_change_date	= self.getDateValue(oMessage, ".//oub:critical.state.change.date")

						# Action - Delete an critical event if it takes place after the critical date
						if critical_state_change_date > self.critical_date:
							oTransaction.remove (oMessage)
							self.register_update("375", "10", "delete", update_type_string, quota_definition_sid, xml_file, "Delete quota critical event with quota definition " + quota_definition_sid)

						# Action - search through this file's quota definitions and look for any items that have been deleted
						# If the quota definition has been deleted, then delete the suspension period too
						for sid in quota_order_definition_list:
							if (quota_definition_sid == sid):
								try:
									oTransaction.remove (oMessage)
									self.register_update("375", "10", "delete", update_type_string, quota_definition_sid, xml_file, "Delete quota critical event with quota definition: " + sid)
								except:
									pass
								break

					# 37515	QUOTA EXHAUSTION EVENT
					if record_code == "375" and sub_record_code == "15" and update_type in ("1", "3"):
						quota_definition_sid	= self.getValue(oMessage, ".//oub:quota.definition.sid")
						exhaustion_date			= self.getDateValue(oMessage, ".//oub:exhaustion.date")

						# Action - Delete an critical event if it takes place after the critical date
						if exhaustion_date > self.critical_date:
							self.d("Delete quota exhaustion event with quota definition " + quota_definition_sid)
							oTransaction.remove (oMessage)
							self.register_update("375", "15", "delete", update_type_string, quota_definition_sid, xml_file, "Delete quota exhaustion event " + update_type_string + " instruction")

						# Action - search through this file's quota definitions and look for any items that have been deleted
						# If the quota definition has been deleted, then delete the suspension period too
						for sid in quota_order_definition_list:
							if (quota_definition_sid == sid):
								try:
									oTransaction.remove (oMessage)
									self.register_update("375", "15", "delete", update_type_string, quota_definition_sid, xml_file, "Delete quota exhaustion event with quota definition: " + sid)
								except:
									pass
								break

					# 37520	QUOTA REOPENING EVENT
					if record_code == "375" and sub_record_code == "20" and update_type in ("1", "3"):
						quota_definition_sid	= self.getValue(oMessage, ".//oub:quota.definition.sid")
						reopening_date			= self.getDateValue(oMessage, ".//oub:reopening.date")

						# Action - Delete an reopening event if it takes place after the critical date
						if reopening_date > self.critical_date:
							oTransaction.remove (oMessage)
							self.register_update("375", "20", "delete", update_type_string, quota_definition_sid, xml_file, "Delete quota reopening event with quota definition " + quota_definition_sid)

						# Action - search through this file's quota definitions and look for any items that have been deleted
						# If the quota definition has been deleted, then delete the reopening event too
						for sid in quota_order_definition_list:
							if (quota_definition_sid == sid):
								try:
									oTransaction.remove (oMessage)
									self.register_update("375", "20", "delete", update_type_string, quota_definition_sid, xml_file, "Delete quota reopening event with quota definition: " + sid)
								except:
									pass
								break

					# 37525	QUOTA UNSUSPENSION EVENT
					if record_code == "375" and sub_record_code == "25" and update_type in ("1", "3"):
						quota_definition_sid	= self.getValue(oMessage, ".//oub:quota.definition.sid")
						unsuspension_date		= self.getDateValue(oMessage, ".//oub:unsuspension.date")

						# Action - Delete an unsuspension event if it takes place after the critical date
						if unsuspension_date > self.critical_date:
							oTransaction.remove (oMessage)
							self.register_update("375", "25", "delete", update_type_string, quota_definition_sid, xml_file, "Delete quota unsuspension event with quota definition " + quota_definition_sid)

						# Action - search through this file's quota definitions and look for any items that have been deleted
						# If the quota definition has been deleted, then delete the unsuspension event too
						for sid in quota_order_definition_list:
							if (quota_definition_sid == sid):
								try:
									oTransaction.remove (oMessage)
									self.register_update("375", "25", "delete", update_type_string, quota_definition_sid, xml_file, "Delete quota unsuspension event with quota definition: " + sid)
								except:
									pass
								break

				# 43005	MEASURE COMPONENT
				if record_code == "430" and sub_record_code == "05":
					if update_type in ("1", "3"):
						measure_sid	= self.getValue(oMessage, ".//oub:measure.sid")
						removed_node = False
						for sid in measure_list:
							if (measure_sid == sid):
								oTransaction.remove (oMessage)
								self.register_update("430", "05", "delete", update_type_string, sid, xml_file, "Delete measure component for deleted measure with sid " + sid)
								removed_node = True
								break
						if not(removed_node):
							for s in self.log_list:
								if s.record_code == "430" and s.sub_record_code == "05" and (s.update_type_string in ("insert", "update")):
									if s.sid == measure_sid:
										oTransaction.remove (oMessage)
										self.register_update("430", "05", "delete", update_type_string, measure_sid, xml_file, "Delete measure component for deleted measure with sid " + measure_sid)
										break

				# 43010	MEASURE CONDITION
				# Look for any measure conditions in the current file that have a measure_sid that matches
				# one that has been deleted from the file - if so, then delete this instuction
				if record_code == "430" and sub_record_code == "10" and update_type in ("1", "3"):
					measure_sid	= self.getValue(oMessage, ".//oub:measure.sid")
					removed_node = False
					for sid in measure_list:
						if (measure_sid == sid):
							oTransaction.remove (oMessage)
							measure_condition_sid = self.getValue(oMessage, ".//oub:measure.condition.sid")
							self.register_update("430", "10", "delete", update_type_string, sid, xml_file, "Delete measure condition for deleted measure with sid " + sid)
							measure_condition_list.append(measure_condition_sid)
							removed_node = True
							break

					# Look in the log for any matching measure conditions that map to this one
					# If found, then delete the measure condition
					if not(removed_node):
						for s in self.log_list:
							if s.record_code == "430" and s.sub_record_code == "10" and (s.update_type_string in ("insert", "update")):
								if s.sid == measure_sid:
									oTransaction.remove (oMessage)
									self.register_update("430", "10", "delete", update_type_string, measure_sid, xml_file, "Delete measure condition for deleted measure with sid " + measure_sid)
									break

				# 43015	MEASURE EXCLUDED GEOGRAPHICAL AREA
				# Look in the current file for any excluded geo areas that map to any of the measure sids that have been removed.
				# If found, then delete them from the XML file
				if record_code == "430" and sub_record_code == "15" and update_type in ("1", "3"):
					measure_sid	= self.getValue(oMessage, ".//oub:measure.sid")
					removed_node = False
					for sid in measure_list:
						if (measure_sid == sid):
							oTransaction.remove (oMessage)
							self.register_update("430", "15", "delete", update_type_string, sid, xml_file, "Delete geographical area exclusion for deleted measure with sid " + sid)
							removed_node = True
							break

					# Look in the log file for any measure sids that match this measure component:
					# If found, then delete them from the XML file
					if not(removed_node):
						for s in self.log_list:
							if s.record_code == "430" and s.sub_record_code == "15" and (s.update_type_string in ("insert", "update")):
								if s.sid == measure_sid:
									oTransaction.remove (oMessage)
									self.register_update("430", "15", "delete", update_type_string, measure_sid, xml_file, "Delete geographical area exclusion for deleted measure with sid " + measure_sid)
									break

				# 43020	FOOTNOTE ASSOCIATION (MEASURE)
				if record_code == "430" and sub_record_code == "20" and update_type in ("1", "3"):
					measure_sid	= self.getValue(oMessage, ".//oub:measure.sid")
					removed_node = False
					for sid in measure_list:
						if (measure_sid == sid):
							oTransaction.remove (oMessage)
							self.register_update("430", "20", "delete", update_type_string, sid, xml_file, "Delete footnote association (measure) for deleted measure with sid " + sid)
							removed_node = True
							break

					if not(removed_node):
						for s in self.log_list:
							if s.record_code == "430" and s.sub_record_code == "20" and (s.update_type_string in ("insert", "update")):
								if s.sid == measure_sid:
									oTransaction.remove (oMessage)
									self.register_update("430", "20", "delete", update_type_string, measure_sid, xml_file, "Delete footnote association (measure) for deleted measure with sid " + measure_sid)
									break

				# 43025	MEASURE PARTIAL TEMPORARY STOP
				if record_code == "430" and sub_record_code == "25" and update_type in ("1", "3"):
					measure_sid	= self.getValue(oMessage, ".//oub:measure.sid")
					removed_node = False
					for sid in measure_list:
						if (measure_sid == sid):
							oTransaction.remove (oMessage)
							self.register_update("430", "25", "delete", update_type_string, sid, xml_file, "Delete partial temporary stop for deleted measure with sid " + sid)
							removed_node = True
							break

					if not(removed_node):
						for s in self.log_list:
							if s.record_code == "430" and s.sub_record_code == "25" and (s.update_type_string in ("insert", "update")):
								if s.sid == measure_sid:
									oTransaction.remove (oMessage)
									self.register_update("430", "25", "delete", update_type_string, measure_sid, xml_file, "Delete partial temporary stop for deleted measure with sid " + measure_sid)
									break

		# The third pass through looks at measure condition components
		# This removes meaure condition components that are created as a result of the measure conditions that have already been
		# removed in the 2nd pass.

		for oTransaction in root.findall('.//env:transaction', self.namespaces):
			for oMessage in oTransaction.findall('.//env:app.message', self.namespaces):
				record_code			= oMessage.find(".//oub:record.code", self.namespaces).text
				sub_record_code		= oMessage.find(".//oub:subrecord.code", self.namespaces).text
				update_type			= oMessage.find(".//oub:update.type", self.namespaces).text
				update_type_string	= action_list[int(update_type) - 1]

				# 43011	MEASURE CONDITION COMPONENT
				# Look in the current file for any measure condition records that have been deleted from the XML file
				# If found, the delete the measure condition component instruction as well
				if record_code == "430" and sub_record_code == "11" and update_type in ("1", "3"):
					measure_condition_sid = self.getNumberValue(oMessage, ".//oub:measure.condition.sid")
					#print (measure_condition_sid)
					removed_node = False

					for sid in measure_condition_list:
						if (int(measure_condition_sid) == int(sid)):
							oTransaction.remove (oMessage)
							self.register_update("430", "11", "delete", update_type_string, str(sid), xml_file, "Delete measure condition component for deleted measure condition with sid " + str(measure_condition_sid))
							removed_node = True
							break

					# Then, look into the log and find any matching measure conditions, from which components
					# would also need to get removed. Then delete those instructions
					if not(removed_node):
						for s in self.log_list:
							if s.record_code == "430" and s.sub_record_code == "10" and (s.update_type_string in ("insert", "update")):
								if s.sid == measure_condition_sid:
									oTransaction.remove (oMessage)
									self.register_update("430", "11", "delete", update_type_string, measure_condition_sid, xml_file, "Delete measure condition component for deleted measure condition with sid " + measure_condition_sid)
									break


		# Loop through the transactions, looking for empty transactions, where the sub messages have all been deleted
		# If found, then delete them all
		self.d("Delete all empty transaction nodes")
		for oTransaction in root.findall('.//env:transaction', self.namespaces):
			count = 0
			for oMessage in oTransaction.findall('.//env:app.message', self.namespaces):
				count += 1
			if count == 0:
				root.remove (oTransaction)

		# Write the XML node tree out in full
		tree.write(open(self.TEMP_FILE, "wb"), encoding = "utf8")

		# Reopen the file and correct the XML errors
		f = open(self.TEMP_FILE, "r", encoding = "utf-8")
		s = f.read()
		f.close()

		s = re.sub("'utf8'", r'"UTF-8"', s)
		s = re.sub("'1.0'", r'"1.0"', s)
		s = re.sub("><", ">\n            <", s)
		s = re.sub(r'xmlns:env=', r'xmlns=', s)
		s = re.sub(r'xmlns:oub=', r'xmlns:env=', s)
		s = re.sub(r':TARIC:MESSAGE:', r':TARIC:TEMPMESSAGE:', s)
		s = re.sub(r':GENERAL:ENVELOPE:', r':TARIC:MESSAGE:', s)
		s = re.sub(r':TARIC:TEMPMESSAGE:', r':GENERAL:ENVELOPE:', s)
		s = re.sub("<oub:transmission>", r'<oub:transmission xmlns:env="urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0" xmlns:oub="urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0">', s)

		s = re.sub(r'<env:envelope xmlns="urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0" id="([0-9]{1,8})" />', '<env:envelope xmlns="urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0" id="\\1">\n</env:envelope>', s)


		s = re.sub(r' xmlns="urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0"', r' xmlns="urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0" xmlns:env="urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0"', s)
		s = re.sub(r' xmlns:env="urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0" xmlns:env="urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0"', r' xmlns:env="urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0"', s)

		# Write the regulation log file
		f = open(self.regulation_log_filename, "w", encoding="utf-8")
		my_set = set(self.regulation_list)
		self.regulation_log = ""

		self.regulation_log += "New regulations\n"
		if len(self.new_regulation_list) > 0:
			for r in self.new_regulation_list:
				self.regulation_log += r.regulation_id + "," + r.information_text + "\n"
		else:
			self.regulation_log += "No new regulations\n"

		self.regulation_log += "\nRegulations referenced\n"
		for r in my_set:
			my_desc = ""
			if len(self.new_regulation_list) > 0:
				for nr in self.new_regulation_list:
					if nr.regulation_id == r:
						my_desc = "<NEW> " + nr.reg_type + " : " + nr.information_text
						break
			self.regulation_log += r + "," + my_desc + "\n"

		f.write(self.regulation_log)
		f.close()

		# Write the XML file
		f = open(self.TEMP_FILE, "w", encoding="utf-8")
		f.write(s)
		f.close()
		#sys.exit()
		self.validate(temp=True)

		files[0] = self.TEMP_FILE

		iCount = len(files)
		if iCount > 1:
			self.d("Merging in additional files", True)
		iLoop = 0
		with open(self.xml_file_out, 'w') as outfile:
			for fname in files:

				a = 1
				iLoop += 1
				if fname.find("temp.xml") == -1:
					print ("   - " + fname.replace(self.TEMP_DIR, ""))
					fname = os.path.join(self.MERGE_DIR, fname)

				with open(fname) as infile:
					for line in infile:

						# Is the first file and not the last file: lose the tail only
						if iLoop == 1 and iLoop != iCount:
							if not("</env:envelope" in line):
								outfile.write(line)

						# Is the first file and is also the last file: copy the entire file
						if iLoop == 1 and iLoop == iCount:
							outfile.write(line)

						# Not the first file and not the last file: lose the top and tail
						if iLoop != 1 and iLoop != iCount:
							if not("<?xml" in line) and not("env:envelope" in line):
								outfile.write(line)

						# Not the first file and is the last file: lose the top only
						if iLoop != 1 and iLoop == iCount:
							if not("<?xml" in line) and not("<env:envelope" in line):
								outfile.write(line)

		self.validate()
		s = "What's in the output file" # (" + self.xml_file_out + ")"
		self.document_report += "\n\n" + s + "\n".upper()
		self.document_report += ("=" * len(s)) + "\n"
		self.document_report += self.document_xml(self.xml_file_out)

		self.write_document_report()
		self.write_deleted_goods_nomenclatures()

	def goods_nomenclature_deleted(self, goods_nomenclature_sid, goods_nomenclature_item_id, productline_suffix):
		deleted = False
		if goods_nomenclature_sid != None:
			for item in self.deleted_goods_nomenclatures:
				if item[0] == goods_nomenclature_sid:
					deleted = True
					break
		else:
			for item in self.deleted_goods_nomenclatures:
				if item[1] == goods_nomenclature_item_id and item[2] == productline_suffix:
					deleted = True
					break
		return (deleted)



	def write_deleted_goods_nomenclatures(self):
		for item in self.deleted_goods_nomenclatures:
			goods_nomenclature_sid		= item[0]
			goods_nomenclature_item_id	= item[1]
			productline_suffix			= item[2]
			sql = """
			INSERT INTO ml.deleted_goods_nomenclatures (goods_nomenclature_sid, goods_nomenclature_item_id, productline_suffix)
			VALUES  (""" + str(goods_nomenclature_sid) + """, '""" + goods_nomenclature_item_id + """', '""" + productline_suffix + """')
			ON CONFLICT ON CONSTRAINT deleted_goods_nomenclatures_pk
			DO NOTHING
			"""
			cur = self.conn.cursor()
			cur.execute(sql)
			self.conn.commit()

	def write_document_report(self):
		path = os.path.join(self.XML_OUT_DIR, self.xml_file_out.replace(".xml", "_report.txt"))
		my_file = open(path, "w+")
		my_file.write(self.document_report)
		my_file.close()

	def validate(self, temp=False):
		if temp:
			s = self.TEMP_FILE
			msg = "Validating the initial XML file against the Taric 3 schema"
		else:
			s = self.xml_file_out
			msg = "Validating the final XML file against the Taric 3 schema"

		self.d(msg)
		schema_path = os.path.join(self.SCHEMA_DIR, "envelope.xsd")
		my_schema = xmlschema.XMLSchema(schema_path)
		try:
			if my_schema.is_valid(s):
				self.d("The file validated successfully")
				success = True
			else:
				self.d("The file did not validate")
				success = False
		except:
			self.d("The file did not validate and crashed the validator")
			success = False

		if success == False:
			my_schema.validate(s)

	def validateMetadata(self):
		self.d("Validating the metadata XML file against the metadata schema")
		schema_path = os.path.join(self.SCHEMA_DIR, "BatchFileInterfaceMetadata-1.0.7.xsd")
		my_schema = xmlschema.XMLSchema(schema_path)
		try:
			if my_schema.is_valid(self.metadata_filepath):
				self.d("The metadata file validated successfully.")
			else:
				self.d("The metadata file did not validate.")
		except:
			self.d("The metadata file did not validate and crashed the validator.")

	def getValue(self, node, xpath, return_null = False):
		try:
			s = node.find(xpath, self.namespaces).text
		except:
			if return_null:
				s = None
			else:
				s = ""
		return (s)

	def getNumberValue(self, node, xpath, return_null = False):
		try:
			s = int(node.find(xpath, self.namespaces).text)
		except:
			if return_null:
				s = None
			else:
				s = ""
		return (s)

	def getNode(self, node, xpath):
		try:
			s = node.find(xpath, self.namespaces)
		except:
			s = None
		return (s)

	def getDateValue(self, node, xpath, return_null = False):
		try:
			s = node.find(xpath, self.namespaces).text
			pos = s.find("T")
			if pos > -1:
				s = s[0:pos]
			s = datetime.strptime(s, "%Y-%m-%d")
		except:
			if return_null:
				s = None
			else:
				s = ""
		return (s)

	def getIndex(self, node, xpath):
		index = -1
		for child in node.iter():
			#print (child.tag)
			index += 1
			s = child.tag.replace("{urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0}", "")
			if s == xpath:
				break
		return index

	def add_edit_node(self, oElement, new_node, after, new_node_text):
		after = after.replace("oub:", "")
		s = self.getValue(oElement, new_node)
		if s == "":
			index = self.getIndex(oElement, after)
			new_element      = ET.Element(new_node)
			new_element.text = new_node_text
			oElement.insert(index, new_element)
		else:
			node = self.getNode(oElement, new_node)
			node.text = new_node_text


	def setNode(self, oElement, xpath, new_node_text):
		node = self.getNode(oElement, xpath)
		node.text = new_node_text

	def d(self, s, include_indent = True):
		if self.debug:
			if include_indent:
				s = "- " + s
			else:
				s = "\n" + s.upper()
			#print (s + "\n")
			print (s)

	def register_update(self, record_code, sub_record_code, python_action_string, update_type_string, sid, filename, desc):
		self.d(desc)

		# Write a record to the log file if the identical string does not already exist
		s = record_code + "," + sub_record_code + "," + update_type_string + "," + sid + "," + filename + "," + desc
		if not(s in self.log_list_string):
			f = open(self.LOG_FILE, "a")
			f.write(s + "\n")
			f.close()

		# Wherever there are deletions of future records, also add these to their own unique log
		if python_action_string == "delete":
			# First for measures
			if record_code == "430" and sub_record_code == "00":
				with open (self.LOG_FILE_MEASURE, "r") as myfile:
					LOG_FILE_MEASURE_content = myfile.read()
				if sid not in LOG_FILE_MEASURE_content:
					f = open(self.LOG_FILE_MEASURE, "a")
					f.write(sid + "\n")
					f.close()

			elif record_code == "430" and sub_record_code == "10":
			# And then for measure conditions
				with open (self.LOG_FILE_MEASURE_CONDITION, "r") as myfile:
					LOG_FILE_MEASURE_CONDITION_content = myfile.read()
				if sid not in LOG_FILE_MEASURE_CONDITION_content:
					f = open(self.LOG_FILE_MEASURE_CONDITION, "a")
					f.write(sid + "\n")
					f.close()


	def generate_metadata(self):
		self.d("Generating metadata file", False)
		# Get the handle
		filename = os.path.join(self.TEMPLATE_DIR, "metadata_template.xml")
		handle = open(filename, "r")
		self.metadata_XML = handle.read()

		self.correlation_id		= str(uuid.uuid1())
		self.checksum			= self.md5Checksum(self.xml_file_out)
		self.filesize			= str(os.path.getsize(self.xml_file_out))
		self.source_file_name	= self.output_filename

		self.metadata_XML = self.metadata_XML.replace("{CORRELATION_ID}",	self.correlation_id)
		self.metadata_XML = self.metadata_XML.replace("{CHECKSUM}", 		self.checksum)
		self.metadata_XML = self.metadata_XML.replace("{FILESIZE}", 		self.filesize)
		self.metadata_XML = self.metadata_XML.replace("{SOURCE_FILE_NAME}", self.source_file_name)

		# Write the output file
		self.metadata_filepath	= os.path.join(self.XML_OUT_DIR, self.metadata_filename)
		f = open(self.metadata_filepath, "w")
		f.write(self.metadata_XML)
		f.close()
		self.validateMetadata()

	def getTimestamp(self):
		ts = datetime.now()
		ts_string = datetime.strftime(ts, "%Y%m%dT%H%M%S")
		return (ts_string)

	def getDatestamp(self):
		ts = datetime.now()
		ts_string = datetime.strftime(ts, "%Y-%m-%d")
		return (ts_string)


	def convertFilename(self, s):
		if self.simple_filenames:
			if len(s) > 12:
				sequence_id		= s[14:16] + "0" + s[16:19]
				s = "DIT" + sequence_id + ".xml"
			else:
				sequence_id		= s[3:5] + "0" + s[5:8]
				s = "DIT" + sequence_id + ".xml"
		else:
			underscore_pos	= s.find('_')
			sequence_id	= s[14:19]
			if underscore_pos > -1:
				dt = s[0:underscore_pos].replace("-", "")
				ts = self.getTimestamp()
				s = "DIT_" + dt + "-" + dt + "-" + ts + "-" + sequence_id + ".XML"
			else:
				self.d("Fail")
				sys.exit()

		self.output_filename	= s
		self.metadata_filename	= s.replace(".", "_metadata.")


	def backup_database(self):
		DB_USER = 'postgres'
		DB_NAME = 'trade_tariff_181212b'
		ts = self.getTimestamp()
		filename = DB_NAME + "_" + ts + ".backup"

		destination = r'%s/%s' % (self.DUMP_DIR, filename)

		print ('Backing up %s database to %s' % (DB_NAME, destination))
		ps = subprocess.Popen(['pg_dump', '-U', DB_USER, '-f', destination, '-Fc', DB_NAME], stdout=subprocess.PIPE)
		output = ps.communicate()[0]
		for line in output.splitlines():
			print (line)


	def md5Checksum(self, filePath):
		with open(filePath, 'rb') as fh:
			m = hashlib.md5()
			while True:
				data = fh.read(8192)
				if not data:
					break
				m.update(data)
			return m.hexdigest()

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

	def doprint(self, s):
		self.log_handle.write ("Message " + str(self.message_count) + " - " + s + "\n")
		if self.show_progress == True:
			print (s)

	def log_error(self, object, action, sid_key, id_key, transaction_id, message_id):
		self.conn.rollback()
		s = "Error - " + object + " " + action + " " + str(sid_key) + " " + str(id_key) + " " + str(transaction_id) + " " + str(message_id)
		self.db_errors.append(s)
		self.log_handle.write (s + "\n")


	def load_classification_trees(self):
		self.nodes = []
		print ("Getting classification trees")
		temp = []
		self.nodes.append (temp)
		for i in range(1, 100):
			chapter = str(i).zfill(2)
			filename = os.path.join(self.CSV_DIR, chapter + ".csv")
			try:
				with open(filename) as csv_file:
					csv_reader = csv.reader(csv_file, delimiter = ",")
					temp = []
					for row in csv_reader:
						c = classification(row[0], row[1], int(row[2]), int(row[3]), int(row[4]))
						temp.append (c)
			except:
				temp = []
			self.nodes.append (temp)
			a = 1

		print ("Classification trees complete")


		print ("Working out nomenclature parent / child relationships")

		for i in range(1, 100):
			nodes = self.nodes[i]
			goods_nomenclature_count = len(nodes)
			for loop1 in range(0, goods_nomenclature_count):
				my_commodity = nodes[loop1]
				if my_commodity.significant_digits == 2:
					pass
				else:
					if my_commodity.number_indents == 0:
						for loop2 in range(loop1 - 1, -1, -1):
							prior_commodity = nodes[loop2]
							if prior_commodity.significant_digits == 2:
								my_commodity.parent_goods_nomenclature_item_id = prior_commodity.goods_nomenclature_item_id
								my_commodity.parent_productline_suffix = prior_commodity.productline_suffix
								break
					else:
						for loop2 in range(loop1 - 1, -1, -1):
							prior_commodity = nodes[loop2]
							if prior_commodity.number_indents == (my_commodity.number_indents - 1):
								my_commodity.parent_goods_nomenclature_item_id = prior_commodity.goods_nomenclature_item_id
								my_commodity.parent_productline_suffix = prior_commodity.productline_suffix
								break
		print ("Working out nomenclature parent / child relationships - complete")


	def find_node(self, commodity_code):
		chapter = commodity_code[0:2]
		int_chapter = int(chapter)
		nodes = self.nodes[int_chapter]
		my_index = -1
		for node in nodes:
			my_index += 1
			if node.goods_nomenclature_item_id == commodity_code and node.productline_suffix == "80":
				# Add myself to the list of relations first - will check ME01
				relations = []
				relations = self.get_relations(my_index, node, "both", relations)
				node.relations = relations
				break
		return (node)


	def get_relations(self, my_index, my_commodity, direction, relations):
		chapter = my_commodity.goods_nomenclature_item_id[0:2]
		int_chapter = int(chapter)
		nodes = self.nodes[int_chapter]
		# Search UP the tree for parentage
		if direction != "down":
			for loop2 in range(my_index - 1, -1, -1):
				prior_commodity = nodes[loop2]
				if prior_commodity.goods_nomenclature_item_id == my_commodity.parent_goods_nomenclature_item_id \
					and prior_commodity.productline_suffix == my_commodity.parent_productline_suffix:
					if prior_commodity.productline_suffix == "80":
						relations.append(prior_commodity.goods_nomenclature_item_id)
						a = 1
					self.get_relations(loop2, prior_commodity, "up", relations)
					if prior_commodity.significant_digits == 4:
						break

		# Now search DOWN the tree for children
		if direction != "up":
			for loop2 in range(my_index + 1, len(nodes)):
				next_commodity = nodes[loop2]
				if next_commodity.parent_goods_nomenclature_item_id == my_commodity.goods_nomenclature_item_id \
					and next_commodity.parent_productline_suffix == my_commodity.productline_suffix:

					if next_commodity.productline_suffix == "80":
						relations.append(next_commodity.goods_nomenclature_item_id)
					self.get_relations(loop2, next_commodity, "down", relations)

					if next_commodity.significant_digits == 4 or loop2 == len(nodes):
						break
		return (relations)


	def write_classification_trees(self):
		# This could do with being cron jobbed eventually to run this at the start of every day
		#print ("Getting classification trees")
		for i in range(1, 100):
			chapter = str(i).zfill(2)
			filename = os.path.join(self.CSV_DIR, chapter + ".csv")
			sql = """select goods_nomenclature_item_id, producline_suffix, number_indents, leaf, significant_digits
			from ml.goods_nomenclature_export_generic('""" + chapter + """%', '2019-11-01')
			order by goods_nomenclature_item_id, producline_suffix"""
			cur = self.conn.cursor()
			cur.execute(sql)
			rows = cur.fetchall()
			if len(rows) > 0:
				with open(filename, 'w+') as csvFile:
					writer = csv.writer(csvFile)
					writer.writerows(rows)
			csvFile.close()


	def import_xml(self, xml_file, prompt = True):
		ret = sys.gettrace()
		if ret == None:
			self.debug_mode = False
		else:
			self.debug_mode = True

		self.import_file = xml_file
		self.load_classification_trees()
		self.all_regulations		= self.get_all_regulations()
		self.geographical_area_sids = self.get_all_geographical_area_sids()
		self.geographical_areas		= self.get_all_geographical_areas()
		self.quota_order_numbers	= self.get_quota_order_numbers()
		self.quota_definitions		= self.get_quota_definitions()
		self.goods_nomenclatures	= self.get_all_goods_nomenclatures()
		self.duty_expressions		= self.get_duty_expressions()

		self.convertFilename(xml_file)
		self.d("Importing file " + xml_file + " using database " + self.DBASE, False)

		if prompt:
			ret = self.yes_or_no("Do you want to continue?")
			if not (ret) or ret in ("n", "N", "No"):
				sys.exit()

		# Check that this file has not already been imported
		sql = "SELECT import_file FROM ml.import_files WHERE import_file = '" + xml_file + "'"
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()

		if self.debug_mode == False:
			if len(rows) > 0:
				print ("\nFile", xml_file, "has already been imported - Aborting now\n")
				return

		self.xml_file_In		= os.path.join(self.IMPORT_DIR,  	xml_file)
		self.IMPORT_LOG_FILE	= os.path.join(self.IMPORT_LOG_DIR, "log_" + xml_file)
		self.IMPORT_LOG_FILE	= self.IMPORT_LOG_FILE.replace("xml", "txt")

		self.log_handle = open(self.IMPORT_LOG_FILE,"w")
		self.db_errors	= []

		# Load file
		ET.register_namespace('oub', 'urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0')
		ET.register_namespace('env', 'urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0')
		try:
			tree = ET.parse(self.xml_file_In)
		except:
			print ("The selected file could not be found")
			sys.exit()
		root = tree.getroot()

		self.register_import_start(xml_file)

		action_list = ["update", "delete", "insert"]

		self.message_count = 0

		for oTransaction in root.findall('.//env:transaction', self.namespaces):
			for oMessage in oTransaction.findall('.//env:app.message', self.namespaces):
				record_code			= oMessage.find(".//oub:record.code", self.namespaces).text
				sub_record_code		= oMessage.find(".//oub:subrecord.code", self.namespaces).text
				update_type			= oMessage.find(".//oub:update.type", self.namespaces).text
				transaction_id		= oMessage.find(".//oub:transaction.id", self.namespaces).text
				message_id			= oMessage.attrib["id"] # message id dummy"

				# 10000	FOOTNOTE TYPE
				if record_code == "100" and sub_record_code == "00":
					o = profile_10000_footnote_type()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 10000	FOOTNOTE TYPE DESCRIPTION
				if record_code == "100" and sub_record_code == "05":
					o = profile_10005_footnote_type_description()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 11000	CERTIFICATE TYPE
				if record_code == "110" and sub_record_code == "00":
					o = profile_11000_certificate_type()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 11005	CERTIFICATE TYPE DESCRIPTION
				if record_code == "110" and sub_record_code == "05":
					o = profile_11005_certificate_type_description()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 12000	ADDITIONAL CODE TYPE
				if record_code == "120" and sub_record_code == "00":
					o = profile_12000_additional_code_type()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 12005	ADDITIONAL CODE TYPE DESCRIPTION
				if record_code == "120" and sub_record_code == "05":
					o = profile_12005_additional_code_type_description()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 13000	LANGUAGE
				if record_code == "130" and sub_record_code == "00":
					o = profile_13000_language()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 13005	LANGUAGE DESCRIPTION
				if record_code == "130" and sub_record_code == "05":
					o = profile_13005_language_description()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 14000	MEASURE TYPE SERIES
				if record_code == "140" and sub_record_code == "00":
					o = profile_14000_measure_type_series()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 14005	MEASURE TYPE SERIES DESCRIPTION
				if record_code == "140" and sub_record_code == "05":
					o = profile_14005_measure_type_series_description()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 15000	REGULATION GROUP
				if record_code == "150" and sub_record_code == "00":
					o = profile_15000_regulation_group()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 15005	REGULATION GROUP DESCRIPTION
				if record_code == "150" and sub_record_code == "05":
					o = profile_15005_regulation_group_description()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 16000	REGULATION ROLE TYPE
				if record_code == "160" and sub_record_code == "00":
					o = profile_16000_regulation_role_type()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 16005	REGULATION ROLE TYPE DESCRIPTION
				if record_code == "160" and sub_record_code == "05":
					o = profile_16005_regulation_role_type_description()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 17000	PUBLICATION SIGLE
				if record_code == "170" and sub_record_code == "00":
					o = profile_17000_publication_sigle()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 20000	FOOTNOTE
				if record_code == "200" and sub_record_code == "00":
					o = profile_20000_footnote()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 20005	FOOTNOTE DESCRIPTION PERIOD
				if record_code == "200" and sub_record_code == "05":
					o = profile_20005_footnote_description_period()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 20010	FOOTNOTE DESCRIPTION
				if record_code == "200" and sub_record_code == "10":
					o = profile_20010_footnote_description()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 20500	CERTIFICATE
				if record_code == "205" and sub_record_code == "00":
					o = profile_20500_certificate()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 20505	CERTIFICATE DESCRIPTION PERIOD
				if record_code == "205" and sub_record_code == "05":
					o = profile_20505_certificate_description_period()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 20510	CERTIFICATE DESCRIPTION
				if record_code == "205" and sub_record_code == "10":
					o = profile_20510_certificate_description()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 21000	MEASUREMENT UNIT
				if record_code == "210" and sub_record_code == "00":
					o = profile_21000_measurement_unit()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 21005	MEASUREMENT UNIT DESCRIPTION
				if record_code == "210" and sub_record_code == "05":
					o = profile_21005_measurement_unit_description()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 21500	MEASUREMENT UNIT QUALIFIER
				if record_code == "215" and sub_record_code == "00":
					o = profile_21500_measurement_unit_qualifier()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 21505	MEASUREMENT UNIT QUALIFIER DESCRIPTION
				if record_code == "215" and sub_record_code == "05":
					o = profile_21505_measurement_unit_qualifier_description()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 22000	MEASUREMENT
				if record_code == "220" and sub_record_code == "00":
					o = profile_22000_measurement()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 22500	MONETARY UNIT
				if record_code == "225" and sub_record_code == "00":
					o = profile_22500_monetary_unit()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 22500	MONETARY UNIT
				if record_code == "225" and sub_record_code == "05":
					o = profile_22505_monetary_unit_description()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 23000	DUTY EXPRESSION
				if record_code == "230" and sub_record_code == "00":
					o = profile_23000_duty_expression()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 23005	DUTY EXPRESSION DESCRIPTION
				if record_code == "230" and sub_record_code == "05":
					o = profile_23005_duty_expression_description()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 23500	MEASURE TYPE
				if record_code == "235" and sub_record_code == "00":
					o = profile_23500_measure_type()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 23505	MEASURE TYPE DESCRIPTION
				if record_code == "235" and sub_record_code == "05":
					o = profile_23505_measure_type_description()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 24000	ADDITIONAL CODE TYPE / MEASURE TYPE
				if record_code == "240" and sub_record_code == "00":
					o = profile_24000_additional_code_type_measure_type()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)


				# 24500	ADDITIONAL CODE
				if record_code == "245" and sub_record_code == "00":
					o = profile_24500_additional_code()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 24505	ADDITIONAL CODE DESCRIPTION PERIOD
				if record_code == "245" and sub_record_code == "05":
					o = profile_24505_additional_code_description_period()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 24510	ADDITIONAL CODE DESCRIPTION
				if record_code == "245" and sub_record_code == "10":
					o = profile_24510_additional_code_description()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 25000	GEOGRAPHICAL AREA
				if record_code == "250" and sub_record_code == "00":
					o = profile_25000_geographical_area()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 25005	GEOGRAPHICAL AREA DESCRIPTION PERIOD
				if record_code == "250" and sub_record_code == "05":
					o = profile_25005_geographical_area_description_period()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 25010	GEOGRAPHICAL AREA DESCRIPTION
				if record_code == "250" and sub_record_code == "10":
					o = profile_25010_geographical_area_description()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 25015	GEOGRAPHICAL AREA MEMBERSHIP
				if record_code == "250" and sub_record_code == "15":
					o = profile_25015_geographical_area_membership()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 27500	COMPLETE ABROGATION REGULATION
				if record_code == "275" and sub_record_code == "00":
					o = profile_27500_complete_abrogation_regulation()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 28000	EXPLICIT ABROGATION REGULATION
				if record_code == "280" and sub_record_code == "00":
					o = profile_28000_explicit_abrogation_regulation()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 28500	BASE REGULATION
				if record_code == "285" and sub_record_code == "00":
					o = profile_28500_base_regulation()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 29000	MODIFICATION REGULATION
				if record_code == "290" and sub_record_code == "00":
					o = profile_29000_modification_regulation()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 29500	PROROGATION REGULATION
				if record_code == "295" and sub_record_code == "00":
					o = profile_29500_prorogation_regulation()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 29505	PROROGATION REGULATION ACTION
				if record_code == "295" and sub_record_code == "05":
					o = profile_29505_prorogation_regulation_action()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 30000	FULL TEMPORARY STOP REGULATION
				if record_code == "300" and sub_record_code == "00":
					o = profile_30000_full_temporary_stop_regulation()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 30005	FTS REGULATION ACTION
				if record_code == "300" and sub_record_code == "05":
					o = profile_30005_fts_regulation_action()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 30500	REGULATION REPLACEMENT
				if record_code == "305" and sub_record_code == "00":
					o = profile_30500_regulation_replacement()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 35000	MEASURE CONDITION
				if record_code == "350" and sub_record_code == "00":
					o = profile_35000_measure_condition_code()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 35005	MEASURE CONDITION DESCRIPTION
				if record_code == "350" and sub_record_code == "05":
					o = profile_35005_measure_condition_code_description()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 35500	MEASURE ACTION
				if record_code == "355" and sub_record_code == "00":
					o = profile_35500_measure_action()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 35505	MEASURE ACTION DESCRIPTION
				if record_code == "355" and sub_record_code == "05":
					o = profile_35505_measure_action_description()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 36000	QUOTA ORDER NUMBER
				if record_code == "360" and sub_record_code == "00":
					o = profile_36000_quota_order_number()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 36005	QUOTA ORDER NUMBER ORIGIN
				if record_code == "360" and sub_record_code == "10":
					o = profile_36010_quota_order_number_origin()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 36000	QUOTA ORDER NUMBER ORIGIN EXCLUSION
				if record_code == "360" and sub_record_code == "15":
					o = profile_36015_quota_order_number_origin_exclusion()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 37000	QUOTA DEFINITION
				if record_code == "370" and sub_record_code == "00":
					o = profile_37000_quota_definition()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 37005	QUOTA ASSOCIATION
				if record_code == "370" and sub_record_code == "05":
					o = profile_37005_quota_association()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 37010	QUOTA BLOCKING PERIOD
				if record_code == "370" and sub_record_code == "10":
					o = profile_37010_quota_blocking_period()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 37015	QUOTA SUSPENSION PERIOD
				if record_code == "370" and sub_record_code == "15":
					o = profile_37015_quota_suspension_period()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 37500	QUOTA BALANCE EVENT
				if record_code == "375" and sub_record_code == "00":
					o = profile_37500_quota_balance_event()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 37505	QUOTA UNBLOCKING EVENT
				if record_code == "375" and sub_record_code == "05":
					o = profile_37505_quota_unblocking_event()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 37510	QUOTA CRITICAL EVENT
				if record_code == "375" and sub_record_code == "10":
					o = profile_37510_quota_critical_event()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 37515	QUOTA EXHAUSTION EVENT
				if record_code == "375" and sub_record_code == "15":
					o = profile_37515_quota_exhaustion_event()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 37520	QUOTA REOPENING EVENT
				if record_code == "375" and sub_record_code == "20":
					o = profile_37520_quota_reopening_event()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 37525	QUOTA UNSUSPENSION EVENT
				if record_code == "375" and sub_record_code == "25":
					o = profile_37525_quota_unsuspension_event()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 37530	CLOSED AND BALANCE TRANSFER EVENT
				if record_code == "375" and sub_record_code == "30":
					o = profile_37530_quota_closed_and_balance_transferred_event()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 40000	GOODS NOMENCLATURE
				if record_code == "400" and sub_record_code == "00":
					o = profile_40000_goods_nomenclature()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 40005	GOODS NOMENCLATURE INDENT
				if record_code == "400" and sub_record_code == "05":
					o = profile_40005_goods_nomenclature_indent()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 40010	GOODS NOMENCLATURE DESCRIPTION PERIOD
				if record_code == "400" and sub_record_code == "10":
					o = profile_40010_goods_nomenclature_description_period()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 40015	GOODS NOMENCLATURE DESCRIPTION
				if record_code == "400" and sub_record_code == "15":
					o = profile_40015_goods_nomenclature_description()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 40020	FOOTNOTE ASSOCIATION GOODS NOMENCLATURE
				if record_code == "400" and sub_record_code == "20":
					o = profile_40020_footnote_association_goods_nomenclature()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 40035	GOODS NOMENCLATURE ORIGIN
				if record_code == "400" and sub_record_code == "35":
					o = profile_40035_goods_nomenclature_origin()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 40040	GOODS NOMENCLATURE SUCCESSOR
				if record_code == "400" and sub_record_code == "40":
					o = profile_40040_goods_nomenclature_successor()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)


				# 43000	MEASURE
				if record_code == "430" and sub_record_code == "00":
					o = profile_43000_measure()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)


				# 43005	MEASURE COMPONENT
				if record_code == "430" and sub_record_code == "05":
					o = profile_43005_measure_component()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 43010	MEASURE CONDITION
				if record_code == "430" and sub_record_code == "10":
					o = profile_43010_measure_condition()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 43011	MEASURE CONDITION COMPONENT
				if record_code == "430" and sub_record_code == "11":
					o = profile_43011_measure_condition_component()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 43015	MEASURE EXCLUDED GEOGRAPHICAL AREA
				if record_code == "430" and sub_record_code == "15":
					o = profile_43015_measure_excluded_geographical_area()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 43020	FOOTNOTE ASSOCIATION - MEASURE
				if record_code == "430" and sub_record_code == "20":
					o = profile_43020_footnote_association_measure()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 43025	MEASURE PARTIAL TEMPORARY STOP
				if record_code == "430" and sub_record_code == "25":
					o = profile_43025_measure_partial_temporary_stop()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 44000	MONETARY EXCHANGE PERIOD
				if record_code == "440" and sub_record_code == "00":
					o = profile_44000_monetary_exchange_period()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

				# 44005	MONETARY EXCHANGE RATE
				if record_code == "440" and sub_record_code == "05":
					o = profile_44005_monetary_exchange_rate()
					o.import_xml(self, update_type, oMessage, transaction_id, message_id)

		self.log_handle.close()

		if self.perform_taric_validation == False:
			# Post load checks
			self.rule_FO04()
			self.rule_CE06()
			self.rule_GA3()

		# Register the load
		self.register_import_complete(xml_file)
		if len(self.load_errors) > 0:
			print ("File failed to load - rolling back")
			self.rollback()
			self.create_error_report()

	def create_error_report(self):
		fname = self.import_file + "_error.txt"
		path = os.path.join(self.ERROR_LOG_DIR, self.DBASE)
		path = os.path.join(path, fname)
		out = ""
		out += "Error log for file " + self.import_file + "\n"
		ln = len(out) - 1
		out += ("=" * ln) + "\n\n"

		now = datetime.now()
		dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
		out += "Load date / time: " + dt_string + "\n\n"

		out += "There are " + str(len(self.load_errors)) + " conformance errors, as follows:\n\n"

		cnt = 0
		for err in self.load_errors:
			cnt += 1
			out += err + "\n"

		out += "\nThere are " + str(len(self.db_errors)) + " data load errors, as follows:\n\n"
		for err in self.db_errors:
			cnt += 1
			out += err + "\n"

		f = open(path, "w+")
		f.write (out)


	def register_import_start(self, xml_file):
		self.import_start_time = self.getTimestamp()
		sql = """
		INSERT INTO ml.import_files (import_file, import_started, status)
		VALUES  ('""" + xml_file + """', '""" + self.import_start_time + """', 'Started')
		"""
		cur = self.conn.cursor()
		cur.execute(sql)
		self.conn.commit()

	def register_import_complete(self, xml_file):
		self.import_complete_time = self.getTimestamp()
		sql = """
		UPDATE ml.import_files SET import_completed = '""" + self.import_complete_time + """',
		status = 'Completed'
		WHERE import_file = '""" + xml_file + """'
		"""
		cur = self.conn.cursor()
		cur.execute(sql)
		self.conn.commit()

	def larger(self, a, b):
		try:
			if a > b:
				return a
			else:
				return b
		except:
			return 0


	def get_scalar(self, sql):
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		#l = list(rows)
		return (rows[0][0])


	def yes_or_no(self, question):
		reply = str(input(question+' (y/n): ')).lower().strip()
		if reply[0] == 'y':
			return True
		if reply[0] == 'n':
			return False
		else:
			return yes_or_no("Uhhhh... please enter ")


	def document_xml(self, filename):
		tree = ET.parse(filename)
		self.root = tree.getroot()
		self.ns = {'env': 'urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0', 'oub': 'urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0'}
		self.counts = {}

		self.add_count("additional.code")
		self.add_count("additional.code.description.period")
		self.add_count("additional.code.description")
		self.add_count("additional.code.type")
		self.add_count("additional.code.type.description")
		self.add_count("additional.code.type.measure.type")
		self.add_count("base.regulation")
		self.add_count("ceiling")
		self.add_count("certificate")
		self.add_count("certificate.description.period")
		self.add_count("certificate.description")
		self.add_count("certificate.type")
		self.add_count("certificate.type.description")
		self.add_count("complete.abrogation.regulation")
		self.add_count("duty.expression")
		self.add_count("duty.expression.description")
		self.add_count("explicit.abrogation.regulation")
		self.add_count("footnote")
		self.add_count("footnote.association.additional.code")
		self.add_count("footnote.association.goods.nomenclature")
		self.add_count("footnote.description.period")
		self.add_count("footnote.description")
		self.add_count("footnote.type")
		self.add_count("footnote.type.description")
		self.add_count("fts.regulation.action")
		self.add_count("full.temporary.stop.regulation")
		self.add_count("geographical.area")
		self.add_count("geographical.area.description.period")
		self.add_count("geographical.area.description")
		self.add_count("geographical.area.membership")
		self.add_count("goods.nomenclature")
		self.add_count("goods.nomenclature.indents")
		self.add_count("goods.nomenclature.description.period")
		self.add_count("goods.nomenclature.description")
		self.add_count("goods.nomenclature.origin")
		self.add_count("goods.nomenclature.successor")
		self.add_count("measure")
		self.add_count("measure.component")
		self.add_count("measure.condition")
		self.add_count("measure.condition.component")
		self.add_count("measure.excluded.geographical.area")
		self.add_count("footnote.association.measure")
		self.add_count("measure.partial.temporary.stop")
		self.add_count("measure.condition.code")
		self.add_count("measure.condition.code.description")
		self.add_count("measure.action")
		self.add_count("measure.action.description")
		self.add_count("measure.type")
		self.add_count("measure.type.description")
		self.add_count("measure.type.series")
		self.add_count("measure.type.series.description")
		self.add_count("measurement.unit")
		self.add_count("measurement.unit.description")
		self.add_count("measurement.unit.qualifier")
		self.add_count("measurement.unit.qualifier.description")
		self.add_count("measurement")
		self.add_count("modification.regulation")
		self.add_count("monetary.exchange.period")
		self.add_count("monetary.exchange.rate")
		self.add_count("monetary.unit")
		self.add_count("monetary.unit.description")
		self.add_count("prorogation.regulation")
		self.add_count("prorogation.regulation.action")
		self.add_count("quota.order.number")
		self.add_count("quota.order.number.origin")
		self.add_count("quota.order.number.origin.exclusions")
		self.add_count("quota.definition")
		self.add_count("quota.association")
		self.add_count("quota.blocking.period")
		self.add_count("quota.suspension.period")
		self.add_count("quota.extended.information")
		self.add_count("quota.balance.event")
		self.add_count("quota.unblocking.event")
		self.add_count("quota.critical.event")
		self.add_count("quota.exhaustion.event")
		self.add_count("quota.reopening.event")
		self.add_count("quota.unsuspension.event")
		self.add_count("quota.closed.and.transferred.event")
		self.add_count("regulation.group")
		self.add_count("regulation.group.description")
		self.add_count("regulation.role.type")
		self.add_count("regulation.role.type.description")
		self.add_count("regulation.replacement")

		ret = ""
		for key in self.counts:
			if key[0] != ">":
				ret += "\n"
			ret += key + " : " + str(self.counts[key]) + "\n"
		return ret

	def add_count(self, node):
		if node == "transaction":
			xpath = ".//env:transaction"
			count = len(self.root.findall(xpath, self.ns))
			self.counts[node] = count
		else:
			# all records
			xpath = "./env:transaction/env:app.message/oub:transmission/oub:record/oub:" + node
			count = len(self.root.findall(xpath, self.ns))
			if count > 0:
				self.counts[node.upper()] = count

			# inserted records
			xpath = "./env:transaction/env:app.message/oub:transmission/oub:record/oub:" + node + "/../[oub:update.type='3']"
			count = len(self.root.findall(xpath, self.ns))
			if count > 0:
				self.counts[">  " + node + " - inserted records"] = count

			# updated records
			xpath = "./env:transaction/env:app.message/oub:transmission/oub:record/oub:" + node + "/../[oub:update.type='1']"
			count = len(self.root.findall(xpath, self.ns))
			if count > 0:
				self.counts[">  " + node + " - updated records"] = count

			# deleted records
			xpath = "./env:transaction/env:app.message/oub:transmission/oub:record/oub:" + node + "/../[oub:update.type='2']"
			count = len(self.root.findall(xpath, self.ns))
			if count > 0:
				self.counts[">  " + node + " - deleted records"] = count


	def add_load_error(self, msg):
		print ("Rule violation -", msg)
		self.load_errors.append(msg)

	def to_nice_time(self, dt):
		r = dt[0:4] + "-" + dt[4:6] + "-" + dt[6:8] + " " + dt[9:11] + ":" + dt[11:13] + ":" + dt[13:15]
		return (r)

	def rollback(self):
		sql = "select * from ml.clear_data('"+ self.to_nice_time(self.import_start_time) + "', '" + self.import_file + "')"

		cur = self.conn.cursor()
		cur.execute(sql)
		self.conn.commit()

	# Functions required for rule checks for import scripts
	def get_footnote_types(self):
		sql = "select footnote_type_id from footnote_types where validity_end_date is null order by 1;"
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		my_list = []
		for row in rows:
			my_list.append (row[0])
		return (my_list)

	def get_footnotes(self):
		sql = "select footnote_type_id || footnote_id as code from footnotes order by 1;"
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		my_list = []
		for row in rows:
			my_list.append (row[0])
		return (my_list)

	def get_footnote_type_descriptions(self):
		sql = "select footnote_type_id from footnote_type_descriptions order by 1;"
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		my_list = []
		for row in rows:
			my_list.append (row[0])
		return (my_list)

	def get_additional_code_types(self):
		sql = "select additional_code_type_id from additional_code_types where validity_end_date is null order by 1;"
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		my_list = []
		for row in rows:
			my_list.append (row[0])
		return (my_list)

	def get_additional_code_type_descriptions(self):
		sql = """select actp.additional_code_type_id
		from additional_code_type_descriptions actp, additional_code_types act
		where actp.additional_code_type_id = act.additional_code_type_id
		and act.validity_end_date is null
		order by 1
		;"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		my_list = []
		for row in rows:
			my_list.append (row[0])
		return (my_list)

	def get_certificate_types(self):
		sql = "select certificate_type_code from certificate_types where validity_end_date is null order by 1;"
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		my_list = []
		for row in rows:
			my_list.append (row[0])
		return (my_list)

	def get_certificate_type_descriptions(self):
		sql = "select certificate_type_code from certificate_type_descriptions order by 1;"
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		my_list = []
		for row in rows:
			my_list.append (row[0])
		return (my_list)

	def get_regulation_groups(self):
		sql = "select regulation_group_id from regulation_groups where validity_end_date is null order by 1;"
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		my_list = []
		for row in rows:
			my_list.append (row[0])
		return (my_list)

	def get_geographical_area_groups(self):
		sql = "select geographical_area_sid from geographical_areas where geographical_code = '1' and validity_end_date is null order by 1;"
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		my_list = []
		for row in rows:
			my_list.append (row[0])
		return (my_list)

	def get_all_geographical_areas(self):
		sql = "select geographical_area_id from geographical_areas where validity_end_date is null order by 1;"
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		my_list = []
		for row in rows:
			my_list.append (row[0])
		return (my_list)

	def get_all_goods_nomenclatures(self):
		sql = "select goods_nomenclature_item_id from goods_nomenclatures " \
		"where (validity_end_date is null or validity_end_date > '2019-11-01') " \
		"and producline_suffix = '80' order by 1;"
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		my_list = []
		for row in rows:
			my_list.append (row[0])
		return (my_list)

	def get_all_geographical_area_sids(self):
		sql = "select geographical_area_sid from geographical_areas where validity_end_date is null order by 1;"
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		my_list = []
		for row in rows:
			my_list.append (row[0])
		return (my_list)

	def get_countries_regions(self):
		sql = "select geographical_area_sid from geographical_areas where geographical_code != '1' and validity_end_date is null order by 1;"
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		my_list = []
		for row in rows:
			my_list.append (row[0])
		return (my_list)

	def get_base_regulations(self):
		sql = "select distinct base_regulation_role || base_regulation_id as code from base_regulations order by 1;"
		sql = "select distinct base_regulation_id as code from base_regulations order by 1;"
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		my_list = []
		for row in rows:
			my_list.append (row[0])
		return (my_list)

	def get_modification_regulations(self):
		sql = "select distinct modification_regulation_role || modification_regulation_id as code from modification_regulations order by 1;"
		sql = "select distinct modification_regulation_id as code from modification_regulations order by 1;"
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		my_list = []
		for row in rows:
			my_list.append (row[0])
		return (my_list)

	def get_all_regulations(self):
		sql = "select distinct modification_regulation_id as code, validity_start_date, " \
		"validity_end_date from modification_regulations " \
		"union select distinct base_regulation_id as code, validity_start_date, validity_end_date from base_regulations " \
		"order by 1;"

		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		my_list = []
		self.all_regulations_with_dates = []
		for row in rows:
			my_list.append (row[0])
			obj = []
			obj.append (row[0])
			obj.append (row[1])
			obj.append (row[2])
			self.all_regulations_with_dates.append (obj)
		return (my_list)

	def get_quota_order_numbers(self):
		sql = """select distinct on (quota_order_number_id)
		quota_order_number_id, quota_order_number_sid, validity_start_date, validity_end_date
		from quota_order_numbers qon
		order by 1, 3 desc"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		my_list = []
		self.all_quota_order_numbers = []
		for row in rows:
			my_list.append (row[0])
			obj = []
			obj.append (row[0])
			obj.append (row[1])
			obj.append (row[2])
			obj.append (row[3])
			self.all_quota_order_numbers.append (obj)
		return (my_list)

	def get_quota_definitions(self):
		sql = """select quota_definition_sid, quota_order_number_id, validity_start_date, validity_end_date
		from quota_definitions order by quota_order_number_id, validity_start_date"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		my_list = []
		self.all_quota_definitions = []
		for row in rows:
			my_list.append (row[0])
			obj = []
			obj.append (row[0])
			obj.append (row[1])
			obj.append (row[2])
			obj.append (row[3])
			self.all_quota_definitions.append (obj)
		return (my_list)

	def get_my_regulation(self, regulation_code):
		for item in self.all_regulations_with_dates:
			#print (item[0], regulation_code)
			if item[0] == regulation_code:
				return (item)
				break

	def get_measure_types(self):
		sql = "select measure_type_id, validity_start_date, validity_end_date, order_number_capture_code from measure_types order by 1;"
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		my_list = []
		self.all_measure_types = []
		for row in rows:
			my_list.append (row[0])
			obj = []
			obj.append (row[0])
			obj.append (row[1])
			obj.append (row[2])
			obj.append (row[3])
			self.all_measure_types.append (obj)
		return (my_list)

	def get_measure_type_series(self):
		sql = "select measure_type_series_id from measure_type_series where validity_end_date is null order by 1;"
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		my_list = []
		for row in rows:
			my_list.append (row[0])
		return (my_list)

	def get_duty_expressions(self):
		sql = """select duty_expression_id, validity_start_date, validity_end_date, duty_amount_applicability_code,
		measurement_unit_applicability_code, monetary_unit_applicability_code
		from duty_expressions where validity_end_date is null order by 1;"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		my_list = []
		self.all_duty_expressions = []
		for row in rows:
			my_list.append (row[0])
			obj = []
			obj.append (row[0])
			obj.append (row[1])
			obj.append (row[2])
			obj.append (row[3])
			obj.append (row[4])
			obj.append (row[5])
			self.all_duty_expressions.append (obj)
		return (my_list)

	def rule_FO04(self):
		# Footnote description period must exist at the start of the footnote
		sql = "select footnote_type_id || footnote_id as code, validity_start_date from footnotes f order by 1, 2;"
		cur = self.conn.cursor()
		cur.execute(sql)
		footnote_list = cur.fetchall()

		sql = "select footnote_type_id || footnote_id as code, validity_start_date from footnote_description_periods fdp order by 1, 2;"
		cur = self.conn.cursor()
		cur.execute(sql)
		fdp_list = cur.fetchall()

		for item in footnote_list:
			code = item[0]
			validity_start_date = item[1]
			matched = False
			for item2 in fdp_list:
				code2 = item2[0]
				validity_start_date2 = item2[1]
				if code == code2 and validity_start_date == validity_start_date2:
					matched = True
					break
			if matched == False:
				self.add_load_error("Rule FO4 - At least one description record is mandatory. The start date of the first description " \
				"period must be equal to the start date of the footnote. No two associated description periods may have the same start date. " \
				"The start date of the footnote must be less than or equal to the end date of the footnote. Issue occurred on footnote """ + code + """.""")

	def rule_CE06(self):
		# Certificate description period must exist at the start of the certificate
		sql = "select certificate_type_code || certificate_code as code, validity_start_date from certificates c order by 1, 2;"
		cur = self.conn.cursor()
		cur.execute(sql)
		certificate_list = cur.fetchall()

		sql = "select certificate_type_code || certificate_code as code, validity_start_date from certificate_description_periods cdp order by 1, 2;"
		cur = self.conn.cursor()
		cur.execute(sql)
		cdp_list = cur.fetchall()

		for item in certificate_list:
			code = item[0]
			validity_start_date = item[1]
			matched = False
			for item2 in cdp_list:
				code2 = item2[0]
				validity_start_date2 = item2[1]
				if code == code2 and validity_start_date == validity_start_date2:
					matched = True
					break
			if matched == False:
				self.add_load_error("At least one description record is mandatory. The start date of the first description period must " \
				"be equal to the start date of the certificate. No two associated description periods for the same certificate and language " \
				"may have the same start date. The validity period of the certificate must span the validity period of the certificate " \
				"description. Issue occurred on certificate """ + code + """.""")


	def rule_GA3(self):
		# Geographical area description period must exist at the start of the Geographical area
		sql = "select geographical_area_id, validity_start_date from geographical_areas order by 1, 2;"
		cur = self.conn.cursor()
		cur.execute(sql)
		ga_list = cur.fetchall()

		sql = "select geographical_area_id, validity_start_date from geographical_area_description_periods cdp order by 1, 2;"
		cur = self.conn.cursor()
		cur.execute(sql)
		gadp_list = cur.fetchall()

		for item in ga_list:
			code = item[0]
			validity_start_date = item[1]
			matched = False
			for item2 in gadp_list:
				code2 = item2[0]
				validity_start_date2 = item2[1]
				if code == code2 and validity_start_date == validity_start_date2:
					matched = True
					break
			if matched == False:
				self.add_load_error("At least one description record is mandatory. The start date of the first description period must " \
				"be equal to the start date of the geographical area. No two associated description periods for the same geographical area and language " \
				"may have the same start date. The validity period of the geographical area must span the validity period of the geographical area " \
				"description. Issue occurred on geographical area """ + code + """.""")


	def copy_xml_to_import_folder(self):
		self.d("Copying file to import directory", False)
		# Copy from self.output_filename to dest
		file_from	= os.path.join(self.XML_OUT_DIR,	self.output_filename)
		file_to		= os.path.join(self.IMPORT_DIR,	self.output_filename)
		#print (file_from)
		#print (file_to)
		shutil.copy (file_from, file_to)