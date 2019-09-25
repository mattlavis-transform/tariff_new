import psycopg2
import sys
import os
from os import system, name 
import csv
import json
from datetime import datetime

import functions as f
from document import document
from hierarchy import hierarchy
from mfn_duty import mfn_duty
from meursing_component import meursing_component
from local_eps import local_eps

class application(object):
	def __init__(self):
		self.clear()
		self.debug				= False
		self.country_codes		= ""

		# Initialise extended information
		self.geographical_area_name = ""
		self.agreement_title		= ""
		self.agreement_date			= ""
		self.version				= ""
		self.country_codes			= ""
		self.origin_quotas			= ""
		self.licensed_quota_volumes	= ""
		self.quota_scope			= ""
		self.quota_staging			= ""
		
		self.BASE_DIR			= os.path.dirname(os.path.abspath(__file__))
		self.SOURCE_DIR			= os.path.join(self.BASE_DIR, "source")
		self.CSV_DIR			= os.path.join(self.BASE_DIR, "csv")
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
		self.BALANCE_FILE_EXCEL	= os.path.join(self.BALANCE_DIR, "November 2019 Quota Volume Master.xlsx")


		# For the word model folders
		self.MODEL_DIR			= os.path.join(self.BASE_DIR, "model")
		self.WORD_DIR			= os.path.join(self.MODEL_DIR, "word")
		self.DOCPROPS_DIR		= os.path.join(self.MODEL_DIR, "docProps")

		# For the output folders
		self.OUTPUT_DIR			= os.path.join(self.BASE_DIR, "output")

		self.get_config()

		# Find the country code
		try:
			self.country_profile = sys.argv[1]
		except:
			print ("No country scope parameter found - ending")
			sys.exit()

		self.get_extended_profile()
		self.get_country_list()
		self.geo_ids = f.list_to_sql(self.country_codes)
		self.get_meursing_components_for_erga_omnes()
		self.get_mfns_for_eps_products()
		self.get_local_eps()

	def get_extended_profile(self):
		# This function has been added to derive additional information to the FTA object
		# to support the display of the quota table in FTA reference documents, which are
		# currently unsupported.
		sql = """select geographical_area_name, agreement_title, agreement_date,
		version, country_codes, origin_quotas, licensed_quota_volumes,
		quota_scope, quota_staging
		from ml.extended_trade_agreement_information where fta_name = '""" + self.country_profile + """'"""

		cur = self.conn_uk.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		if len(rows) > 0:
			rw = rows[0]
			self.geographical_area_name = rw[0]
			self.agreement_title		= rw[1]
			self.agreement_date			= rw[2]
			self.version				= rw[3]
			self.country_codes			= rw[4]
			self.origin_quotas			= rw[5]
			self.licensed_quota_volumes	= rw[6]
			self.quota_scope			= rw[7]
			self.quota_staging			= rw[8]
		else:
			print ("Extended information on quotas not found - please review profile")
			sys.exit()

	def create_document(self):
		# Create the document
		my_document = document()

		# Check if there are any quotas associated with this geographical area
		# If there are, then we will use the XML document template that contains
		# the quota table, if not then we will not
		my_document.check_for_quotas()

		# Read in the XML component templates that are used to build document.xml
		self.read_templates(my_document.has_quotas)

		# Create the measures table
		my_document.get_duties("preferences")
		my_document.write_tariff_preferences()

		# Create the quotas table
		my_document.get_duties("quotas")
		my_document.get_quota_order_numbers()
		my_document.get_quota_balances_from_excel()
		my_document.get_quota_measures()
		my_document.get_quota_definitions()
		my_document.write_quotas()

		"""
		my_document.start_rta()
		my_document.get_duties_rta("preferences")
		my_document.write_tariff_preferences_rta()
		my_document.end_rta()
		"""

		# Personalise and write the document
		my_document.create_core()
		my_document.write()
		print ("\nPROCESS COMPLETE - File written to " + my_document.FILENAME + "\n")
			
	def clear(self): 
		# for windows 
		if name == 'nt': 
			_ = system('cls') 
		# for mac and linux(here, os.name is 'posix') 
		else: 
			_ = system('clear')

	def get_config(self):
		# Get global config items
		with open(self.CONFIG_FILE, 'r') as f:
			my_dict = json.load(f)

		#self.DBASE	= my_dict['dbase']
		#self.DBASE	= "tariff_eu"
		
		self.DBASE_EU	= my_dict['dbase_reference_documents']
		self.DBASE_UK	= my_dict['dbase_reference_documents']
		
		self.p		= my_dict['p']

		# Get local config items
		with open(self.CONFIG_FILE_LOCAL, 'r') as f:
			my_dict = json.load(f)

		self.all_country_profiles = my_dict['country_profiles']

		# Connect to the database
		#print (self.DBASE)
		self.connect()

	def get_country_list(self):
		try:
			self.country_codes = self.all_country_profiles[self.country_profile]["country_codes"]
		except:
			print ("Country profile does not exist")
			sys.exit()
		
		# Get exclusions
		try:
			self.exclusion_check = self.all_country_profiles[self.country_profile]["exclusion_check"]
		except:
			self.exclusion_check = ""
			pass

		# Get agreement name
		self.agreement_name			= self.all_country_profiles[self.country_profile]["agreement_name"]

		self.agreement_date_short	= self.all_country_profiles[self.country_profile]["agreement_date"]
		temp = datetime.strptime(self.agreement_date_short, "%d/%m/%Y")
		self.agreement_date_long	= datetime.strftime(temp, "%d %B %Y").lstrip("0")

		self.table_per_country		= self.all_country_profiles[self.country_profile]["table_per_country"]
		self.version				= self.all_country_profiles[self.country_profile]["version"]
		self.country_name			= self.all_country_profiles[self.country_profile]["country_name"]

	def connect(self):
		self.conn_uk = psycopg2.connect("dbname=" + self.DBASE_UK + " user=postgres password=" + self.p)
		self.conn_eu = psycopg2.connect("dbname=" + self.DBASE_EU + " user=postgres password=" + self.p)

	def shutDown(self):
		self.conn_uk.close()
		self.conn_eu.close()

	def read_templates(self, has_quotas):
		# Read in the XML fragments that will make up the pieced together HTML document.
		# All of these fragments are stored in the xmlcomponents subfolder
		# This function also creates the overall document template and adds in the specific
		# data to replace the placeholders (e.g. agreement name, version etc.)
		self.COMPONENT_DIR = os.path.join(self.COMPONENT_DIR, "")
		if has_quotas:
			fDocument = open(os.path.join(self.COMPONENT_DIR, "document_hasquotas.xml"), "r")
		else:
			fDocument = open(os.path.join(self.COMPONENT_DIR, "document_noquotas.xml"), "r")
		self.sDocumentXML = fDocument.read()
		self.sDocumentXML = self.sDocumentXML.replace("{AGREEMENT_NAME}",		self.agreement_name)
		self.sDocumentXML = self.sDocumentXML.replace("{VERSION}",				self.version)
		self.sDocumentXML = self.sDocumentXML.replace("{AGREEMENT_DATE}",		self.agreement_date_long)
		self.sDocumentXML = self.sDocumentXML.replace("{AGREEMENT_DATE_SHORT}",	self.agreement_date_short)
		self.sDocumentXML = self.sDocumentXML.replace("{COUNTRY_NAME}",			self.country_name)

		# Get the template used to represent a Heading 1 style in the Word document
		fHeading1 = open(os.path.join(self.COMPONENT_DIR, "heading1.xml"), "r") 
		self.sHeading1XML = fHeading1.read()

		# Get the template used to represent a Heading 2 style in the Word document
		fHeading2 = open(os.path.join(self.COMPONENT_DIR, "heading2.xml"), "r") 
		self.sHeading2XML = fHeading2.read()

		# Get the template used to represent a Heading 3 style in the Word document
		fHeading3 = open(os.path.join(self.COMPONENT_DIR, "heading3.xml"), "r") 
		self.sHeading3XML = fHeading3.read()

		# Get the template used to represent a standard paragraph in the Word document
		fPara = open(os.path.join(self.COMPONENT_DIR, "paragraph.xml"), "r") 
		self.sParaXML = fPara.read()

		# Get the template used to represent a bullet in the Word document
		fBullet = open(os.path.join(self.COMPONENT_DIR, "bullet.xml"), "r") 
		self.sBulletXML = fBullet.read()

		# Get the template used to represent the tariff preference schedule table in the Word document
		fTable    = open(os.path.join(self.COMPONENT_DIR, "table_schedule.xml"), "r") 
		self.sTableXML = fTable.read()

		# Get the template used to represent a row within the tariff preference schedule table in the Word document
		fTableRow = open(os.path.join(self.COMPONENT_DIR, "tablerow_schedule.xml"), "r") 
		self.sTableRowXML = fTableRow.read()

		# Get the template used to represent the quota schedule table in the Word document
		fQuotaTable    = open(os.path.join(self.COMPONENT_DIR, "table_quota.xml"), "r") 
		self.sQuotaTableXML = fQuotaTable.read()

		# Get the template used to represent the a row of the quota schedule table in the Word document
		fQuotaTableRow = open(os.path.join(self.COMPONENT_DIR, "tablerow_quota.xml"), "r")
		self.sQuotaTableRowXML = fQuotaTableRow.read()

		# Horizontal line for putting dividers into the quota table
		fHorizLine = open(os.path.join(self.COMPONENT_DIR, "horiz_line.xml"), "r") 
		self.sHorizLineXML = fHorizLine.read()

		# Soft horizontal line for putting dividers into the quota table
		fHorizLineSoft = open(os.path.join(self.COMPONENT_DIR, "horiz_line_soft.xml"), "r") 
		self.sHorizLineSoftXML = fHorizLineSoft.read()

		# core.xml that contains document information
		fCore = open(os.path.join(self.COMPONENT_DIR, "core.xml"), "r") 
		self.sCoreXML = fCore.read()

	def get_local_eps(self):
		# Get all commodities where there is an Entry Price System-related measure
		# This must come from the EU database
		sql = """
		SELECT DISTINCT m.goods_nomenclature_item_id, m.validity_start_date, mc.condition_duty_amount,
		mc.condition_monetary_unit_code, mc.condition_measurement_unit_code
		FROM ml.v5_2019 m, measure_conditions mc, measure_condition_components mcm
		WHERE mc.measure_sid = m.measure_sid
		AND mc.measure_condition_sid = mcm.measure_condition_sid
		AND mc.condition_code = 'V' AND geographical_area_id IN (""" + self.geo_ids + """) AND mcm.duty_amount != 0
		ORDER BY 1, 2 DESC, 3 DESC
		"""

		cur = self.conn_eu.cursor()
		cur.execute(sql)
		rows = cur.fetchall()

		self.local_eps					= []
		self.local_eps_commodities_only	= []

		for rw in rows:
			goods_nomenclature_item_id		= rw[0]
			validity_start_date				= rw[1]
			condition_duty_amount			= rw[2]
			condition_monetary_unit_code	= rw[3]
			condition_measurement_unit_code	= rw[4]

			obj = local_eps(goods_nomenclature_item_id, validity_start_date, condition_duty_amount, condition_monetary_unit_code, condition_measurement_unit_code)
			self.local_eps.append(obj)
			self.local_eps_commodities_only.append(goods_nomenclature_item_id)

	
	def get_mfns_for_eps_products(self):
		# This function gets a list of all of the MFNs on the database that have Entry Price System
		# threshold conditions attached to them; these will then be used later in the calculation of
		# the rebase price for commodities that are subject to the Entry Price System.

		# This must come from the production EU database rather than from the UK database

		sql = """SELECT DISTINCT m.goods_nomenclature_item_id, mcc.duty_amount, m.validity_start_date, m.validity_end_date
		FROM measures m, measure_conditions mc, measure_condition_components mcc
		WHERE mcc.measure_condition_sid = mc.measure_condition_sid
		AND m.measure_sid = mc.measure_sid
		AND mcc.duty_expression_id = '01'
		AND (m.validity_start_date > '2018-01-01')
		AND mc.condition_code = 'V'
		AND m.measure_type_id IN ('103', '105')
		AND m.geographical_area_id = '1011'
		ORDER BY m.goods_nomenclature_item_id, m.validity_start_date
		"""
		cur = self.conn_eu.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		self.mfn_list = []
		for r in rows:
			goods_nomenclature_item_id	= r[0]
			duty_amount					= r[1]
			validity_start_date			= r[2]
			validity_end_date			= r[3]
			mfn = mfn_duty(goods_nomenclature_item_id, duty_amount, validity_start_date, validity_end_date)
			self.mfn_list.append(mfn)

	def get_mfn_rate(self, commodity_code, validity_start_date, validity_end_date):
		mfn_rate = 0.0
		found = False
		for mfn in self.mfn_list:
			if commodity_code == mfn.commodity_code:
				if validity_start_date == mfn.validity_start_date:
					mfn_rate = mfn.duty_amount
					found = True
					break
		if found == False:
			#print ("Error matching SIVs on", commodity_code, " for date", validity_start_date)
			if commodity_code[8:10] != "00":
				commodity_code = commodity_code[0:8] + "00"
				#print (commodity_code)
				#sys.exit()
				mfn_rate = self.get_mfn_rate(commodity_code, validity_start_date, validity_end_date)
			elif commodity_code[6:10] != "0000":
				commodity_code = commodity_code[0:6] + "00"
				mfn_rate = self.get_mfn_rate(commodity_code, validity_start_date, validity_end_date)
				#pass
		return (mfn_rate)

	def get_meursing_components_for_erga_omnes(self):
		# Take an average of the duty amount chargeable on Meursing components for RoW (Erga Omnes)
		# This will then be used in working out the Rebase P clause of measures that are
		# subject to Meursing agricultural duties in the EU' tariff
		# Please note this uses a custom SQL view from the ml schema (ml.meursing_components)
		# This must come from the EU database
		sql = "SELECT AVG(duty_amount) FROM ml.meursing_components WHERE geographical_area_id = '1011'"
		cur = self.conn_eu.cursor()
		cur.execute(sql)
		row = cur.fetchone()
		self.erga_omnes_average = row[0]


	def get_meursing_percentage(self, reduction_indicator, geographical_area_id):
		# Get the Erga Omnes Meursing average
		# Derive information from the EU database, as this is not going to be available
		# in the UK database after a valid data load
		sql = """
		SELECT AVG(duty_amount) FROM ml.meursing_components WHERE geographical_area_id = '""" + geographical_area_id + """' AND reduction_indicator = """ + str(reduction_indicator)
		cur = self.conn_eu.cursor()
		cur.execute(sql)
		row = cur.fetchone()
		reduced_average = row[0]
		try:
			reduction = round((reduced_average / self.erga_omnes_average) * 100)
		except:
			reduction = 100
		return (reduction)
