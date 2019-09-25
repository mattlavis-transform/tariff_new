import xml.etree.ElementTree as ET
import xmlschema
import psycopg2
import sys
import csv
import os
import json
from os import system, name
import re
import codecs
import math
from datetime import datetime
from datetime import timedelta

from classes.footnote import footnote
from classes.footnote_type import footnote_type
from classes.certificate_type import certificate_type
from classes.certificate import certificate
from classes.measure_type import measure_type
from classes.measure import measure
from classes.measure_component import measure_component
from classes.measure_condition import measure_condition
from classes.measure_footnote import measure_footnote
from classes.base_regulation import base_regulation
from classes.geographical_area import geographical_area
from classes.geographical_area_membership import geographical_area_membership
from classes.measure_excluded_geographical_area import measure_excluded_geographical_area
from classes.regulation_group import regulation_group

import classes.functions as fn
from classes.progressbar import ProgressBar

class application(object):
	def __init__(self):
		self.BASE_DIR			= os.path.dirname(os.path.abspath(__file__))
		self.BASE_DIR			= os.path.join(self.BASE_DIR,	"..")
		self.CONFIG_DIR			= os.path.join(self.BASE_DIR, 	"..")
		self.CONFIG_DIR			= os.path.join(self.CONFIG_DIR, "config")
		self.CONFIG_FILE		= os.path.join(self.CONFIG_DIR, "config_common.json")
		self.CONFIG_FILE_LOCAL	= os.path.join(self.CONFIG_DIR, "config_vat_excise.json")

		self.SCHEMA_DIR			= os.path.join(self.BASE_DIR, 	"..")
		self.SCHEMA_DIR			= os.path.join(self.SCHEMA_DIR, 	"xsd")

		self.TEMPLATE_DIR		= os.path.join(self.BASE_DIR,	"templates")
		self.SOURCE_DIR 		= os.path.join(self.BASE_DIR,	"source")
		self.XML_OUT_DIR		= os.path.join(self.BASE_DIR,	"xml_out")

		self.content = ""
		self.vat_excise	= True
		self.excise		= True
		self.vat		= True

		self.get_config()
		self.measures_list						= []
		self.base_regulations_list				= []
		self.regulation_groups_list				= []
		self.footnotes_list						= []
		self.certificates_list					= []
		self.certificate_types_list				= []
		self.footnote_types_list				= []
		self.measure_types_list					= []
		self.geographical_areas_list			= []
		self.geographical_area_memberships_list	= []
		self.transaction_id						= 1
		self.sequence_id						= 1
		self.message_id							= 1
		self.namespaces = {'oub': 'urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0', 'env': 'urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0', } # add more as needed

		self.geographical_area_mappings			= []

		self.get_templates()
		self.connect()
		self.get_minimum_sids()
		self.clear()

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


	def get_filename(self):
		if self.vat_excise == False:
			if self.retain == True:
				self.output_filename = "national_pandr_retain.xml"
			else:
				self.output_filename = "national_pandr.xml"
		else:
			if self.vat == True:
				if self.vat_type_list == ["VTS"]:
					self.output_filename = "vat_vts.xml"
				else:
					self.output_filename = "vat_vtz_vta_vte.xml"
			else:
				self.output_filename = "excise.xml"

	def write_footnote_types(self):
		self.d("Writing XML for footnote types")
		p = ProgressBar(len(self.footnote_types_list), sys.stdout)
		cnt = 1
		self.content += "\t<!-- Begin footnote types //-->\n"
		for obj in self.footnote_types_list:
			p.print_progress(cnt)
			self.content += obj.xml()
			cnt += 1
		self.content += "\t<!-- End footnote types //-->\n"
		print ("\n")

	def write_regulation_groups(self):
		self.d("Writing XML for regulation groups")
		p = ProgressBar(len(self.regulation_groups_list), sys.stdout)
		cnt = 1
		self.content += "\t<!-- Begin regulation groups //-->\n"
		for obj in self.regulation_groups_list:
			p.print_progress(cnt)
			self.content += obj.xml()
			cnt += 1
		self.content += "\t<!-- End regulation groups //-->\n"
		print ("\n")

	def write_certificates(self):
		self.d("Writing XML for certificates")
		p = ProgressBar(len(self.certificates_list), sys.stdout)
		cnt = 1
		self.content += "\t<!-- Begin certificates //-->\n"
		for obj in self.certificates_list:
			p.print_progress(cnt)
			self.content += obj.xml()
			cnt += 1
		self.content += "\t<!-- End certificates //-->\n"
		print ("\n")


	def write_certificate_types(self):
		self.d("Writing data", False)
		self.d("Writing XML for certificate types")
		p = ProgressBar(len(self.certificate_types_list), sys.stdout)
		cnt = 1
		self.content += "\t<!-- Begin certificate types //-->\n"
		for obj in self.certificate_types_list:
			p.print_progress(cnt)
			self.content += obj.xml()
			cnt += 1
		self.content += "\t<!-- End certificate types //-->\n"
		print ("\n")

	def write_measure_types(self):
		self.d("Writing XML for measure types")
		p = ProgressBar(len(self.measure_types_list), sys.stdout)
		cnt = 1
		self.content += "\t<!-- Begin measure types //-->\n"
		for obj in self.measure_types_list:
			p.print_progress(cnt)
			self.content += obj.xml()
			cnt += 1
		self.content += "\t<!-- End measure types //-->\n"
		print ("\n")

	def write_geographical_areas(self):
		self.d("Writing XML for geographical areas")
		p = ProgressBar(len(self.geographical_areas_list), sys.stdout)
		cnt = 1
		self.content += "\t<!-- Begin geographical areas //-->\n"
		for obj in self.geographical_areas_list:
			p.print_progress(cnt)
			self.content += obj.xml()
			cnt += 1
		self.content += "\t<!-- End geographical areas //-->\n"
		print ("\n")

	def write_geographical_area_memberships(self):
		self.d("Writing XML for geographical area memberships")
		p = ProgressBar(len(self.geographical_area_memberships_list), sys.stdout)
		cnt = 1
		self.content += "\t<!-- Begin geographical area memberships //-->\n"
		for obj in self.geographical_area_memberships_list:
			p.print_progress(cnt)
			self.content += obj.xml()
			cnt += 1
		self.content += "\t<!-- End geographical memberships //-->\n"
		print ("\n")

	def write_footnotes(self):
		self.d("Writing XML for footnotes")
		p = ProgressBar(len(self.footnotes_list), sys.stdout)
		cnt = 1
		self.content += "\t<!-- Begin footnotes //-->\n"
		for obj in self.footnotes_list:
			p.print_progress(cnt)
			self.content += obj.xml()
			cnt += 1
		self.content += "\t<!-- End footnotes //-->\n"
		print ("\n")

	def write_regulations(self):
		self.d("Writing XML for base regulation")
		p = ProgressBar(len(self.base_regulations_list), sys.stdout)
		cnt = 1
		self.content += "\t<!-- Begin base regulations //-->\n"
		for obj in self.base_regulations_list:
			p.print_progress(cnt)
			self.content += obj.xml()
			cnt += 1
		self.content += "\t<!-- End base regulations //-->\n"
		print ("\n")

	def write_measures(self):
		self.d("Writing XML for national measures")

		xml_string = self.template_envelope.replace("[CONTENT]", self.content)
		xml_string = xml_string.replace("</env:envelope>", "")
		
		filename = os.path.join(self.XML_OUT_DIR,	self.output_filename)
		f = open(filename, "w+", encoding="utf8")
		f.write(xml_string)

		# Temp CSV write
		if self.vat:
			filename2 = os.path.join(self.XML_OUT_DIR,	"measures_vat.csv")
		elif self.excise:
			filename2 = os.path.join(self.XML_OUT_DIR,	"measures_excise.csv")
		else:
			filename2 = os.path.join(self.XML_OUT_DIR,	"measures_pandr.csv")
		f2 = open(filename2, "w+", encoding="utf8")

		p = ProgressBar(len(self.measures_list), sys.stdout)
		cnt = 1
		f.write("\t<!-- Begin measures //-->\n")

		# Sort the measures by commodity, by measuretype, date, starting with the oldest
		self.measures_list.sort(key=lambda x: x.validity_start_date, reverse = False)
		self.measures_list.sort(key=lambda x: x.measure_type_id, reverse = False)
		self.measures_list.sort(key=lambda x: x.goods_nomenclature_item_id, reverse = False)

		for obj in self.measures_list:
			p.print_progress(cnt)
			f.write(obj.xml())
			f2.write(obj.csv())
			cnt += 1

		f.write("\t<!-- End measures //-->\n")
		f.write("</env:envelope>")
		f.close()
		f2.close()

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

	def larger(self, a, b):
		if a > b:
			return a
		else:
			return b

	def get_config(self):
		# Choices are VTA, VTE, VTS, VTZ
		self.retain = False
		if sys.argv[0] == "vat.py":
			self.vat		= True
			self.excise		= False
			self.vat_excise	= True
			try:
				vat_type = sys.argv[1].upper()
				if vat_type == "VTS":
					self.vat_type_list = ["VTS"]
				else:
					self.vat_type_list = ["VTA", "VTE", "VTZ"]
			except:
				print ("VAT selected, but no VAT type chosen")
				sys.exit()
		elif sys.argv[0] == "excise.py":
			self.vat		= False
			self.excise		= True
			self.vat_excise	= True

		else:
			self.vat		= False
			self.excise		= False
			self.vat_excise	= False
			if len(sys.argv) > 1:
				try:
					if sys.argv[1] in ("r", "retain", "keep"):
						self.retain = True
					else:
						self.retain = False
				except:
					sys.exit()

		with open(self.CONFIG_FILE, 'r') as f:
			my_dict = json.load(f)

		self.DBASE					= my_dict['dbase_national']
		self.DBASE_STAGING			= my_dict['dbase']
		self.p						= my_dict['p']
		self.debug              	= fn.mbool2(my_dict['debug'])
		self.critical_date			= datetime.strptime(my_dict['critical_date'], '%Y-%m-%d')
		self.critical_date_plus_one	= self.critical_date + timedelta(days = 1)

	def get_scalar(self, sql):
		self.connect_staging()
		cur = self.conn_staging.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		return (rows[0][0])

	def roundup(self, a, digits=0):
		n = 10**-digits
		return round(math.ceil(a / n) * n, digits)

	def get_regulation_groups(self):
		self.d("Getting regulation groups")
		if self.vat_excise == False and self.retain == True:
			sql = """
			SELECT DISTINCT rg.regulation_group_id, rg.validity_start_date, rg.validity_end_date, rgd.description
			FROM base_regulations br, regulation_groups rg, regulation_group_descriptions rgd
			WHERE br.regulation_group_id = rg.regulation_group_id
			AND rg.regulation_group_id = rgd.regulation_group_id
			AND br.base_regulation_id IN (SELECT m.measure_generating_regulation_id
			FROM measures m, measure_types mt
			WHERE m.national = True
			AND m.measure_type_id = mt.measure_type_id
			AND measure_type_series_id IN ('B', 'P', 'Q')
			AND m.validity_start_date < CURRENT_DATE
			AND (m.validity_end_date > CURRENT_DATE OR m.validity_end_date IS NULL))
			ORDER BY 1
			"""
			cur = self.conn.cursor()
			cur.execute(sql)
			rows = cur.fetchall()
			for r in rows:
				regulation_group_id	= r[0]
				validity_start_date	= r[1]
				validity_end_date	= r[2]
				description			= r[3]

				obj = regulation_group(regulation_group_id, validity_start_date, validity_end_date, description)
				self.regulation_groups_list.append(obj)

	def get_regulations(self):
		self.d("Getting regulations")
		if self.vat_excise == True:
			if self.excise == True:
				sql = """
				SELECT DISTINCT * FROM base_regulations WHERE base_regulation_id IN (SELECT m.measure_generating_regulation_id
				FROM measures m, measure_types mt
				WHERE m.national = True
				AND m.measure_type_id = mt.measure_type_id
				AND measure_type_series_id IN ('xP', 'xQ')
				AND m.validity_start_date < CURRENT_DATE
				AND (m.validity_end_date > CURRENT_DATE OR m.validity_end_date IS NULL))
				"""
			else:
				sql = """
				SELECT DISTINCT * FROM base_regulations WHERE base_regulation_id IN (SELECT m.measure_generating_regulation_id
				FROM measures m, measure_types mt
				WHERE m.national = True
				AND m.measure_type_id = mt.measure_type_id
				AND measure_type_series_id IN ('xP', 'xQ')
				AND m.validity_start_date < CURRENT_DATE
				AND (m.validity_end_date > CURRENT_DATE OR m.validity_end_date IS NULL))
				"""
		else:
			sql = """
			SELECT DISTINCT * FROM base_regulations WHERE base_regulation_id IN (SELECT m.measure_generating_regulation_id
			FROM measures m, measure_types mt
			WHERE m.national = True
			AND m.measure_type_id = mt.measure_type_id
			AND measure_type_series_id IN ('B')
			AND m.validity_start_date < CURRENT_DATE
			AND (m.validity_end_date > CURRENT_DATE OR m.validity_end_date IS NULL))
			"""
		#print ("regulations", sql)
		#sys.exit()
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		for r in rows:
			base_regulation_role				= r[0]
			base_regulation_id					= r[1]
			validity_start_date					= r[2]
			validity_end_date					= r[3]
			community_code						= r[4]
			regulation_group_id					= r[5]
			replacement_indicator				= r[6]
			stopped_flag						= r[7]
			information_text					= r[8]
			approved_flag						= r[9]
			published_date						= r[10]
			officialjournal_number				= r[11]
			officialjournal_page				= r[12]
			effective_end_date					= r[13]
			antidumping_regulation_role			= r[14]
			related_antidumping_regulation_id	= r[15]
			complete_abrogation_regulation_role	= r[16]
			complete_abrogation_regulation_id	= r[17]
			explicit_abrogation_regulation_role	= r[18]
			explicit_abrogation_regulation_id	= r[19]

			b = base_regulation(base_regulation_role, base_regulation_id, validity_start_date, validity_end_date, community_code, regulation_group_id, replacement_indicator, stopped_flag, information_text, approved_flag, published_date, officialjournal_number, officialjournal_page, effective_end_date, antidumping_regulation_role, related_antidumping_regulation_id, complete_abrogation_regulation_role, complete_abrogation_regulation_id, explicit_abrogation_regulation_role, explicit_abrogation_regulation_id)
			
			self.base_regulations_list.append(b)


	def get_footnote_types(self):
		# This works for both P & R and VAT / excise
		# For P&R, this requires that the footnote types are moved over to the global structure though
		# With the load of national VAT and excise, this just needs us to give the data as is
		self.d("Getting footnote types")
		if self.vat_excise == True:
			if self.excise == True:
				sql = """
				SELECT ft.footnote_type_id, ft.validity_start_date, ft.validity_end_date, ft.application_code, ftd.description
				FROM footnote_types ft, footnote_type_descriptions ftd
				WHERE ft.footnote_type_id = ftd.footnote_type_id
				AND ft.footnote_type_id IN ('01', '02', '03')
				AND ft.national = True ORDER BY 1
				"""
			else:
				sql = """
				SELECT ft.footnote_type_id, ft.validity_start_date, ft.validity_end_date, ft.application_code, ftd.description
				FROM footnote_types ft, footnote_type_descriptions ftd
				WHERE ft.footnote_type_id = ftd.footnote_type_id
				AND ft.footnote_type_id IN ('01', '02', '03', '04')
				ORDER BY 1
				"""
			# The code above has been deliberately hamstrung to prevent the data from being created
			# All certificate types will be created as part of the P&R load
			# Certificate types are allowed to be alpha or numeric or combination in both Taric and National formats
		else:
			# We are presuming that ED will load the P&R first [VERY IMPORTANT] and the VAT and Excise second
			# In this respect, we have changed the code below to extract all data from the system
			# for all footnotes

			# For copying across to the Taric (i.e. non-national), the Footnotes will be migrated to use the "FM" footnote
			# type instead of the 01-04; need to really look into ensuring there are no duplicates
			sql = """
			SELECT ft.footnote_type_id, ft.validity_start_date, ft.validity_end_date, ft.application_code, ftd.description
			FROM footnote_types ft, footnote_type_descriptions ftd
			WHERE ft.footnote_type_id = ftd.footnote_type_id
			AND ft.footnote_type_id IN ('01', '02', '03','04')
			ORDER BY 1
			"""

		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		for r in rows:
			footnote_type_id				= r[0]
			validity_start_date				= r[1]
			validity_end_date				= r[2]
			application_code				= r[3]
			description						= r[4]

			b = footnote_type(footnote_type_id, validity_start_date, validity_end_date, application_code, description)
			self.footnote_types_list.append(b)
		a = 1
			
	def get_certificates(self):
		self.d("Getting certificates")
		sql = """
		SELECT DISTINCT cd1.certificate_type_code, cd1.certificate_code,
		c.validity_start_date, c.validity_end_date, cd1.description, cd1.certificate_description_period_sid
		FROM certificate_descriptions cd1, certificates c
		WHERE c.certificate_type_code = cd1.certificate_type_code
		AND c.certificate_code = cd1.certificate_code
		AND (cd1.oid IN ( SELECT max(cd2.oid) AS max
		FROM certificate_descriptions cd2
		WHERE cd1.certificate_type_code::text = cd2.certificate_type_code::text AND cd1.certificate_code::text = cd2.certificate_code::text))
		AND ((cd1.certificate_type_code = '9') OR (cd1.certificate_type_code = 'X' AND cd1.certificate_code = '018'))
		ORDER BY 1, 2
		"""
		#print (sql)
		#sys.exit()
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		for r in rows:
			certificate_type_code				= r[0]
			certificate_code					= r[1]
			validity_start_date					= r[2]
			validity_end_date					= r[3]
			description							= r[4]
			certificate_description_period_sid	= r[5]

			b = certificate(certificate_type_code, certificate_code, validity_start_date, validity_end_date, description, certificate_description_period_sid)
			self.certificates_list.append(b)
			
	def get_certificate_types(self):
		self.d("Retrieving information from database", False)
		self.d("Getting certificate types")
		sql = """
		SELECT ct.certificate_type_code, ct.validity_start_date, ct.validity_end_date, ctd.description
		FROM certificate_types ct, certificate_type_descriptions ctd 
		WHERE ct.certificate_type_code = ctd.certificate_type_code
		AND ct.certificate_type_code = '9' is True ORDER BY 1
		"""

		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		for r in rows:
			certificate_type_code	= r[0]
			validity_start_date		= r[1]
			validity_end_date		= r[2]
			description				= r[3]

			b = certificate_type(certificate_type_code, validity_start_date, validity_end_date, description)
			self.certificate_types_list.append(b)
			
	def get_geographical_area_memberships(self):
		self.d("Getting geographical area memberships")
		sql = """
		SELECT geographical_area_sid, geographical_area_group_sid, validity_start_date, validity_end_date
		FROM geographical_area_memberships WHERE geographical_area_group_sid IN (
		SELECT DISTINCT g.geographical_area_sid
		FROM geographical_areas g, measures m, measure_types mt
		WHERE m.national = True
		AND m.measure_type_id = mt.measure_type_id
		AND m.geographical_area_id = g.geographical_area_id
		AND measure_type_series_id IN ('B', 'P', 'Q')
		AND m.validity_start_date < CURRENT_DATE
		AND (m.validity_end_date > CURRENT_DATE OR m.validity_end_date IS NULL)
		AND length(m.geographical_area_id) = 4 AND LEFT(m.geographical_area_id, 1) > '9'
		) ORDER BY 1
		"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		for r in rows:
			geographical_area_sid			= r[0]
			geographical_area_group_sid		= r[1]
			validity_start_date				= r[2]
			validity_end_date				= r[3]

			obj = geographical_area_membership(geographical_area_sid, geographical_area_group_sid, validity_start_date, validity_end_date)
			self.geographical_area_memberships_list.append(obj)
			
	def get_geographical_areas(self):
		self.d("Getting geographical areas")

		self.geographical_area_id_from_list		= ['F006', 'D065', 'D064', 'D063', 'D010']
		self.geographical_area_id_to_list		= ['N006', 'N065', 'N064', 'N063', 'N010']

		self.geographical_area_sid_from_list	= [-306, -300, -299, -298, -248]
		self.geographical_area_sid_to_list		= [11000, 11001, 11002, 11003, 11004]

		self.geographical_area_description_period_sid_from_list	= [-309, -301, -300, -299, -248]
		self.geographical_area_description_period_sid_to_list	= [11000, 11001, 11002, 11003, 11004]

		sql = """
		SELECT geographical_area_sid, parent_geographical_area_group_sid, geographical_area_id, description,
		geographical_code, validity_start_date, validity_end_date
		FROM ml.ml_geographical_areas WHERE geographical_area_id IN (
		SELECT DISTINCT m.geographical_area_id
		FROM measures m, measure_types mt
		WHERE m.national = True
		AND m.measure_type_id = mt.measure_type_id
		AND measure_type_series_id IN ('B', 'P', 'Q')
		AND m.validity_start_date < CURRENT_DATE
		AND (m.validity_end_date > CURRENT_DATE OR m.validity_end_date IS NULL)
		AND length(m.geographical_area_id) = 4 AND LEFT(m.geographical_area_id, 1) > '9') ORDER BY 1
		"""
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		for r in rows:
			geographical_area_sid						= r[0]
			parent_geographical_area_group_sid			= r[1]
			geographical_area_id						= r[2]
			description									= r[3]
			geographical_code							= r[4]
			validity_start_date							= r[5]
			validity_end_date							= r[6]
			#geographical_area_description_period_sid	= r[7]
			geographical_area_description_period_sid = 0

			obj = geographical_area(geographical_area_sid, parent_geographical_area_group_sid, geographical_area_id,
			description, geographical_code, validity_start_date, validity_end_date, geographical_area_description_period_sid)
			self.geographical_areas_list.append(obj)
			

	def get_measure_types(self):
		self.d("Getting measure types")
		if self.vat_excise == True:
			if self.excise == True:
				# No longer relevant
				sql = """
				SELECT mt.measure_type_id, mt.validity_start_date, mt.validity_end_date, trade_movement_code, priority_code, measure_component_applicable_code,
				origin_dest_code, order_number_capture_code, measure_explosion_level, measure_type_series_id, mtd.description
				FROM measure_types mt, measure_type_descriptions mtd
				WHERE mt.measure_type_id = mtd.measure_type_id
				AND measure_type_series_id IN ('P', 'Q')
				AND measure_type_id > 'A'
				ORDER BY 1
				"""
			else:
				# No longer relevant
				sql = """
				SELECT mt.measure_type_id, mt.validity_start_date, mt.validity_end_date, trade_movement_code, priority_code, measure_component_applicable_code,
				origin_dest_code, order_number_capture_code, measure_explosion_level, measure_type_series_id, mtd.description
				FROM measure_types mt, measure_type_descriptions mtd
				WHERE mt.measure_type_id = mtd.measure_type_id
				AND measure_type_series_id IN ('xP', 'xQ')
				AND mt.national = True ORDER BY 1
				"""
		else:
			# Relevant
			sql = """
			SELECT mt.measure_type_id, mt.validity_start_date, mt.validity_end_date, trade_movement_code, priority_code,
			measure_component_applicable_code,
			origin_dest_code, order_number_capture_code, measure_explosion_level, measure_type_series_id, mtd.description
			FROM measure_types mt, measure_type_descriptions mtd
			WHERE mt.measure_type_id = mtd.measure_type_id
			AND measure_type_series_id IN ('B')
			AND mt.measure_type_id > 'A'
			ORDER BY 1
			"""
		#print ("measure types", sql)
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		for r in rows:
			measure_type_id						= r[0]
			validity_start_date					= r[1]
			validity_end_date					= r[2]
			trade_movement_code					= r[3]
			priority_code						= r[4]
			measure_component_applicable_code	= r[5]
			origin_dest_code					= r[6]
			order_number_capture_code			= r[7]
			measure_explosion_level				= r[8]
			measure_type_series_id				= r[9]
			description							= r[10]

			b = measure_type(measure_type_id, validity_start_date, validity_end_date, trade_movement_code,
			priority_code, measure_component_applicable_code, origin_dest_code, order_number_capture_code,
			measure_explosion_level, measure_type_series_id, description)
			self.measure_types_list.append(b)
			
	def get_footnotes(self):
		#print ("in get_footnotes")
		self.d("Getting footnotes")
		if self.vat_excise == True:
			if self.vat == True:
				# Not relevant anymore
				vat_types = fn.list_to_sql(self.vat_type_list)
				sql = """
				SELECT DISTINCT f.footnote_type_id, f.footnote_id, f.validity_start_date, f.validity_end_date, fd.description, fdp.footnote_description_period_sid
				FROM footnotes f, footnote_association_measures fam, footnote_descriptions fd, footnote_description_periods fdp
				WHERE f.footnote_id = fd.footnote_id AND f.footnote_type_id = fd.footnote_type_id 
				AND fd.footnote_id = fdp.footnote_id AND fd.footnote_type_id = fdp.footnote_type_id 
				AND f.footnote_id = fam.footnote_id AND f.footnote_type_id = fam.footnote_type_id 
				AND measure_sid IN (SELECT measure_sid FROM measures m, measure_types mt
				WHERE m.national = True AND m.measure_type_id = mt.measure_type_id
				AND measure_type_series_id IN ('P')
				AND m.measure_type_id IN (""" + vat_types + """)
				AND m.validity_start_date < CURRENT_DATE
				AND (m.validity_end_date > CURRENT_DATE OR m.validity_end_date IS NULL))
				"""
			else:
				# Not relevant anymore
				sql = """
				SELECT DISTINCT f.footnote_type_id, f.footnote_id, f.validity_start_date, f.validity_end_date, fd.description, fdp.footnote_description_period_sid
				FROM footnotes f, footnote_association_measures fam, footnote_descriptions fd, footnote_description_periods fdp
				WHERE f.footnote_id = fd.footnote_id AND f.footnote_type_id = fd.footnote_type_id 
				AND fd.footnote_id = fdp.footnote_id AND fd.footnote_type_id = fdp.footnote_type_id 
				AND f.footnote_id = fam.footnote_id AND f.footnote_type_id = fam.footnote_type_id 
				AND measure_sid IN (SELECT measure_sid FROM measures m, measure_types mt
				WHERE m.national = True AND m.measure_type_id = mt.measure_type_id
				AND measure_type_series_id IN ('Q')
				AND m.validity_start_date < CURRENT_DATE
				AND (m.validity_end_date > CURRENT_DATE OR m.validity_end_date IS NULL))
				"""
		else:
			# Relevant now
			sql = """
			SELECT DISTINCT f.footnote_type_id, f.footnote_id, f.validity_start_date, f.validity_end_date, fd.description, fdp.footnote_description_period_sid
			FROM footnotes f, footnote_association_measures fam, footnote_descriptions fd, footnote_description_periods fdp
			WHERE f.footnote_id = fd.footnote_id AND f.footnote_type_id = fd.footnote_type_id 
			AND fd.footnote_id = fdp.footnote_id AND fd.footnote_type_id = fdp.footnote_type_id 
			AND f.footnote_id = fam.footnote_id AND f.footnote_type_id = fam.footnote_type_id 
			AND measure_sid IN (SELECT measure_sid FROM measures m, measure_types mt
			WHERE f.footnote_type_id = '04'
			AND m.measure_type_id = mt.measure_type_id
			AND measure_type_series_id IN ('B')
			AND m.validity_start_date < CURRENT_DATE
			AND (m.validity_end_date > CURRENT_DATE OR m.validity_end_date IS NULL))
			"""


			sql = """
			select footnote_type_id, footnote_id, validity_start_date, validity_end_date, description
			from ml.ml_footnotes f
			where footnote_type_id = '04'
			order by footnote_id
			"""

		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		for r in rows:
			footnote_type_id				= r[0]
			footnote_id						= r[1]
			validity_start_date				= r[2]
			validity_end_date				= r[3]
			description						= r[4]

			b = footnote(footnote_type_id, footnote_id, validity_start_date, validity_end_date, description)
			self.footnotes_list.append(b)
			

	def get_measures(self):
		self.d("Getting measures")
		if self.vat_excise == True:
			if self.vat == True: # Vat
				# No longer relevant
				vat_types = fn.list_to_sql(self.vat_type_list)

				sql = """
				SELECT DISTINCT measure_sid, m.measure_type_id, m.geographical_area_id, m.goods_nomenclature_item_id,
				m.additional_code_type_id, m.additional_code_id, m.ordernumber, m.reduction_indicator,
				m.validity_start_date, m.measure_generating_regulation_role, m.measure_generating_regulation_id,
				m.validity_end_date, m.justification_regulation_role, m.justification_regulation_id, m.stopped_flag,
				m.geographical_area_sid, m.goods_nomenclature_sid, m.additional_code_sid, m.export_refund_nomenclature_sid
				FROM measures m, measure_types mt
				WHERE m.national = True
				AND m.measure_type_id = mt.measure_type_id
				AND measure_type_series_id IN ('P')
				AND m.measure_type_id IN (""" + vat_types + """)
				AND m.validity_start_date < CURRENT_DATE
				AND (m.validity_end_date > CURRENT_DATE OR m.validity_end_date IS NULL)
				ORDER BY m.measure_type_id, m.goods_nomenclature_item_id, m.validity_start_date DESC
				"""
			else: # Excise
				# No longer relevant
				sql = """
				SELECT measure_sid, m.measure_type_id, m.geographical_area_id, m.goods_nomenclature_item_id, m.additional_code_type_id, m.additional_code_id, m.ordernumber, m.reduction_indicator, m.validity_start_date, m.measure_generating_regulation_role, m.measure_generating_regulation_id, m.validity_end_date, m.justification_regulation_role, m.justification_regulation_id, m.stopped_flag, m.geographical_area_sid, m.goods_nomenclature_sid, m.additional_code_sid, m.export_refund_nomenclature_sid
				FROM measures m, measure_types mt
				WHERE m.national = True
				AND m.measure_type_id = mt.measure_type_id
				AND measure_type_series_id IN ('Q')
				AND m.validity_start_date < CURRENT_DATE
				AND (m.validity_end_date > CURRENT_DATE OR m.validity_end_date IS NULL)
				ORDER BY m.measure_type_id, m.goods_nomenclature_item_id, m.validity_start_date DESC
				"""
		else: # Prohibitions and restrictions (P & R)
			# This is the one
			sql = """
			SELECT measure_sid, m.measure_type_id, m.geographical_area_id, m.goods_nomenclature_item_id, m.additional_code_type_id,
			m.additional_code_id, m.ordernumber, m.reduction_indicator, m.validity_start_date, m.measure_generating_regulation_role,
			m.measure_generating_regulation_id, m.validity_end_date, m.justification_regulation_role, m.justification_regulation_id,
			m.stopped_flag, m.geographical_area_sid, m.goods_nomenclature_sid, m.additional_code_sid, m.export_refund_nomenclature_sid
			FROM measures m, measure_types mt, goods_nomenclatures gn
			WHERE m.measure_type_id in ('AHC', 'AIL', 'ATT', 'CEX', 'COE', 'COI', 'CVD', 'EQC', 'HOP', 'HSE', 'PHC', 'PRE', 'PRT', 'QRC')
			AND m.measure_type_id = mt.measure_type_id
			AND measure_type_series_id IN ('B')
			AND (m.validity_end_date > CURRENT_DATE OR m.validity_end_date IS NULL)
			AND gn.goods_nomenclature_item_id = m.goods_nomenclature_item_id
			AND gn.producline_suffix = '80'
			/*
			AND m.goods_nomenclature_item_id != '2939799000'
			AND m.goods_nomenclature_item_id != '0303894500'
			AND NOT (m.goods_nomenclature_item_id = '0307810000' AND m.measure_type_id = 'AHC')
			AND NOT (m.goods_nomenclature_item_id = '0307119000' AND m.measure_type_id = 'AHC')
			AND NOT (m.goods_nomenclature_item_id = '0307119000' AND m.measure_type_id = 'AHC')
			AND NOT (m.goods_nomenclature_item_id = '0307710000' AND m.measure_type_id = 'AHC')
			*/
			AND gn.validity_end_date IS NULL
			ORDER BY m.measure_type_id, m.goods_nomenclature_item_id, m.validity_start_date DESC
			"""
		#print ("measures", sql)
		#sys.exit()
		cur = self.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		for r in rows:
			measure_sid							= r[0]
			measure_type_id						= fn.mstr(r[1])
			geographical_area_id				= fn.mstr(r[2])
			goods_nomenclature_item_id			= fn.mstr(r[3])
			additional_code_type_id				= fn.mstr(r[4])
			additional_code_id					= fn.mstr(r[5])
			ordernumber							= fn.mstr(r[6])
			reduction_indicator					= fn.mstr(r[7])
			validity_start_date					= r[8]
			measure_generating_regulation_role	= fn.mstr(r[9])
			measure_generating_regulation_id	= fn.mstr(r[10])
			validity_end_date					= r[11]
			justification_regulation_role		= fn.mstr(r[12])
			justification_regulation_id			= fn.mstr(r[13])
			stopped_flag						= fn.mstr(r[14])
			geographical_area_sid				= r[15]
			goods_nomenclature_sid				= r[16]
			additional_code_sid					= r[17]
			export_refund_nomenclature_sid		= r[18]
			
			m = measure(measure_sid, measure_type_id, geographical_area_id, goods_nomenclature_item_id, additional_code_type_id, additional_code_id, ordernumber, reduction_indicator, validity_start_date, measure_generating_regulation_role, measure_generating_regulation_id, validity_end_date, justification_regulation_role, justification_regulation_id, stopped_flag, geographical_area_sid, goods_nomenclature_sid, additional_code_sid, export_refund_nomenclature_sid)
			
			self.measures_list.append(m)

		
		self.append_end_dates = False
		if self.append_end_dates == True:
			if self.vat or self.excise or self.retain:
				last_goods_nomenclature_item_id = ""
				last_measure_type_id			= ""
				last_start_date					= "2099-01-01"
				for m in self.measures_list:
					if (m.goods_nomenclature_item_id == last_goods_nomenclature_item_id) and (m.measure_type_id == last_measure_type_id):
						m.justification_regulation_id = m.measure_generating_regulation_id
						m.justification_regulation_role = m.measure_generating_regulation_role
						temp = datetime.strptime(last_start_date, "%Y-%m-%dT%H:%M:%S") - timedelta(minutes = 1) # Take away one minute
						m.validity_end_date = datetime.strftime(temp, "%Y-%m-%dT%H:%M:%S")
						#print (m.validity_start_date, m.validity_end_date)
						#sys.exit()
					
					last_goods_nomenclature_item_id = m.goods_nomenclature_item_id
					last_measure_type_id			= m.measure_type_id
					last_start_date					= m.validity_start_date
					
		self.measures_list.sort(key=lambda x: x.measure_sid, reverse = False)


	def get_measure_components(self):
		# Get components related to all these measures
		self.d("Getting measure components")

		if self.vat_excise == True:
			if self.vat == True:
				vat_types = fn.list_to_sql(self.vat_type_list)
				sql = """SELECT measure_sid, duty_expression_id, duty_amount, monetary_unit_code,
				measurement_unit_code, measurement_unit_qualifier_code FROM measure_components WHERE measure_sid IN (
				SELECT measure_sid
				FROM measures m, measure_types mt
				WHERE m.national = True
				AND m.measure_type_id = mt.measure_type_id
				AND m.measure_type_id IN (""" + vat_types + """)
				AND measure_type_series_id IN ('P')
				AND m.validity_start_date < CURRENT_DATE
				AND (m.validity_end_date > CURRENT_DATE OR m.validity_end_date IS NULL))
				ORDER BY measure_sid
				"""
			else:
				sql = """SELECT measure_sid, duty_expression_id, duty_amount, monetary_unit_code,
				measurement_unit_code, measurement_unit_qualifier_code FROM measure_components WHERE measure_sid IN (
				SELECT measure_sid
				FROM measures m, measure_types mt
				WHERE m.national = True
				AND m.measure_type_id = mt.measure_type_id
				AND measure_type_series_id IN ('Q')
				AND m.validity_start_date < CURRENT_DATE
				AND (m.validity_end_date > CURRENT_DATE OR m.validity_end_date IS NULL))
				ORDER BY measure_sid
				"""
		else:
			sql = """SELECT measure_sid, duty_expression_id, duty_amount, monetary_unit_code,
			measurement_unit_code, measurement_unit_qualifier_code FROM measure_components WHERE measure_sid IN (
			SELECT measure_sid
			FROM measures m, measure_types mt
			WHERE m.national = True
			AND m.measure_type_id = mt.measure_type_id
			AND measure_type_series_id IN ('B')
			AND m.validity_start_date < CURRENT_DATE
			AND (m.validity_end_date > CURRENT_DATE OR m.validity_end_date IS NULL))
			ORDER BY measure_sid
			"""

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

		self.d("Assigning measure components to measures")
		p = ProgressBar(len(self.measure_component_list), sys.stdout)
		cnt = 1
		start = 0
		for c in self.measure_component_list:
			found = False
			for i in range(start, len(self.measures_list)):
				m = self.measures_list[i]
				#print (c.measure_sid, m.measure_sid)
				if (c.measure_sid + 0) == (m.measure_sid + 0):
					found = True
					m.measure_component_list.append(c)
					start = i
				else:
					if found == True:
						break
			p.print_progress(cnt)
			cnt +=1
		print ("\n")


	def get_measure_excluded_geographical_areas(self):
		self.d("Getting measure excluded geographical areas")
		if self.vat_excise == True:
			sql = """
			SELECT mega.measure_sid, mega.excluded_geographical_area, mega.geographical_area_sid
			FROM measure_excluded_geographical_areas mega, measures m, measure_types mt
			WHERE m.measure_sid = mega.measure_sid
			AND m.measure_type_id = mt.measure_type_id
			AND m.national = True
			AND measure_type_series_id IN ('P', 'Q')
			AND m.validity_start_date < CURRENT_DATE
			AND (m.validity_end_date > CURRENT_DATE OR m.validity_end_date IS NULL)
			ORDER BY 1
			"""
		else:
			sql = """
			SELECT mega.measure_sid, mega.excluded_geographical_area, mega.geographical_area_sid
			FROM measure_excluded_geographical_areas mega, measures m, measure_types mt
			WHERE m.measure_sid = mega.measure_sid
			AND m.measure_type_id = mt.measure_type_id
			AND m.national = True
			AND measure_type_series_id IN ('B')
			AND m.validity_start_date < CURRENT_DATE
			AND (m.validity_end_date > CURRENT_DATE OR m.validity_end_date IS NULL)
			ORDER BY 1
			"""
			cur = self.conn.cursor()
			cur.execute(sql)
			rows = cur.fetchall()

			self.measure_excluded_geographical_area_list = []
			for obj in rows:
				measure_sid					= obj[0]
				excluded_geographical_area	= obj[1]
				geographical_area_sid		= obj[2]

				obj = measure_excluded_geographical_area(measure_sid, excluded_geographical_area, geographical_area_sid)
				self.measure_excluded_geographical_area_list.append(obj)

			self.d("Assigning measure excluded geographical areas to measures")
			p = ProgressBar(len(self.measure_excluded_geographical_area_list), sys.stdout)
			cnt = 1
			start = 0

			for obj in self.measure_excluded_geographical_area_list:
				found = False
				for i in range(start, len(self.measures_list)):
					m = self.measures_list[i]
					if (obj.measure_sid) == (m.measure_sid):
						found = True
						m.measure_excluded_geographical_area_list.append(obj)
						#print ("Appending to the measure object")
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

		if self.vat_excise == True:
			self.measure_condition_list = [] # There are no conditions associated with VAT and Excise
			return
		else:
			sql = """
			SELECT m.measure_sid, mc.measure_condition_sid, mc.condition_code, mc.component_sequence_number, mc.condition_duty_amount,
			mc.condition_monetary_unit_code, mc.condition_measurement_unit_code, mc.condition_measurement_unit_qualifier_code,
			mc.action_code, mc.certificate_type_code, mc.certificate_code
			FROM measures m, measure_types mt, measure_conditions mc
			left outer join certificates c on mc.certificate_code = c.certificate_code and mc.certificate_type_code = c.certificate_type_code
			WHERE m.measure_type_id = mt.measure_type_id
			and mc.measure_sid = m.measure_sid
			and m.validity_start_date < CURRENT_DATE
			and (m.validity_end_date > CURRENT_DATE OR m.validity_end_date IS NULL)
			and mt.measure_type_series_id IN ('B')
			and m.measure_type_id in ('AHC', 'AIL', 'ATT', 'CEX', 'COE', 'COI', 'CVD', 'EQC', 'HOP', 'HSE', 'PHC', 'PRE', 'PRT', 'QRC')
			ORDER BY m.measure_sid, mc.component_sequence_number
			"""

			cur = self.conn.cursor()
			cur.execute(sql)
			rows = cur.fetchall()

			self.measure_condition_list = []
			for obj in rows:
				measure_sid									= obj[0]
				measure_condition_sid						= obj[1]
				condition_code								= obj[2]
				component_sequence_number					= obj[3]
				condition_duty_amount						= obj[4]
				condition_monetary_unit_code				= obj[5]
				condition_measurement_unit_code				= obj[6]
				condition_measurement_unit_qualifier_code	= obj[7]
				action_code									= obj[8]
				certificate_type_code						= obj[9]
				certificate_code							= obj[10]

				measure_condition_object = measure_condition(measure_sid, measure_condition_sid, condition_code,
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
				for i in range(start, len(self.measures_list)):
					m = self.measures_list[i]
					if (c.measure_sid + 0) == (m.measure_sid + 0):
						found = True
						m.measure_condition_list.append(c)
						start = i
					else:
						if found == True:
							#break
							pass
				p.print_progress(cnt)
				cnt +=1
			print ("\n")

	def get_measure_footnotes(self):
		self.d("Getting measure footnote associations")

		if self.vat_excise == True:
			if self.vat == True:
				vat_types = fn.list_to_sql(self.vat_type_list)
				sql = """SELECT measure_sid, footnote_type_id, footnote_id FROM footnote_association_measures WHERE measure_sid IN (
				SELECT measure_sid
				FROM measures m, measure_types mt
				WHERE m.national = True
				AND m.measure_type_id = mt.measure_type_id
				AND measure_type_series_id IN ('P')
				AND m.measure_type_id IN (""" + vat_types + """)
				AND m.validity_start_date < CURRENT_DATE
				AND (m.validity_end_date > CURRENT_DATE OR m.validity_end_date IS NULL))
				ORDER BY measure_sid
				"""
			else:
				sql = """SELECT measure_sid, footnote_type_id, footnote_id FROM footnote_association_measures WHERE measure_sid IN (
				SELECT measure_sid
				FROM measures m, measure_types mt
				WHERE m.national = True
				AND m.measure_type_id = mt.measure_type_id
				AND measure_type_series_id IN ('Q')
				AND m.validity_start_date < CURRENT_DATE
				AND (m.validity_end_date > CURRENT_DATE OR m.validity_end_date IS NULL))
				ORDER BY measure_sid
				"""
		else:
			sql = """SELECT measure_sid, footnote_type_id, footnote_id FROM footnote_association_measures WHERE measure_sid IN (
			SELECT measure_sid
			FROM measures m, measure_types mt
			WHERE m.national = True
			AND m.measure_type_id = mt.measure_type_id
			AND measure_type_series_id IN ('B')
			AND m.validity_start_date < CURRENT_DATE
			AND (m.validity_end_date > CURRENT_DATE OR m.validity_end_date IS NULL))
			ORDER BY measure_sid
			"""
		#print ("measure footnotes", sql)
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
			for i in range(start, len(self.measures_list)):
				m = self.measures_list[i]
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


	def connect(self):
		self.conn = psycopg2.connect("dbname=" + self.DBASE + " user=postgres password=" + self.p)

	def connect_staging(self):
		self.conn_staging = psycopg2.connect("dbname=" + self.DBASE_STAGING + " user=postgres password=" + self.p)

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

		# Get template - footnote.association.measure
		filename = os.path.join(self.TEMPLATE_DIR,	"footnote.association.measure.xml")
		file = open(filename, mode = 'r')
		self.template_footnote_association_measure = file.read()
		file.close()

		# Get template - footnote
		filename = os.path.join(self.TEMPLATE_DIR,	"footnote.xml")
		file = open(filename, mode = 'r')
		self.template_footnote = file.read()
		file.close()

		# Get template - measure type
		filename = os.path.join(self.TEMPLATE_DIR,	"measure_type.xml")
		file = open(filename, mode = 'r')
		self.template_measure_type = file.read()
		file.close()

		# Get template - footnote type
		filename = os.path.join(self.TEMPLATE_DIR,	"footnote_type.xml")
		file = open(filename, mode = 'r')
		self.template_footnote_type = file.read()
		file.close()

		# Get template - base regulation
		filename = os.path.join(self.TEMPLATE_DIR,	"base.regulation.xml")
		file = open(filename, mode = 'r')
		self.template_base_regulation = file.read()
		file.close()

		# Get template - base regulation
		filename = os.path.join(self.TEMPLATE_DIR,	"base.regulation.xml")
		file = open(filename, mode = 'r')
		self.template_base_regulation = file.read()
		file.close()

		# Get template - geographical areas
		filename = os.path.join(self.TEMPLATE_DIR,	"geographical.area.insert.xml")
		file = open(filename, mode = 'r')
		self.template_geographical_area = file.read()
		file.close()

		# Get template - geographical area memberships
		filename = os.path.join(self.TEMPLATE_DIR,	"geographical.area.membership.insert.xml")
		file = open(filename, mode = 'r')
		self.template_geographical_area_membership = file.read()
		file.close()

		# Get template - geographical area measure exclusions
		filename = os.path.join(self.TEMPLATE_DIR,	"measure.excluded.geographical.area.insert.xml")
		file = open(filename, mode = 'r')
		self.template_measure_excluded_geographical_area = file.read()
		file.close()

		# Get template - certificate types
		filename = os.path.join(self.TEMPLATE_DIR,	"certificate.type.xml")
		file = open(filename, mode = 'r')
		self.template_certificate_type = file.read()
		file.close()

		# Get template - certificates
		filename = os.path.join(self.TEMPLATE_DIR,	"certificate.insert.xml")
		file = open(filename, mode = 'r')
		self.template_certificate = file.read()
		file.close()

		# Get template - regulation groups
		filename = os.path.join(self.TEMPLATE_DIR,	"regulation.group.xml")
		file = open(filename, mode = 'r')
		self.template_regulation_group = file.read()
		file.close()


	def write_content(self):
		# Deprecated
		if len(self.content) == 0:
			print ("No matching measures found")
			sys.exit()
		else:
			xml_string = self.template_envelope.replace("[CONTENT]", self.content)
			filename = os.path.join(self.XML_OUT_DIR,	self.output_filename)
			f = open(filename, "w+", encoding="utf8")
			f.write(xml_string)
			f.close()

	def validate(self):
		fname = os.path.join(self.XML_OUT_DIR,	self.output_filename)
		if self.retain or self.vat_excise:
			schema_path = os.path.join(self.SCHEMA_DIR, "envelopeNational.xsd")
		else:
			schema_path = os.path.join(self.SCHEMA_DIR, "envelope.xsd")
		msg = "Validating the XML file against the Taric 3 schema - using " + schema_path
		self.d(msg, False)
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

