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

from classes.progressbar import ProgressBar
from classes.quota_order_number import quota_order_number
from classes.quota_order_number_origin import quota_order_number_origin
from classes.quota_definition import quota_definition
from classes.measure import measure
from classes.geographical_area import geographical_area
from classes.goods_nomenclature import goods_nomenclature
from classes.measure_excluded_geographical_area import measure_excluded_geographical_area
import classes.functions as fn


class application(object):
    def __init__(self):
        self.clear()

        self.BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        self.BASE_DIR = os.path.join(self.BASE_DIR, "..")
        self.TEMPLATE_DIR = os.path.join(self.BASE_DIR, "templates")
        self.CSV_DIR = os.path.join(self.BASE_DIR, "source")
        self.SOURCE_DIR = os.path.join(self.BASE_DIR, "source")
        self.XML_OUT_DIR = os.path.join(self.BASE_DIR, "xml_out")
        self.XML_REPORT_DIR = os.path.join(self.BASE_DIR, "xml_report")
        self.TEMP_DIR = os.path.join(self.BASE_DIR, "temp")
        self.TEMP_FILE = os.path.join(self.TEMP_DIR, "temp.xml")
        self.LOG_DIR = os.path.join(self.BASE_DIR, "log")
        self.IMPORT_LOG_DIR = os.path.join(self.LOG_DIR, "import")
        self.LOG_FILE = os.path.join(self.LOG_DIR, "log.csv")
        self.MERGE_DIR = os.path.join(self.BASE_DIR, "..")
        self.MERGE_DIR = os.path.join(self.MERGE_DIR, "migrate_reference_data")
        self.MERGE_DIR = os.path.join(self.MERGE_DIR, "xml")
        self.DUMP_DIR = os.path.join(self.BASE_DIR, "dump")

        self.CONFIG_DIR = os.path.join(self.BASE_DIR, "..")
        self.CONFIG_DIR = os.path.join(self.CONFIG_DIR, "config")
        self.CONFIG_FILE = os.path.join(self.CONFIG_DIR, "config_common.json")
        self.CONFIG_FILE_LOCAL = os.path.join(self.CONFIG_DIR, "config_migrate_measures_and_quotas.json")

        self.SCHEMA_DIR = os.path.join(self.BASE_DIR, "..")
        self.SCHEMA_DIR = os.path.join(self.SCHEMA_DIR, "xsd")

        self.SOURCE_DIR = os.path.join(self.BASE_DIR, "source")
        self.QUOTA_DIR = os.path.join(self.SOURCE_DIR, "quotas")
        self.BALANCE_FILE = os.path.join(self.QUOTA_DIR, "quota_volume_master.csv")
        self.QUOTA_DESCRIPTION_FILE = os.path.join(self.QUOTA_DIR, "quota definitions.csv")
        self.MFN_COMPONENTS_FILE = os.path.join(self.SOURCE_DIR, "mfn_components.csv")
        self.TEMPLATE_DIR = os.path.join(self.BASE_DIR, "templates")

        self.DBASE = "tariff_staging"

        self.envelope_id = "100000001"
        self.sequence_id = 1
        self.content = ""
        self.namespaces = {'oub': 'urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0', 'env': 'urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0', }  # add more as needed

        self.measure_list = []
        self.quota_definition_list = []
        self.quota_order_number_list = []

        self.get_config()
        self.connect()
        self.get_minimum_sids()
        self.get_templates()
        self.message_id = 1
        self.debug = True

        if len(sys.argv) > 1:
            self.output_profile = sys.argv[1]
            msg = "Getting data for GSP jurisdiction " + self.output_profile
            self.d(msg, False)
            self.output_filename = os.path.join(self.XML_OUT_DIR, "gsp_" + self.output_profile.strip() + ".xml")
        else:
            print("No profile specified")
            sys.exit()

    def get_config(self):
        with open(self.CONFIG_FILE, 'r') as f:
            my_dict = json.load(f)

        self.DBASE = my_dict['dbase']
        self.p = my_dict['p']
        self.transaction_id = my_dict["minimum_sids"][self.DBASE]["last_transaction_id"]

        critical_date = my_dict['critical_date']
        self.critical_date = datetime.strptime(critical_date, '%Y-%m-%d')
        self.critical_date_plus_one = self.critical_date + timedelta(days=1)
        self.critical_date_plus_one_string = datetime.strftime(self.critical_date_plus_one, '%Y-%m-%d')

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

    def write_xml(self):
        file = open(self.output_filename, "w+")
        msg = "Writing XML file for jurisdiction " + self.output_profile + " to file " + self.output_filename
        self.d(msg, True)
        xml = '<?xml version="1.0" encoding="UTF-8"?>\n'
        xml += '<env:envelope xmlns="urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0" xmlns:env="urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0" id="ENV">\n'
        file.write(xml)
        # Write the measures
        for m in self.measure_list:
            if m.goods_nomenclature_item_id in self.valid_goods_nomenclature_list:
                xml = m.xml()
                file.write(xml)

        xml = '</env:envelope>'
        file.write(xml)
        file.close()
        msg = "File write complete."
        self.d(msg, True)

    def get_measures_from_csv(self):
        self.measure_list = []
        self.goods_nomenclature_list = []
        my_file = os.path.join(self.CSV_DIR, "gsp_" + self.output_profile + ".csv")
        with open(my_file) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=",")
            for row in csv_reader:
                if (len(row) > 0):
                    geographical_area_id = row[0]
                    goods_nomenclature_item_id = row[1]
                    duty_amount = row[2]
                    start_date_override = fn.mstr(row[4])
                    end_date_override = fn.mstr(row[5])
                    """
                    try:
                        exclusion = row[3]
                    except:
                        exclusion = ""
                    """

                    if (goods_nomenclature_item_id != "goods nomenclature") and (goods_nomenclature_item_id != ""):
                        obj = measure(geographical_area_id, goods_nomenclature_item_id, duty_amount, start_date_override, end_date_override)
                        self.measure_list.append(obj)
                        self.goods_nomenclature_list.append(goods_nomenclature_item_id)

    def associate_exclusions(self):
        if self.output_profile == "2020":
            for m in self.measure_list:
                for x in self.exclusions_list:
                    if x.goods_nomenclature_item_id == m.goods_nomenclature_item_id:
                        # print("found an exclusion")
                        m.measure_excluded_geographical_area_list.append(x)

    def get_quota_order_numbers_from_csv(self):
        self.quota_order_number_list = []
        my_file = os.path.join(self.CSV_DIR, "quota_order_numbers.csv")
        with open(my_file) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=",")
            for row in csv_reader:
                if (len(row) > 0):
                    quota_order_number_id = row[0]
                    regulation_id = row[1]
                    measure_type_id = row[2]
                    origin_string = row[3]
                    origin_exclusion_string = row[4]
                    validity_start_date = row[5]
                    subject = row[6]
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
            csv_reader = csv.reader(csv_file, delimiter=",")
            for row in csv_reader:
                if (len(row) > 0):
                    quota_order_number_id = row[0]
                    validity_start_date = row[1]
                    validity_end_date = row[2]
                    initial_volume = row[3]
                    measurement_unit_code = row[4]
                    maximum_precision = row[5]
                    critical_state = row[6]
                    critical_threshold = row[7]
                    monetary_unit_code = row[8]
                    measurement_unit_qualifier_code = row[9]
                    blocking_period_start = row[10]
                    blocking_period_end = row[11]

                    obj = quota_definition(quota_order_number_id, validity_start_date, validity_end_date, initial_volume,
                    measurement_unit_code, maximum_precision, critical_state, critical_threshold, monetary_unit_code,
                    measurement_unit_qualifier_code, blocking_period_start, blocking_period_end)

                    self.quota_definition_list.append(obj)

    def get_geographical_areas(self):
        sql = """SELECT geographical_area_id, geographical_area_sid FROM geographical_areas
        WHERE geographical_area_id IN ('2020', '2027', '2005', '1032', 'IN', 'ID', 'KE', 'UA');"""
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        if len(rows) > 0:
            self.geographical_area_list = []
            for rw in rows:
                geographical_area_id = rw[0]
                geographical_area_sid = rw[1]
                g = geographical_area(geographical_area_id, geographical_area_sid)
                self.geographical_area_list.append(g)

        # sys.exit()

    def get_minimum_sids(self):
        with open(self.CONFIG_FILE, 'r') as f:
            my_dict = json.load(f)

        self.min_list = my_dict['minimum_sids'][self.DBASE]

        self.last_additional_code_description_period_sid = self.larger(self.get_scalar("SELECT MAX(additional_code_description_period_sid) FROM additional_code_description_periods_oplog;"), self.min_list['additional.code.description.periods']) + 1
        self.last_additional_code_sid = self.larger(self.get_scalar("SELECT MAX(additional_code_sid) FROM additional_codes_oplog;"), self.min_list['additional.codes']) + 1

        self.last_certificate_description_period_sid = self.larger(self.get_scalar("SELECT MAX(certificate_description_period_sid) FROM certificate_description_periods_oplog;"), self.min_list['certificate.description.periods']) + 1
        self.last_footnote_description_period_sid = self.larger(self.get_scalar("SELECT MAX(footnote_description_period_sid) FROM footnote_description_periods_oplog;"), self.min_list['footnote.description.periods']) + 1
        self.last_geographical_area_description_period_sid = self.larger(self.get_scalar("SELECT MAX(geographical_area_description_period_sid) FROM geographical_area_description_periods_oplog;"), self.min_list['geographical.area.description.periods']) + 1
        self.last_geographical_area_sid = self.larger(self.get_scalar("SELECT MAX(geographical_area_sid) FROM geographical_areas_oplog;"), self.min_list['geographical.areas']) + 1

        self.last_goods_nomenclature_sid = self.larger(self.get_scalar("SELECT MAX(goods_nomenclature_sid) FROM goods_nomenclatures_oplog;"), self.min_list['goods.nomenclature']) + 1
        self.last_goods_nomenclature_indent_sid = self.larger(self.get_scalar("SELECT MAX(goods_nomenclature_indent_sid) FROM goods_nomenclature_indents_oplog;"), self.min_list['goods.nomenclature.indents']) + 1
        self.last_goods_nomenclature_description_period_sid = self.larger(self.get_scalar("SELECT MAX(goods_nomenclature_description_period_sid) FROM goods_nomenclature_description_periods_oplog;"), self.min_list['goods.nomenclature.description.periods']) + 1

        self.last_measure_sid = self.larger(self.get_scalar("SELECT MAX(measure_sid) FROM measures_oplog;"), self.min_list['measures']) + 1
        self.last_measure_condition_sid = self.larger(self.get_scalar("SELECT MAX(measure_condition_sid) FROM measure_conditions_oplog"), self.min_list['measure.conditions']) + 1

        self.last_quota_order_number_sid = self.larger(self.get_scalar("SELECT MAX(quota_order_number_sid) FROM quota_order_numbers_oplog"), self.min_list['quota.order.numbers']) + 1
        self.last_quota_order_number_origin_sid = self.larger(self.get_scalar("SELECT MAX(quota_order_number_origin_sid) FROM quota_order_number_origins_oplog"), self.min_list['quota.order.number.origins']) + 1
        self.last_quota_definition_sid = self.larger(self.get_scalar("SELECT MAX(quota_definition_sid) FROM quota_definitions_oplog"), self.min_list['quota.definitions']) + 1
        self.last_quota_suspension_period_sid = self.larger(self.get_scalar("SELECT MAX(quota_suspension_period_sid) FROM quota_suspension_periods_oplog"), self.min_list['quota.suspension.periods']) + 1
        self.last_quota_blocking_period_sid = self.larger(self.get_scalar("SELECT MAX(quota_blocking_period_sid) FROM quota_blocking_periods_oplog"), self.min_list['quota.blocking.periods']) + 1

    def larger(self, a, b):
        if a > b:
            return a
        else:
            return b

    def get_scalar(self, sql):
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        # l = list(rows)
        return (rows[0][0])

    def d(self, s, include_indent=True):
        if self.debug:
            if include_indent:
                s = "- " + s
            else:
                s = "\n" + s.upper()
            print(s)

    def clear(self):
        # for windows
        if name == 'nt':
            _ = system('cls')
        # for mac and linux(here, os.name is 'posix')
        else:
            _ = system('clear')

    def connect(self):
        self.conn = psycopg2.connect("dbname=" + self.DBASE + " user=postgres password=" + self.p)

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
            csv_reader = csv.reader(csv_file, delimiter=",")
            for row in csv_reader:
                if (len(row) > 0):
                    geographical_area_id_group = row[0]
                    goods_nomenclature_item_id = row[1]
                    geographical_area_id_group_excluded = row[2]

                    if (geographical_area_id_group != ""):
                        obj = measure_excluded_geographical_area(geographical_area_id_group, goods_nomenclature_item_id, geographical_area_id_group_excluded)
                        self.exclusions_list.append(obj)

    def set_config(self):
        jsonFile = open(self.CONFIG_FILE, "r")  # Open the JSON file for reading
        data = json.load(jsonFile)  # Read the JSON into the buffer
        jsonFile.close()  # Close the JSON file

        data["minimum_sids"][self.DBASE]["last_transaction_id"] = self.transaction_id
        data["minimum_sids"][self.DBASE]["measures"] = self.last_measure_sid
        data["minimum_sids"][self.DBASE]["measure.conditions"] = self.last_measure_condition_sid

        self.d("Completing extract", False)
        msg = "The last measure ID is " + str(self.last_measure_sid)
        self.d(msg, True)

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
        WHERE goods_nomenclature_item_id IN (""" + clause + """)
        AND (validity_end_date IS NULL or validity_end_date > '""" + self.critical_date_plus_one_string + """')
        ORDER BY 1"""

        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        if len(rows) > 0:
            self.goods_nomenclature_list = []
            self.valid_goods_nomenclature_list = []
            for rw in rows:
                goods_nomenclature_item_id = rw[0]
                validity_start_date = rw[1]
                validity_end_date = rw[2]
                g = goods_nomenclature(goods_nomenclature_item_id, "80", validity_start_date, validity_end_date)
                self.goods_nomenclature_list.append(g)
                self.valid_goods_nomenclature_list.append(goods_nomenclature_item_id)
