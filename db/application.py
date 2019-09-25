import sys
import os
from os import system, name
import psycopg2
from datetime import datetime
from datetime import timedelta
from sh import pg_dump
import time

class application(object):
	def __init__(self):
		self.clear()
		self.p = "zanzibar"

		self.get_config()
		self.BASE_DIR		= os.path.dirname(os.path.abspath(__file__))
		self.BACKUP_DIR		= "/Users/matt.admin/tariff_data/"
		self.BACKUP_DIR		= os.path.join(self.BACKUP_DIR, self.database_name)
		self.connect()


	def get_config(self):
		self.database_name = "tariff_dev"
		# Get name of database to backup
		err_msg = '\nPlease enter a genuine database name - aborting\n\nUse one of the following - "tariff_dev", "tariff_eu", "t_eu", "tariff_staging", "tariff_cds", "tariff_load", "tariff_steve", "tariff_tap", "tariff_fta"'
		try:
			self.database_name = sys.argv[1]
		except:
			print (err_msg)
			sys.exit()
		if self.database_name not in ("tariff_dev", "t_eu", "tariff_eu", "tariff_staging", "tariff_national", "tariff_load", "tariff_cds", "tariff_steve", "tariff_tap", "tariff_fta"):
			print (err_msg)
			sys.exit()
		# Optionally, you may choose to just back up a singel schema - this can be specified in the second parameter
		try:
			self.schema = sys.argv[2]
			if self.schema not in ("public", "ml", "schema"):
				print ("Please enter a valid schema name or leave blank to backup all schemas")
				sys.exit()
		except:
			self.schema = ""


	def db_backup(self):
		start = time.time()
		self.get_latest_import_file()
		self.date_string = datetime.today().strftime('%y%m%d')
		self.output_filename = self.database_name + "_" + self.import_string + "_" + self.date_string
		if self.schema != "":
			self.output_filename += "_" + self.schema
		self.output_filename += ".backup"
		self.path = os.path.join(self.BACKUP_DIR, self.output_filename)
		print ("Backing up database", self.database_name, "to file:\n" + self.path)
		print ("\nThis may take a few minutes ...\n")
		if self.schema == "schema":
			pg_dump('-h', 'localhost', '-U', 'postgres', '-s', self.database_name, _out=self.path)
		elif self.schema != "":
			pg_dump('-h', 'localhost', '-U', 'postgres', '-F', 'c', '-Z', '9', '--schema', self.schema, self.database_name, _out=self.path)
		else:
			#pg_dump('-h', 'localhost', '-U', 'postgres', '-F', 'c', '-Z', '9', self.database_name, _out=self.path)
			pg_dump('-h', 'localhost', '-U', 'postgres', '-F', 'c', self.database_name, _out=self.path)
		end = time.time()
		print ("Backup complete in", "{0:.1f}".format(end - start), "seconds\n")
		pass

	def get_latest_import_file(self):
		self.import_string = "base"
		try:
			# Get the latest EU data load
			sql = """select import_file from ml.import_files
			where import_completed is not null
			order by import_completed desc, import_file desc limit 1"""
			cur = self.conn.cursor()
			cur.execute(sql)
			rows = cur.fetchall()
			if len(rows) > 0:
				self.import_string = rows[0][0]
		except:
			self.import_string = "base"

		# Get the latest UK data load - this should supersede the EU file
		try:
			sql = """select import_file from ml.import_files where import_file like '%DIT%'
			order by import_file desc limit 1"""
			cur = self.conn.cursor()
			cur.execute(sql)
			rows = cur.fetchall()
			if len(rows) > 0:
				self.import_string = rows[0][0]
		except:
			self.import_string = "base"

		# Remove the .xml from the end of the import string
		self.import_string = self.import_string.replace(".xml", "")

		pass

	def clear(self):
		# for windows
		if name == 'nt':
			_ = system('cls')
		# for mac and linux(here, os.name is 'posix')
		else:
			_ = system("printf '\33c\e[3J'")

	def connect(self):
		self.conn = psycopg2.connect("dbname=" + self.database_name + " user=postgres password=" + self.p)
