import psycopg2
import common.globals as g
import sys
from datetime import datetime

class profile_29000_modification_regulation(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date						= app.getTimestamp()
		modification_regulation_role		= app.get_value(oMessage, ".//oub:modification.regulation.role", True)
		modification_regulation_id			= app.get_value(oMessage, ".//oub:modification.regulation.id", True)
		published_date		        		= app.get_date_value(oMessage, ".//oub:published.date", True)
		officialjournal_number			    = app.get_value(oMessage, ".//oub:officialjournal.number", True)
		officialjournal_page	            = app.get_value(oMessage, ".//oub:officialjournal.page", True)
		validity_start_date					= app.get_date_value(oMessage, ".//oub:validity.start.date", True)
		validity_end_date					= app.get_date_value(oMessage, ".//oub:validity.end.date", True)
		effective_end_date					= app.get_date_value(oMessage, ".//oub:effective.end.date", True)
		base_regulation_role				= app.get_value(oMessage, ".//oub:base.regulation.role", True)
		base_regulation_id					= app.get_value(oMessage, ".//oub:base.regulation.id", True)
		complete_abrogation_regulation_role	= app.get_value(oMessage, ".//oub:complete.abrogation.regulation.role", True)
		complete_abrogation_regulation_id	= app.get_value(oMessage, ".//oub:complete.abrogation.regulation.id", True)
		explicit_abrogation_regulation_role	= app.get_value(oMessage, ".//oub:explicit.abrogation.regulation.role", True)
		explicit_abrogation_regulation_id	= app.get_value(oMessage, ".//oub:explicit.abrogation.regulation.id", True)
		replacement_indicator               = app.get_value(oMessage, ".//oub:replacement.indicator", True)
		stopped_flag                  		= app.get_value(oMessage, ".//oub:stopped.flag", True)
		information_text                  	= app.get_value(oMessage, ".//oub:information.text", True)
		approved_flag                  		= app.get_value(oMessage, ".//oub:approved.flag", True)

		
		
		# Get the earlier of the 2 - effective or validity end dates
		if validity_end_date != None and effective_end_date != None:
			if validity_end_date < effective_end_date:
				my_end_date = validity_end_date
			else:
				my_end_date = effective_end_date
		elif validity_end_date != None:
			my_end_date = validity_end_date
		elif validity_end_date != None:
			my_end_date = effective_end_date
		else:
			my_end_date = None

		if my_end_date != None:
			my_end_date_string = my_end_date.strftime("%Y-%m-%d")

			if validity_end_date != None or effective_end_date != None:
				sql = """select * from ml.measures_real_end_dates
				where measure_generating_regulation_id = '""" + modification_regulation_id + """'
				and measure_generating_regulation_role = '""" + modification_regulation_role + """'
				and validity_end_date is not null
				and validity_end_date > '""" + my_end_date_string + """'
				"""

				cur = g.app.conn.cursor()
				cur.execute(sql)
				rows = cur.fetchall()
				if len(rows) != 0:
					g.app.add_load_error("Modification regulation found with a specific end date - this will cause issues " + str(modification_regulation_id))


		if update_type == "1":	# Update
			operation = "U"
			app.doprint ("Updating modification regulation " + str(modification_regulation_id))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting modification regulation " + str(modification_regulation_id))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating modification regulation " + str(modification_regulation_id))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO modification_regulations_oplog (modification_regulation_role,
			modification_regulation_id, published_date, officialjournal_number, officialjournal_page, 
			validity_start_date, validity_end_date, effective_end_date, 
			base_regulation_role, base_regulation_id,
			complete_abrogation_regulation_role, complete_abrogation_regulation_id,
			explicit_abrogation_regulation_role, explicit_abrogation_regulation_id, 
			replacement_indicator, stopped_flag, information_text, approved_flag,
			operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", 
			(modification_regulation_role,
			modification_regulation_id, published_date, officialjournal_number, officialjournal_page, 
			validity_start_date, validity_end_date, effective_end_date, 
			base_regulation_role, base_regulation_id,
			complete_abrogation_regulation_role, complete_abrogation_regulation_id,
			explicit_abrogation_regulation_role, explicit_abrogation_regulation_id, 
			replacement_indicator, stopped_flag, information_text, approved_flag,
			operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("modification regulation", operation, None, modification_regulation_id, transaction_id, message_id)
		cur.close()

		if g.app.perform_taric_validation == True:
			g.app.all_regulations = g.app.get_all_regulations()
