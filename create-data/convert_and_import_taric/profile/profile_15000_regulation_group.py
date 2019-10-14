import psycopg2
import sys
from datetime import datetime
import common.globals as g

class profile_15000_regulation_group(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date		        = app.getTimestamp()
		regulation_group_id         = app.get_value(oMessage, ".//oub:regulation.group.id", True)
		validity_start_date	        = app.get_date_value(oMessage, ".//oub:validity.start.date", True)
		validity_end_date	        = app.get_date_value(oMessage, ".//oub:validity.end.date", True)

		regulation_groups = g.app.get_regulation_groups()

		if update_type == "1":	    # UPDATE
			if g.app.perform_taric_validation == True:
				if regulation_group_id not in regulation_groups:
					g.app.add_load_error("RG2 - Aborting while trying to update regulation group. " + regulation_group_id + " does not exist.")

			operation = "U"
			app.doprint ("Updating regulation group " + str(regulation_group_id))

		elif update_type == "2":	# DELETE
			if g.app.perform_taric_validation == True:
				if regulation_group_id not in regulation_groups:
					g.app.add_load_error("RG2 - Aborting while trying to delete regulation group. " + regulation_group_id + " does not exist. Please roll back.")

			operation = "D"
			app.doprint ("Deleting regulation group " + str(regulation_group_id))

		else:					    # INSERT
			if g.app.perform_taric_validation == True:
				if regulation_group_id in regulation_groups:
					g.app.add_load_error("RG1 - Aborting while trying to insert a regulation group. " + regulation_group_id + " already exists. Please roll back.")

			operation = "C"
			app.doprint ("Creating regulation group " + str(regulation_group_id))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO regulation_groups_oplog (regulation_group_id, validity_start_date,
			validity_end_date, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s)""", 
			(regulation_group_id, validity_start_date, validity_end_date, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("regulation group", operation, None, regulation_group_id, transaction_id, message_id)
		cur.close()
