import psycopg2
import sys
from datetime import datetime
import common.globals as g

class profile_35500_measure_action(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date		= app.getTimestamp()
		action_code			= app.get_value(oMessage, ".//oub:action.code", True)
		validity_start_date	= app.get_date_value(oMessage, ".//oub:validity.start.date", True)
		validity_end_date	= app.get_date_value(oMessage, ".//oub:validity.end.date", True)

		if update_type == "1":	# UPDATE
			operation = "U"
			app.doprint ("Updating measure action " + str(action_code))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting measure action " + str(action_code))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating measure action " + str(action_code))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO measure_actions_oplog (action_code, validity_start_date,
			validity_end_date, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s)""", 
			(action_code, validity_start_date, validity_end_date, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("measure action", operation, None, action_code, transaction_id, message_id)
		cur.close()
