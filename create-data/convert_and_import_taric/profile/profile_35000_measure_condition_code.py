import psycopg2
import sys
from datetime import datetime
import common.globals as g

class profile_35000_measure_condition_code(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date		= app.getTimestamp()
		condition_code		= app.get_value(oMessage, ".//oub:condition.code", True)
		validity_start_date	= app.get_date_value(oMessage, ".//oub:validity.start.date", True)
		validity_end_date	= app.get_date_value(oMessage, ".//oub:validity.end.date", True)

		if update_type == "1":	# UPDATE
			operation = "U"
			app.doprint ("Updating measure condition " + str(condition_code))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting measure condition " + str(condition_code))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating measure condition " + str(condition_code))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO measure_condition_codes_oplog (condition_code, validity_start_date,
			validity_end_date, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s)""", 
			(condition_code, validity_start_date, validity_end_date, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("measure condition", operation, None, condition_code, transaction_id, message_id)
		cur.close()
