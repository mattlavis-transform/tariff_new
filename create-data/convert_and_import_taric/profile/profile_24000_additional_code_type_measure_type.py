import psycopg2
import sys
from datetime import datetime
import common.globals as g

class profile_24000_additional_code_type_measure_type(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date		    = app.getTimestamp()
		measure_type_id			= app.get_value(oMessage, ".//oub:measure.type.id", True)
		additional_code_type_id = app.get_value(oMessage, ".//oub:additional.code.type.id", True)
		validity_start_date	    = app.get_date_value(oMessage, ".//oub:validity.start.date", True)
		validity_end_date	    = app.get_date_value(oMessage, ".//oub:validity.end.date", True)

		if update_type == "1":	# UPDATE
			operation = "U"
			app.doprint ("Updating additional code type / measure type " + str(measure_type_id))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting additional code type / measure type " + str(measure_type_id))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating additional code type / measure type " + str(measure_type_id))

		cur = app.conn.cursor()
		
		try:
			cur.execute("""INSERT INTO additional_code_type_measure_types_oplog (measure_type_id, additional_code_type_id,
            validity_start_date, validity_end_date, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s)""", 
			(measure_type_id, additional_code_type_id, validity_start_date, validity_end_date, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("additional code type / measure type", operation, None, measure_type_id, transaction_id, message_id)
		cur.close()
