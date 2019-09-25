import psycopg2
import sys
from datetime import datetime
import common.globals as g

class profile_21000_measurement_unit(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date		    = app.getTimestamp()
		measurement_unit_code	= app.getValue(oMessage, ".//oub:measurement.unit.code", True)
		validity_start_date	    = app.getDateValue(oMessage, ".//oub:validity.start.date", True)
		validity_end_date	    = app.getDateValue(oMessage, ".//oub:validity.end.date", True)

		if update_type == "1":	# UPDATE
			operation = "U"
			app.doprint ("Updating measurement unit " + str(measurement_unit_code))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting measurement unit " + str(measurement_unit_code))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating measurement unit " + str(measurement_unit_code))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO measurement_units_oplog (measurement_unit_code, validity_start_date,
			validity_end_date, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s)""", 
			(measurement_unit_code, validity_start_date, validity_end_date, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("measure unit", operation, None, measurement_unit_code, transaction_id, message_id)
		cur.close()
