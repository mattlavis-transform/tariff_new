import psycopg2
import sys
from datetime import datetime
import common.globals as g

class profile_14000_measure_type_series(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date		        = app.getTimestamp()
		measure_type_series_id      = app.getValue(oMessage, ".//oub:measure.type.series.id", True)
		validity_start_date	        = app.getDateValue(oMessage, ".//oub:validity.start.date", True)
		validity_end_date	        = app.getDateValue(oMessage, ".//oub:validity.end.date", True)
		measure_type_combination    = app.getValue(oMessage, ".//oub:measure.type.combination", True)

		if update_type == "1":	# UPDATE
			operation = "U"
			app.doprint ("Updating measure type series " + str(measure_type_series_id))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting measure type series " + str(measure_type_series_id))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating measure type series " + str(measure_type_series_id))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO measure_type_series_oplog (measure_type_series_id, validity_start_date,
			validity_end_date, measure_type_combination, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s)""", 
			(measure_type_series_id, validity_start_date, validity_end_date, measure_type_combination, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("measure type series", operation, None, measure_type_series_id, transaction_id, message_id)
		cur.close()
