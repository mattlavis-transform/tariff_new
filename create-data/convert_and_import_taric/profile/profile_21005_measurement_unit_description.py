import psycopg2
import common.globals as g

class profile_21005_measurement_unit_description(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date          = app.getTimestamp()
		measurement_unit_code   = app.getValue(oMessage, ".//oub:measurement.unit.code", True)
		language_id			    = app.getValue(oMessage, ".//oub:language.id", True)
		description			    = app.getValue(oMessage, ".//oub:description", True)

		if update_type == "1":	    # UPDATE
			operation = "U"
			app.doprint ("Updating measurement unit description " + str(measurement_unit_code))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting measurement unit description " + str(measurement_unit_code))
		else:					    # INSERT
			operation = "C"
			app.doprint ("Creating measurement unit description " + str(measurement_unit_code))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO measurement_unit_descriptions_oplog (measurement_unit_code, language_id, description, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s)""", 
			(measurement_unit_code, language_id, description, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("measurement unit description", operation, None, measurement_unit_code, transaction_id, message_id)
		cur.close()
