import psycopg2
import common.globals as g

class profile_21505_measurement_unit_qualifier_description(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date          = app.getTimestamp()
		measurement_unit_qualifier_code = app.get_value(oMessage, ".//oub:measurement.unit.qualifier.code", True)
		language_id			            = app.get_value(oMessage, ".//oub:language.id", True)
		description			            = app.get_value(oMessage, ".//oub:description", True)

		if update_type == "1":	    # UPDATE
			operation = "U"
			app.doprint ("Updating measurement unit qualifier description " + str(measurement_unit_qualifier_code))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting measurement unit qualifier description " + str(measurement_unit_qualifier_code))
		else:					    # INSERT
			operation = "C"
			app.doprint ("Creating measurement unit qualifier description " + str(measurement_unit_qualifier_code))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO measurement_unit_qualifier_descriptions_oplog (measurement_unit_qualifier_code, language_id, description, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s)""", 
			(measurement_unit_qualifier_code, language_id, description, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("measurement unit qualifier description", operation, None, measurement_unit_qualifier_code, transaction_id, message_id)
		cur.close()
