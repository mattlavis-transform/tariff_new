import psycopg2
import common.globals as g

class profile_23505_measure_type_description(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date          = app.getTimestamp()
		measure_type_id			= app.getValue(oMessage, ".//oub:measure.type.id", True)
		language_id				= app.getValue(oMessage, ".//oub:language.id", True)
		description				= app.getValue(oMessage, ".//oub:description", True)

		measure_types = g.app.get_measure_types()

		if update_type == "1":		# UPDATE
			if g.app.perform_taric_validation == True:
				if measure_type_id not in measure_types:
					g.app.add_load_error("MTx - The measure type code must exist for updates. Aborting while trying to update measure type description " + measure_type_id + ".")

			operation = "U"
			app.doprint ("Updating measure type description " + str(measure_type_id))
		
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting measure type description " + str(measure_type_id))
		
		else:						# INSERT
			operation = "C"
			app.doprint ("Creating measure type description " + str(measure_type_id))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO measure_type_descriptions_oplog (measure_type_id, language_id,
			description, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s)""", 
			(measure_type_id, language_id,
			description, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("measure type", operation, None, measure_type_id, transaction_id, message_id)
		cur.close()