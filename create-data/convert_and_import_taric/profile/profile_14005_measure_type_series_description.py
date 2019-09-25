import psycopg2
import common.globals as g

class profile_14005_measure_type_series_description(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date                  = app.getTimestamp()
		measure_type_series_id			= app.getValue(oMessage, ".//oub:measure.type.series.id", True)
		language_id						= app.getValue(oMessage, ".//oub:language.id", True)
		description						= app.getValue(oMessage, ".//oub:description", True)

		if update_type == "1":	# UPDATE
			operation = "U"
			app.doprint ("Updating measure type series description " + str(measure_type_series_id))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting measure type series description " + str(measure_type_series_id))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating measure type series description " + str(measure_type_series_id))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO measure_type_series_descriptions_oplog (measure_type_series_id, language_id,
			description, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s)""", 
			(measure_type_series_id, language_id,
			description, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("measure type series description", operation, None, measure_type_series_id, transaction_id, message_id)
		cur.close()
