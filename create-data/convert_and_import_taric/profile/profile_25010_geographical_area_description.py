import psycopg2
import common.globals as g

class profile_25010_geographical_area_description(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date                  			= app.getTimestamp()
		geographical_area_description_period_sid	= app.getNumberValue(oMessage, ".//oub:geographical.area.description.period.sid", True)
		language_id									= app.getValue(oMessage, ".//oub:language.id", True)
		geographical_area_sid						= app.getNumberValue(oMessage, ".//oub:geographical.area.sid", True)
		geographical_area_id 				   		= app.getValue(oMessage, ".//oub:geographical.area.id", True)
		description									= app.getValue(oMessage, ".//oub:description", True)

		if update_type == "1":	# Update
			operation = "U"
			app.doprint ("Updating geographical area description " + str(geographical_area_description_period_sid))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting geographical area description " + str(geographical_area_description_period_sid))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating geographical area description " + str(geographical_area_description_period_sid))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO geographical_area_descriptions_oplog (geographical_area_description_period_sid,
			language_id, geographical_area_sid, geographical_area_id,
			description, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s, %s)""", 
			(geographical_area_description_period_sid,
			language_id, geographical_area_sid, geographical_area_id,
			description, operation, operation_date))
		except:
			g.app.log_error("geographical area description", operation, geographical_area_description_period_sid, geographical_area_id, transaction_id, message_id)
		app.conn.commit()
		cur.close()
