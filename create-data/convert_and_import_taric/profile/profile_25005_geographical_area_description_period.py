import psycopg2
import common.globals as g

class profile_25005_geographical_area_description_period(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date				    			= app.getTimestamp()
		geographical_area_description_period_sid	= app.get_number_value(oMessage, ".//oub:geographical.area.description.period.sid", True)
		geographical_area_sid			    		= app.get_number_value(oMessage, ".//oub:geographical.area.sid", True)
		validity_start_date			    			= app.get_date_value(oMessage, ".//oub:validity.start.date", True)
		geographical_area_id					    = app.get_value(oMessage, ".//oub:geographical.area.id", True)

		if update_type == "1":	# Update
			operation = "U"
			app.doprint ("Updating geographical area description period " + str(geographical_area_description_period_sid))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting geographical area description period " + str(geographical_area_description_period_sid))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating geographical area description period " + str(geographical_area_description_period_sid))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO geographical_area_description_periods_oplog (geographical_area_description_period_sid,
			geographical_area_sid, validity_start_date, geographical_area_id, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s)""", 
			(geographical_area_description_period_sid,
			geographical_area_sid, validity_start_date, geographical_area_id, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("geographical area description period", operation, geographical_area_description_period_sid, geographical_area_id, transaction_id, message_id)
		cur.close()
