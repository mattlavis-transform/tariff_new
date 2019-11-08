import psycopg2
import sys
import common.globals as g

class profile_43015_measure_excluded_geographical_area(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date              = app.getTimestamp()
		measure_sid					= app.get_number_value(oMessage, ".//oub:measure.sid", True)
		excluded_geographical_area	= app.get_value(oMessage, ".//oub:excluded.geographical.area", True)
		geographical_area_sid	    = app.get_number_value(oMessage, ".//oub:geographical.area.sid", True)


		if update_type == "1":	# Update
			operation = "U"
			app.doprint ("Updating measure excluded geographical area " + str(measure_sid))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting measure excluded geographical area " + str(measure_sid))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating measure excluded geographical area " + str(measure_sid))


		# Check the measure exists before approving
		if g.app.perform_taric_validation == True:
			if update_type in ("1", "3"):
				sql = "select measure_sid from measures where measure_sid = %s limit 1"
				params = [
					str(measure_sid)
				]
				cur = g.app.conn.cursor()
				cur.execute(sql, params)
				rows = cur.fetchall()
				try:
					row = rows[0]
					measure_exists = True
				except:
					measure_exists = False

				if measure_exists == False:
					g.app.add_load_error("DBFK: measure excluded geographical area - please revert database.  No measure with SID " + str(measure_sid))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO measure_excluded_geographical_areas_oplog (measure_sid,
			excluded_geographical_area, geographical_area_sid, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s)""", 
			(measure_sid, excluded_geographical_area, geographical_area_sid, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("measure excluded geographical area", operation, measure_sid, None, transaction_id, message_id)
		cur.close()
