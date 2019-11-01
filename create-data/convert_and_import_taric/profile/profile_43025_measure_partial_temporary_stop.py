import psycopg2
import common.globals as g

class profile_43025_measure_partial_temporary_stop(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date                  = app.getTimestamp()
		measure_sid						                            = app.get_number_value(oMessage, ".//oub:measure.sid", True)
		validity_start_date			                                = app.get_date_value(oMessage, ".//oub:validity.start.date", True)
		validity_end_date			                                = app.get_date_value(oMessage, ".//oub:validity.end.date", True)
		partial_temporary_stop_regulation_id				        = app.get_value(oMessage, ".//oub:partial.temporary.stop.regulation.id", True)
		partial_temporary_stop_regulation_officialjournal_number    = app.get_value(oMessage, ".//oub:partial.temporary.stop.regulation.officialjournal.number", True)
		partial_temporary_stop_regulation_officialjournal_page		= app.get_value(oMessage, ".//oub:partial.temporary.stop.regulation.officialjournal.page", True)
		abrogation_regulation_id			                        = app.get_value(oMessage, ".//oub:abrogation.regulation.id", True)
		abrogation_regulation_officialjournal_number	            = app.get_value(oMessage, ".//oub:abrogation.regulation.officialjournal.number", True)
		abrogation_regulation_officialjournal_page                  = app.get_value(oMessage, ".//oub:abrogation.regulation.officialjournal.page", True)

		if update_type == "1":	# Update
			operation = "U"
			app.doprint ("Updating measure partial temporary stop " + str(measure_sid))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting measure partial temporary stop " + str(measure_sid))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating measure partial temporary stop " + str(measure_sid))

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
			g.app.add_load_error("DBFK: measure partial temporary stop - please revert database.  No measure with SID " + str(measure_sid))



		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO measure_partial_temporary_stops_oplog (measure_sid, validity_start_date, validity_end_date,
			partial_temporary_stop_regulation_id, partial_temporary_stop_regulation_officialjournal_number,
			partial_temporary_stop_regulation_officialjournal_page, abrogation_regulation_id,
			abrogation_regulation_officialjournal_number, abrogation_regulation_officialjournal_page,
			operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", 
			(measure_sid, validity_start_date, validity_end_date,
			partial_temporary_stop_regulation_id, partial_temporary_stop_regulation_officialjournal_number,
			partial_temporary_stop_regulation_officialjournal_page, abrogation_regulation_id,
			abrogation_regulation_officialjournal_number, abrogation_regulation_officialjournal_page,
			operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("measure partial temporary stop", operation_date, measure_sid, None, transaction_id, message_id)
		cur.close()
