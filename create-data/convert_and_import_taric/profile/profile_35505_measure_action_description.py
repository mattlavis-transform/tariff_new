import psycopg2
import common.globals as g

class profile_35505_measure_action_description(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date  = app.getTimestamp()
		action_code  	= app.getValue(oMessage, ".//oub:action.code", True)
		language_id		= app.getValue(oMessage, ".//oub:language.id", True)
		description		= app.getValue(oMessage, ".//oub:description", True)

		if update_type == "1":	    # UPDATE
			operation = "U"
			app.doprint ("Updating measure action description " + str(action_code))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting measure action description " + str(action_code))
		else:					    # INSERT
			operation = "C"
			app.doprint ("Creating measure action description " + str(action_code))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO measure_action_descriptions_oplog (action_code, language_id, description, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s)""", 
			(action_code, language_id, description, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("measure action description", operation, None, action_code, transaction_id, message_id)
		cur.close()
