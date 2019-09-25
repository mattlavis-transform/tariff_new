import psycopg2
import common.globals as g

class profile_35005_measure_condition_code_description(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date  = app.getTimestamp()
		condition_code  = app.getValue(oMessage, ".//oub:condition.code", True)
		language_id		= app.getValue(oMessage, ".//oub:language.id", True)
		description		= app.getValue(oMessage, ".//oub:description", True)

		if update_type == "1":	    # UPDATE
			operation = "U"
			app.doprint ("Updating measure condition description " + str(condition_code))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting measure condition description " + str(condition_code))
		else:					    # INSERT
			operation = "C"
			app.doprint ("Creating measure condition description " + str(condition_code))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO measure_condition_code_descriptions_oplog (condition_code, language_id, description, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s)""", 
			(condition_code, language_id, description, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("measure condition description", operation, "EN", condition_code, transaction_id, message_id)
		cur.close()
