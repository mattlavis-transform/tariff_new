import psycopg2
import common.globals as g

class profile_29505_prorogation_regulation_action(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date						= app.getTimestamp()
		prorogation_regulation_role		= app.getValue(oMessage, ".//oub:prorogation.regulation.role", True)
		prorogation_regulation_id		= app.getValue(oMessage, ".//oub:prorogation.regulation.id", True)
		prorogated_regulation_role		= app.getValue(oMessage, ".//oub:prorogated.regulation.role", True)
		prorogated_regulation_id		= app.getValue(oMessage, ".//oub:prorogated.regulation.id", True)
		prorogated_date					= app.getDateValue(oMessage, ".//oub:prorogated.date", True)

		if update_type == "1":		# UPDATE
			operation = "U"
			app.doprint ("Updating prorogation regulation action " + str(prorogation_regulation_id))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting prorogation regulation action " + str(prorogation_regulation_id))
		else:						# INSERT
			operation = "C"
			app.doprint ("Creating prorogation regulation action " + str(prorogation_regulation_id))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO prorogation_regulation_actions_oplog (prorogation_regulation_role,
			prorogation_regulation_id, prorogated_regulation_role, prorogated_regulation_id, prorogated_date,
			operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s, %s)""", 
			(prorogation_regulation_role,
			prorogation_regulation_id, prorogated_regulation_role, prorogated_regulation_id, prorogated_date,
			operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("prorogation regulation action", operation, None, prorogation_regulation_id, transaction_id, message_id)
		cur.close()
