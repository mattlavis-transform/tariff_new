import psycopg2
import common.globals as g

class profile_30005_fts_regulation_action(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date			= app.getTimestamp()
		fts_regulation_role     = app.getValue(oMessage, ".//oub:fts.regulation.role", True)
		fts_regulation_id	    = app.getValue(oMessage, ".//oub:fts.regulation.id", True)
		stopped_regulation_role	= app.getValue(oMessage, ".//oub:stopped.regulation.role", True)
		stopped_regulation_id	= app.getValue(oMessage, ".//oub:stopped.regulation.id", True)

		if update_type == "1":	# UPDATE    
			operation = "U"
			app.doprint ("Updating FTS regulation action " + str(fts_regulation_id))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting FTS regulation action " + str(fts_regulation_id))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating FTS regulation action " + str(fts_regulation_id))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO fts_regulation_actions_oplog (fts_regulation_role, fts_regulation_id,
			stopped_regulation_role, stopped_regulation_id,
			operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s)""", 
			(fts_regulation_role, fts_regulation_id, stopped_regulation_role, stopped_regulation_id,
			operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("full temporary stop regulation", operation, None, full_temporary_stop_regulation_id, transaction_id, message_id)
		cur.close()
