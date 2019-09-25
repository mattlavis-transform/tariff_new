import psycopg2
import common.globals as g

class profile_37530_quota_closed_and_balance_transferred_event(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date				= app.getTimestamp()
		quota_definition_sid		= app.getNumberValue(oMessage, ".//oub:quota.definition.sid", True)
		occurrence_timestamp		= app.getValue(oMessage, ".//oub:occurrence.timestamp", True)
		closing_date				= app.getDateValue(oMessage, ".//oub:closing.date", True)
		transferred_amount			= app.getNumberValue(oMessage, ".//oub:transferred.amount", True)
		target_quota_definition_sid	= app.getNumberValue(oMessage, ".//oub:target.quota.definition.sid", True)


		if update_type == "1":	# Update
			operation = "U"
			app.doprint ("Updating quota closed and balance transferred event for quota definition " + str(quota_definition_sid))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting quota closed and balance transferred event for quota definition " + str(quota_definition_sid))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating quota closed and balance transferred event for quota definition " + str(quota_definition_sid))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO quota_unblocking_events_oplog (
			quota_definition_sid, occurrence_timestamp, closing_date, transferred_amount, target_quota_definition_sid, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s, %s)""", 
			(quota_definition_sid, occurrence_timestamp, closing_date, transferred_amount, target_quota_definition_sid, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("quota closed and balance transferred event", operation, quota_definition_sid, None, transaction_id, message_id)
		cur.close()
