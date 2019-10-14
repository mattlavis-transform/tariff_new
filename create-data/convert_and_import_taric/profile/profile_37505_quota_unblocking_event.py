import psycopg2
import common.globals as g

class profile_37505_quota_unblocking_event(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date			= app.getTimestamp()
		quota_definition_sid	= app.get_number_value(oMessage, ".//oub:quota.definition.sid", True)
		occurrence_timestamp	= app.get_value(oMessage, ".//oub:occurrence.timestamp", True)
		unblocking_date			= app.get_date_value(oMessage, ".//oub:unblocking.date", True)


		if update_type == "1":	# Update
			operation = "U"
			app.doprint ("Updating quota unblocking event for quota definition " + str(quota_definition_sid))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting quota unblocking event for quota definition " + str(quota_definition_sid))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating quota unblocking event for quota definition " + str(quota_definition_sid))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO quota_unblocking_events_oplog (
			quota_definition_sid, occurrence_timestamp, unblocking_date, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s)""", 
			(quota_definition_sid, occurrence_timestamp, unblocking_date, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("quota unblocking event", operation, quota_definition_sid, None, transaction_id, message_id)
		cur.close()
