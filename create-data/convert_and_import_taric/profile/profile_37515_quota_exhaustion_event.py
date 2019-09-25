import psycopg2
import common.globals as g

class profile_37515_quota_exhaustion_event(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date			= app.getTimestamp()
		quota_definition_sid	= app.getNumberValue(oMessage, ".//oub:quota.definition.sid", True)
		occurrence_timestamp	= app.getValue(oMessage, ".//oub:occurrence.timestamp", True)
		exhaustion_date			= app.getDateValue(oMessage, ".//oub:exhaustion.date", True)


		if update_type == "1":	# Update
			operation = "U"
			app.doprint ("Updating quota exhaustion event for quota definition " + str(quota_definition_sid))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting quota exhaustion event for quota definition " + str(quota_definition_sid))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating quota exhaustion event for quota definition " + str(quota_definition_sid))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO quota_exhaustion_events_oplog (
			quota_definition_sid, occurrence_timestamp, exhaustion_date, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s)""", 
			(quota_definition_sid, occurrence_timestamp, exhaustion_date, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("quota exhaustion event", operation, quota_definition_sid, None, transaction_id, message_id)
		cur.close()
