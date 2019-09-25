import psycopg2
import common.globals as g

class profile_37500_quota_balance_event(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date			= app.getTimestamp()
		quota_definition_sid			= app.getNumberValue(oMessage, ".//oub:quota.definition.sid", True)
		occurrence_timestamp			= app.getValue(oMessage, ".//oub:occurrence.timestamp", True)
		old_balance						= app.getValue(oMessage, ".//oub:old.balance", True)
		new_balance						= app.getValue(oMessage, ".//oub:new.balance", True)
		imported_amount					= app.getNumberValue(oMessage, ".//oub:imported.amount", True)
		last_import_date_in_allocation	= app.getDateValue(oMessage, ".//oub:last.import.date.in.allocation", True)


		if quota_definition_sid not in g.app.quota_definitions:
			g.app.add_load_error("Quota definition does not exist in loading quota balance for definition " + str(quota_definition_sid))


		if update_type == "1":	# Update
			operation = "U"
			app.doprint ("Updating quota balance event for quota definition " + str(quota_definition_sid))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting quota balance event for quota definition " + str(quota_definition_sid))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating quota balance event for quota definition " + str(quota_definition_sid))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO quota_balance_events_oplog (
			quota_definition_sid, occurrence_timestamp, old_balance, new_balance, imported_amount, last_import_date_in_allocation, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", 
			(quota_definition_sid, occurrence_timestamp, old_balance, new_balance, imported_amount, last_import_date_in_allocation, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("quota balance event", operation, quota_definition_sid, None, transaction_id, message_id)
		cur.close()
