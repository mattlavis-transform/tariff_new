import psycopg2
import common.globals as g

class profile_37010_quota_blocking_period(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date				= app.getTimestamp()
		quota_blocking_period_sid   = app.get_number_value(oMessage, ".//oub:quota.blocking.period.sid", True)
		quota_definition_sid        = app.get_number_value(oMessage, ".//oub:quota.definition.sid", True)
		blocking_start_date			= app.get_date_value(oMessage, ".//oub:blocking.start.date", True)
		blocking_end_date			= app.get_date_value(oMessage, ".//oub:blocking.end.date", True)
		blocking_period_type		= app.get_value(oMessage, ".//oub:blocking.period.type", True)
		description					= app.get_value(oMessage, ".//oub:description", True)

		if update_type == "1":	# Update
			operation = "U"
			app.doprint ("Updating quota blocking period " + str(quota_blocking_period_sid))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting quota blocking period " + str(quota_blocking_period_sid))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating quota blocking period " + str(quota_blocking_period_sid))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO quota_blocking_periods_oplog (quota_blocking_period_sid,
			quota_definition_sid, blocking_start_date, blocking_end_date, blocking_period_type,
			description, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", 
			(quota_blocking_period_sid,
			quota_definition_sid, blocking_start_date, blocking_end_date, blocking_period_type,
			description, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("quota blocking period", operation, quota_blocking_period_sid, None, transaction_id, message_id)
		cur.close()
