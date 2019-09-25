import psycopg2
import common.globals as g

class profile_37015_quota_suspension_period(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date				= app.getTimestamp()
		quota_suspension_period_sid = app.getNumberValue(oMessage, ".//oub:quota.suspension.period.sid", True)
		quota_definition_sid        = app.getNumberValue(oMessage, ".//oub:quota.definition.sid", True)
		suspension_start_date		= app.getDateValue(oMessage, ".//oub:suspension.start.date", True)
		suspension_end_date			= app.getDateValue(oMessage, ".//oub:suspension.end.date", True)
		description					= app.getValue(oMessage, ".//oub:description", True)

		if update_type == "20":
			app.doprint ("Deleting quota suspension period " + str(quota_suspension_period_sid))
			cur = app.conn.cursor()
			try:
				cur.execute("DELETE FROM quota_suspension_periods_oplog WHERE quota_suspension_period_sid = %s", (quota_suspension_period_sid,))
				app.conn.commit()
			except:
				g.app.log_error("quota suspension period", "D", quota_suspension_period_sid, None, transaction_id, message_id)
			cur.close()

		else:
			if update_type == "1":	# Update
				operation = "U"
				app.doprint ("Updating quota suspension period " + str(quota_suspension_period_sid))
			elif update_type == "2":	# DELETE
				operation = "D"
				app.doprint ("Deleting quota suspension period " + str(quota_suspension_period_sid))
			else:					# INSERT
				operation = "C"
				app.doprint ("Creating quota suspension period " + str(quota_suspension_period_sid))

			cur = app.conn.cursor()
			try:
				cur.execute("""INSERT INTO quota_suspension_periods_oplog (quota_suspension_period_sid,
				quota_definition_sid, suspension_start_date, suspension_end_date,
				description, operation, operation_date)
				VALUES (%s, %s, %s, %s, %s, %s, %s)""", 
				(quota_suspension_period_sid,
				quota_definition_sid, suspension_start_date, suspension_end_date,
				description, operation, operation_date))
				app.conn.commit()
			except:
				g.app.log_error("quota suspension period", operation, quota_suspension_period_sid, None, transaction_id, message_id)
			cur.close()
