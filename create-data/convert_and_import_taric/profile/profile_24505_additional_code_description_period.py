import psycopg2
import common.globals as g

class profile_24505_additional_code_description_period(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date				            = app.getTimestamp()
		additional_code_description_period_sid  = app.getNumberValue(oMessage, ".//oub:additional.code.description.period.sid", True)
		additional_code_sid                     = app.getNumberValue(oMessage, ".//oub:additional.code.sid", True)
		additional_code_type_id			        = app.getValue(oMessage, ".//oub:additional.code.type.id", True)
		additional_code 					    = app.getValue(oMessage, ".//oub:additional.code", True)
		validity_start_date			            = app.getDateValue(oMessage, ".//oub:validity.start.date", True)

		if update_type == "1":	# UPDATE
			operation = "U"
			app.doprint ("Updating additional code description period " + str(additional_code_sid))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting additional code description period " + str(additional_code_sid))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating additional code description period " + str(additional_code_sid))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO additional_code_description_periods_oplog (additional_code_description_period_sid,
			additional_code_sid, additional_code_type_id, additional_code, validity_start_date, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s, %s)""", 
			(additional_code_description_period_sid, additional_code_sid, additional_code_type_id, additional_code, validity_start_date, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("additional code description period", operation, additional_code_description_period_sid, additional_code_type_id + "|" + additional_code, transaction_id, message_id)
		cur.close()
