import psycopg2
import common.globals as g

class profile_23005_duty_expression_description(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date      = app.getTimestamp()
		duty_expression_id	= app.getValue(oMessage, ".//oub:duty.expression.id", True)
		language_id			= app.getValue(oMessage, ".//oub:language.id", True)
		description			= app.getValue(oMessage, ".//oub:description", True)

		if update_type == "1":	    # UPDATE
			operation = "U"
			app.doprint ("Updating duty expression description " + str(duty_expression_id))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting duty expression description " + str(duty_expression_id))
		else:					    # INSERT
			operation = "C"
			app.doprint ("Creating duty expression description " + str(duty_expression_id))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO duty_expression_descriptions_oplog (duty_expression_id, language_id, description, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s)""", 
			(duty_expression_id, language_id, description, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("duty expression description", operation, None, duty_expression_id, transaction_id, message_id)
		cur.close()
