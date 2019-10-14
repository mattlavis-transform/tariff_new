import psycopg2
import sys
from datetime import datetime
import common.globals as g

class profile_23000_duty_expression(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date		    			= app.getTimestamp()
		duty_expression_id					= app.get_value(oMessage, ".//oub:duty.expression.id", True)
		validity_start_date	    			= app.get_date_value(oMessage, ".//oub:validity.start.date", True)
		validity_end_date	    			= app.get_date_value(oMessage, ".//oub:validity.end.date", True)
		duty_amount_applicability_code	    = app.get_number_value(oMessage, ".//oub:duty.amount.applicability.code", True)
		measurement_unit_applicability_code	= app.get_number_value(oMessage, ".//oub:measurement.unit.applicability.code", True)
		monetary_unit_applicability_code	= app.get_number_value(oMessage, ".//oub:monetary.unit.applicability.code", True)

		if update_type == "1":	# UPDATE
			operation = "U"
			app.doprint ("Updating duty_expression " + str(duty_expression_id))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting duty_expression " + str(duty_expression_id))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating duty_expression " + str(duty_expression_id))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO duty_expressions_oplog (duty_expression_id, validity_start_date,
			validity_end_date, duty_amount_applicability_code, measurement_unit_applicability_code, monetary_unit_applicability_code,
			operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", 
			(duty_expression_id, validity_start_date, validity_end_date,
			duty_amount_applicability_code, measurement_unit_applicability_code, monetary_unit_applicability_code,
			operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("duty expression", operation, None, duty_expression_id, transaction_id, message_id)
		cur.close()
