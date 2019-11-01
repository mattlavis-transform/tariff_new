import psycopg2
import common.globals as g

class profile_43011_measure_condition_component(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date                  = app.getTimestamp()
		measure_condition_sid			= app.get_number_value(oMessage, ".//oub:measure.condition.sid", True)
		duty_expression_id				= app.get_value(oMessage, ".//oub:duty.expression.id", True)
		duty_amount					    = app.get_value(oMessage, ".//oub:duty.amount", True)
		monetary_unit_code			    = app.get_value(oMessage, ".//oub:monetary.unit.code", True)
		measurement_unit_code			= app.get_value(oMessage, ".//oub:measurement.unit.code", True)
		measurement_unit_qualifier_code	= app.get_value(oMessage, ".//oub:measurement.unit.qualifier.code", True)

		if update_type in ("1", "3"):	# Update or insert
			sql = "select measure_condition_sid from measure_conditions where measure_condition_sid = %s "
			params = [
				str(measure_condition_sid)
			]
			cur = g.app.conn.cursor()
			cur.execute(sql, params)
			rows = cur.fetchall()
			if len(rows) == 0:
				g.app.add_load_error("DBFK: measure condition component tries to use SID " + str(measure_condition_sid))

		if update_type == "1":	# Update
			operation = "U"
			app.doprint ("Updating measure condition component " + str(measure_condition_sid))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting measure condition component " + str(measure_condition_sid))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating measure condition component " + str(measure_condition_sid))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO measure_condition_components_oplog (measure_condition_sid, duty_expression_id, duty_amount,
			monetary_unit_code, measurement_unit_code,
			measurement_unit_qualifier_code, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", 
			(measure_condition_sid, duty_expression_id, duty_amount,
			monetary_unit_code, measurement_unit_code,
			measurement_unit_qualifier_code, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("measure condition component", operation, measure_condition_sid, None, transaction_id, message_id)
		cur.close()
