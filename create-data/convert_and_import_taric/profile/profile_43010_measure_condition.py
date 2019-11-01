import psycopg2
import common.globals as g

class profile_43010_measure_condition(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date                              = app.getTimestamp()
		measure_condition_sid			            = app.get_number_value(oMessage, ".//oub:measure.condition.sid", True)
		measure_sid			                        = app.get_number_value(oMessage, ".//oub:measure.sid", True)
		condition_code				                = app.get_value(oMessage, ".//oub:condition.code", True)
		component_sequence_number				    = app.get_value(oMessage, ".//oub:component.sequence.number", True)
		condition_duty_amount					    = app.get_value(oMessage, ".//oub:condition.duty.amount", True)
		condition_monetary_unit_code			    = app.get_value(oMessage, ".//oub:condition.monetary.unit.code", True)
		condition_measurement_unit_code			    = app.get_value(oMessage, ".//oub:condition.measurement.unit.code", True)
		condition_measurement_unit_qualifier_code	= app.get_value(oMessage, ".//oub:condition.measurement.unit.qualifier.code", True)
		action_code	                                = app.get_value(oMessage, ".//oub:action.code", True)
		certificate_type_code	                    = app.get_value(oMessage, ".//oub:certificate.type.code", True)
		certificate_code                        	= app.get_value(oMessage, ".//oub:certificate.code", True)

		if update_type == "1":	# Update
			operation = "U"
			app.doprint ("Updating measure condition " + str(measure_condition_sid))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting measure condition " + str(measure_condition_sid))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating measure condition " + str(measure_condition_sid))


		# A measure must exist for a measure condition to be inserted or updated (not deleted, as this may have happened in the same transaction)
		update_string = g.app.get_update_string(operation)
		if update_type in ("1", "3"):
			sql = "select count(measure_sid) from measures where measure_sid = %s"
			params = [
				str(measure_sid)
			]
			cur = g.app.conn.cursor()
			cur.execute(sql, params)
			row = cur.fetchone()
			if row[0] == 1:
				g.app.add_load_error("DBFK: attempt to " + update_string + " a measure condition for a measure_sid which does not exist (measure_sid = " + str(measure_sid) + ", measure_condition_sid = " + str(measure_condition_sid) + ")")


		# You must not be able to insert a new measure condition if that measure condition already exists
		if update_type == "3":
			sql = "select count(measure_sid) from measure_conditions where measure_condition_sid = %s"
			params = [
				str(measure_condition_sid)
			]
			cur = g.app.conn.cursor()
			cur.execute(sql, params)
			row = cur.fetchone()
			if row[0] == 1:
				g.app.add_load_error("DBFK: attempt to insert a measure condition which already exists (measure_condition_sid = " + str(measure_condition_sid))


		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO measure_conditions_oplog (measure_condition_sid, measure_sid, condition_code,
			component_sequence_number, condition_duty_amount, condition_monetary_unit_code,
			condition_measurement_unit_code, condition_measurement_unit_qualifier_code, action_code,
			certificate_type_code, certificate_code, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", 
			(measure_condition_sid, measure_sid, condition_code,
			component_sequence_number, condition_duty_amount, condition_monetary_unit_code,
			condition_measurement_unit_code, condition_measurement_unit_qualifier_code, action_code,
			certificate_type_code, certificate_code, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("measure condition", operation, measure_condition_sid, None, transaction_id, message_id)
		cur.close()
