import psycopg2
import common.globals as g

class profile_43005_measure_component(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date                  = app.getTimestamp()
		measure_sid						= app.get_number_value(oMessage, ".//oub:measure.sid", True)
		duty_expression_id				= app.get_value(oMessage, ".//oub:duty.expression.id", True)
		duty_amount					    = app.get_value(oMessage, ".//oub:duty.amount", True)
		monetary_unit_code			    = app.get_value(oMessage, ".//oub:monetary.unit.code", True)
		measurement_unit_code			= app.get_value(oMessage, ".//oub:measurement.unit.code", True)
		measurement_unit_qualifier_code	= app.get_value(oMessage, ".//oub:measurement.unit.qualifier.code", True)

		if g.app.perform_taric_validation == True:
			# Check for ME41 error
			if duty_expression_id not in g.app.duty_expressions:
				g.app.add_load_error("ME41 - The referenced duty expression must exist, " \
				"when loading measure with SID " + str(measure_sid))

			# Check for ME45, ME46 and ME47 errors
			if duty_expression_id in g.app.duty_expressions:
				for item in g.app.all_duty_expressions:
					duty_expression_id2					= item[0]
					validity_start_date 				= item[1]
					validity_end_date					= item[2]
					duty_amount_applicability_code		= item[3]
					measurement_unit_applicability_code	= item[4]
					monetary_unit_applicability_code	= item[5]

					if duty_expression_id == duty_expression_id2:
						# ME45
						if duty_amount_applicability_code == 1: # Mandatory
							if duty_amount == None:
								g.app.add_load_error("ME45(a) - If the flag 'amount' on duty expression is 'mandatory' then an amount must " \
								"be specified. If the flag is set 'not permitted' then no amount may be entered, " \
								"when loading measure component for measure with SID " + str(measure_sid))
						elif duty_amount_applicability_code == 2: # Not permitted
							if duty_amount != None:
								g.app.add_load_error("ME45(b) - If the flag 'amount' on duty expression is 'mandatory' then an amount must " \
								"be specified. If the flag is set 'not permitted' then no amount may be entered, " \
								"when loading measure component for measure with SID " + str(measure_sid))

						# ME46
						if monetary_unit_applicability_code == 1: # Mandatory
							if monetary_unit_code == None:
								g.app.add_load_error("ME46(a) - If the flag 'monetary unit' on duty expression is 'mandatory' then a monetary unit " \
								"must be specified. If the flag is set 'not permitted' then no monetary unit may be entered. , " \
								"when loading measure component for measure with SID " + str(measure_sid))
						elif monetary_unit_applicability_code == 2: # Not permitted
							"""
							if monetary_unit_code == None:
								g.app.add_load_error("ME46(b) - If the flag 'monetary unit' on duty expression is 'mandatory' then a monetary unit " \
								"must be specified. If the flag is set 'not permitted' then no monetary unit may be entered. , " \
								"when loading measure component for measure with SID " + str(measure_sid))
							"""

						# ME47
						"""
						if measurement_unit_applicability_code == 1: # Mandatory
							if measurement_unit_code == None:
								g.app.add_load_error("ME47(a) - If the flag 'measurement unit' on duty expression is 'mandatory' then a " \
								"measurement unit must be specified. If the flag is set 'not permitted' then no measurement unit may be entered. , " \
								"when loading measure component for measure with SID " + str(measure_sid))
						elif measurement_unit_applicability_code == 2: # Not permitted
							if measurement_unit_code == None:
								g.app.add_load_error("ME47(b) - If the flag 'measurement unit' on duty expression is 'mandatory' then a " \
								"measurement unit must be specified. If the flag is set 'not permitted' then no measurement unit may be entered. , " \
								"when loading measure component for measure with SID " + str(measure_sid))
						"""
						break


		if update_type == "1":	# Update
			operation = "U"
			app.doprint ("Updating measure component on measure_sid " + str(measure_sid))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting measure component on measure_sid " + str(measure_sid))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating measure component on measure_sid " + str(measure_sid))

		if update_type in ("1", "3"):
			sql = "select measure_sid from measures where measure_sid = " + str(measure_sid) + " limit 1"
			cur = g.app.conn.cursor()
			cur.execute(sql)
			rows = cur.fetchall()
			try:
				row = rows[0]
				measure_exists = True
			except:
				measure_exists = False

			if measure_exists == False:
				g.app.add_load_error("DBFK: measure component - please revert database.  No measure with SID " + str(measure_sid))



		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO measure_components_oplog (measure_sid, duty_expression_id, duty_amount,
			monetary_unit_code, measurement_unit_code,
			measurement_unit_qualifier_code, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", 
			(measure_sid, duty_expression_id, duty_amount,
			monetary_unit_code, measurement_unit_code,
			measurement_unit_qualifier_code, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("measure component", operation, measure_sid, None, transaction_id, message_id)
		cur.close()
