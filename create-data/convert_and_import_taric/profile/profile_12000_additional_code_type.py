import psycopg2
import common.globals as g

class profile_12000_additional_code_type(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date                      = app.getTimestamp()
		additional_code_type_id				= app.get_value(oMessage, ".//oub:additional.code.type.id", True)
		validity_start_date					= app.get_date_value(oMessage, ".//oub:validity.start.date", True)
		validity_end_date					= app.get_date_value(oMessage, ".//oub:validity.end.date", True)
		application_code					= app.get_value(oMessage, ".//oub:application.code", True)
		meursing_table_plan_id				= app.get_value(oMessage, ".//oub:meursing.table.plan.id", True)

		additional_code_types = g.app.get_additional_code_types()


		if update_type == "1":		# UPDATE
			if g.app.perform_taric_validation == True:
				if additional_code_type_id not in additional_code_types:
					g.app.add_load_error("CTx - The additional code type must exist for updates. Aborting while trying to update additional code type  " + additional_code_type_id + ".")
			operation = "U"
			app.doprint ("Updating additional code type " + str(additional_code_type_id))

		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting additional code type " + str(additional_code_type_id))

		else:						# INSERT
			if g.app.perform_taric_validation == True:
				if additional_code_type_id in additional_code_types:
					g.app.add_load_error("CT1 - The additional code type must be unique. Aborting while trying to insert additional code type " + additional_code_type_id + ".")
			operation = "C"
			app.doprint ("Creating additional code type " + str(additional_code_type_id))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO additional_code_types_oplog (additional_code_type_id, validity_start_date,
			validity_end_date, application_code, meursing_table_plan_id, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s, %s)""", 
			(additional_code_type_id, validity_start_date,
			validity_end_date, application_code, meursing_table_plan_id, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("additional_code_type", operation, None, additional_code_type_id, transaction_id, message_id)
		cur.close()
