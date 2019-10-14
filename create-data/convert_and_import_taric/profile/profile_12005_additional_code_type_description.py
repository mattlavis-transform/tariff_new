import psycopg2
import common.globals as g

class profile_12005_additional_code_type_description(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date                  = app.getTimestamp()
		additional_code_type_id			= app.get_value(oMessage, ".//oub:additional.code.type.id", True)
		language_id						= app.get_value(oMessage, ".//oub:language.id", True)
		description						= app.get_value(oMessage, ".//oub:description", True)

		additional_code_type_descriptions = g.app.get_additional_code_type_descriptions()

		if update_type == "1":	# UPDATE
			if g.app.perform_taric_validation == True:
				if additional_code_type_id not in additional_code_type_descriptions:
					g.app.add_load_error("CTx - The additional code type must exist for updates. Aborting while trying to update additional code type description " + additional_code_type_id + ".")
			operation = "U"
			app.doprint ("Updating additional code type description " + str(additional_code_type_id))

		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting additional code type description " + str(additional_code_type_id))

		else:					# INSERT
			operation = "C"
			app.doprint ("Creating additional code type description " + str(additional_code_type_id))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO additional_code_type_descriptions_oplog (additional_code_type_id, language_id,
			description, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s)""", 
			(additional_code_type_id, language_id,
			description, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("additional_code_type_description", operation, None, additional_code_type_id, transaction_id, message_id)
		cur.close()
