import psycopg2
import common.globals as g

class profile_24510_additional_code_description(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date                          = app.getTimestamp()
		additional_code_description_period_sid	= app.get_number_value(oMessage, ".//oub:additional.code.description.period.sid", True)
		additional_code_sid	                    = app.get_number_value(oMessage, ".//oub:additional.code.sid", True)
		additional_code_type_id		            = app.get_value(oMessage, ".//oub:additional.code.type.id", True)
		additional_code			    			= app.get_value(oMessage, ".//oub:additional.code", True)
		language_id								= app.get_value(oMessage, ".//oub:language.id", True)
		description								= app.get_value(oMessage, ".//oub:description", True)

		if update_type == "1":		# UPDATE
			operation = "U"
			app.doprint ("Updating additional code type description " + str(additional_code_description_period_sid))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting additional code type description " + str(additional_code_description_period_sid))
		else:						# INSERT
			operation = "C"
			app.doprint ("Creating additional code type description " + str(additional_code_description_period_sid))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO additional_code_descriptions_oplog (additional_code_description_period_sid,
			additional_code_sid, additional_code_type_id,
			additional_code, language_id, description, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", 
			(additional_code_description_period_sid, additional_code_sid, additional_code_type_id,
			additional_code, language_id, description, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("additional code description", operation, additional_code_description_period_sid, additional_code_type_id + "|" + additional_code, transaction_id, message_id)
		cur.close()
