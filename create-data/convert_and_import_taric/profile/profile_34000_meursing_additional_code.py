import common.globals as g

class profile_34000_meursing_additional_code(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date               	= app.getTimestamp()
		meursing_additional_code_sid	= app.get_number_value(oMessage, ".//oub:meursing.additional.code.sid", True)
		validity_end_date				= app.get_date_value(oMessage, ".//oub:validity.end.date", True)
		additional_code					= app.get_value(oMessage, ".//oub:additional.code", True)
		validity_start_date				= app.get_date_value(oMessage, ".//oub:validity.start.date", True)

		if update_type == "1":		# UPDATE
			operation = "U"
			app.doprint ("Updating Meursing additional code " + str(additional_code))

		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting Meursing additional code " + str(additional_code))

		else:					# INSERT
			operation = "C"
			app.doprint ("Creating Meursing additional code " + str(additional_code))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO meursing_additional_codes_oplog (meursing_additional_code_sid,
			validity_end_date, additional_code, validity_start_date, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s)""", 
			(meursing_additional_code_sid,
			validity_end_date, additional_code, validity_start_date, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("meursing additional code", operation, additional_code, transaction_id, message_id)
		cur.close()
