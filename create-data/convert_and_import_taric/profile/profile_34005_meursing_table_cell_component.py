import common.globals as g

class profile_34005_meursing_table_cell_component(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date               	= app.getTimestamp()
		meursing_additional_code_sid	= app.get_number_value(oMessage, ".//oub:meursing.additional.code.sid", True)
		meursing_table_plan_id			= app.get_number_value(oMessage, ".//oub:meursing.table.plan.id", True)
		heading_number					= app.get_value(oMessage, ".//oub:heading.number", True)
		row_column_code     			= app.get_value(oMessage, ".//oub:row.column.code", True)
		subheading_sequence_number      = app.get_number_value(oMessage, ".//oub:subheading.sequence.number", True)
		validity_start_date				= app.get_date_value(oMessage, ".//oub:validity.start.date", True)
		validity_end_date				= app.get_date_value(oMessage, ".//oub:validity.end.date", True)
		additional_code					= app.get_value(oMessage, ".//oub:additional.code", True)

		if update_type == "1":		# UPDATE
			operation = "U"
			app.doprint ("Updating Meursing table cell component for " + str(additional_code))

		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting Meursing table cell component for " + str(additional_code))

		else:					# INSERT
			operation = "C"
			app.doprint ("Creating Meursing table cell component for " + str(additional_code))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO meursing_table_cell_components_oplog (meursing_additional_code_sid,
			meursing_table_plan_id, heading_number, row_column_code, subheading_sequence_number,
			validity_start_date, validity_end_date, additional_code, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", 
			(meursing_additional_code_sid,
			meursing_table_plan_id, heading_number, row_column_code, subheading_sequence_number,
			validity_start_date, validity_end_date, additional_code, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("meursing table cell component", operation, additional_code, transaction_id, message_id)
		cur.close()
