import common.globals as g

class profile_32505_meursing_heading_text(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date                      = app.getTimestamp()
		meursing_table_plan_id				= app.get_number_value(oMessage, ".//oub:meursing.table.plan.id", True)
		meursing_heading_number				= app.get_value(oMessage, ".//oub:meursing.heading.number", True)
		row_column_code     				= app.get_value(oMessage, ".//oub:row.column.code", True)
		language_id     				    = app.get_value(oMessage, ".//oub:language.id", True)
		description     				    = app.get_value(oMessage, ".//oub:description", True)

		if update_type == "1":		# UPDATE
			operation = "U"
			app.doprint ("Updating Meursing heading text for Meursing heading number " + str(meursing_heading_number))

		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting Meursing heading text for Meursing heading number " + str(meursing_heading_number))

		else:					# INSERT
			operation = "C"
			app.doprint ("Creating Meursing heading text for Meursing heading number " + str(meursing_heading_number))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO meursing_heading_texts_oplog (meursing_table_plan_id, meursing_heading_number,
			row_column_code, language_id, description, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s, %s)""", 
			(meursing_table_plan_id, meursing_heading_number,
            row_column_code, language_id, description, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("meursing table heading text", operation, meursing_table_plan_id, transaction_id, message_id)
		cur.close()
