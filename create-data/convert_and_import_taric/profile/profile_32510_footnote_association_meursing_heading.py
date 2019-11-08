import common.globals as g

class profile_32510_footnote_association_meursing_heading(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date                      = app.getTimestamp()
		meursing_table_plan_id				= app.get_number_value(oMessage, ".//oub:meursing.table.plan.id", True)
		meursing_heading_number				= app.get_value(oMessage, ".//oub:meursing.heading.number", True)
		row_column_code     				= app.get_value(oMessage, ".//oub:row.column.code", True)
		footnote_type     				    = app.get_value(oMessage, ".//oub:footnote.type", True)
		footnote_id     				    = app.get_value(oMessage, ".//oub:footnote.id", True)
		validity_start_date					= app.get_date_value(oMessage, ".//oub:validity.start.date", True)
		validity_end_date					= app.get_date_value(oMessage, ".//oub:validity.end.date", True)

		if update_type == "1":		# UPDATE
			operation = "U"
			app.doprint ("Updating footnote association for Meursing heading text for Meursing heading number " + str(meursing_heading_number))

		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting footnote association for Meursing heading text for Meursing heading number " + str(meursing_heading_number))

		else:					# INSERT
			operation = "C"
			app.doprint ("Creating footnote association for Meursing heading text for Meursing heading number " + str(meursing_heading_number))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO footnote_association_meursing_headings_oplog (meursing_table_plan_id, meursing_heading_number,
			row_column_code, footnote_type, footnote_id, validity_start_date, validity_end_date, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""", 
			(meursing_table_plan_id, meursing_heading_number,
            row_column_code, footnote_type, footnote_id, validity_start_date, validity_end_date, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("footnote association for Meursing heading text", operation, meursing_table_plan_id, transaction_id, message_id)
		cur.close()

