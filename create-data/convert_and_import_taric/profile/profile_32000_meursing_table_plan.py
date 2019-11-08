import common.globals as g

class profile_32000_meursing_table_plan(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date                      = app.getTimestamp()
		meursing_table_plan_id				= app.get_number_value(oMessage, ".//oub:meursing.table.plan.id", True)
		validity_start_date					= app.get_date_value(oMessage, ".//oub:validity.start.date", True)
		validity_end_date					= app.get_date_value(oMessage, ".//oub:validity.end.date", True)

		if update_type == "1":		# UPDATE
			operation = "U"
			app.doprint ("Updating Meursing table plan ID " + str(meursing_table_plan_id))

		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting Meursing table plan ID " + str(meursing_table_plan_id))

		else:					# INSERT
			operation = "C"
			app.doprint ("Creating Meursing table plan ID " + str(meursing_table_plan_id))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO meursing_table_plans_oplog (meursing_table_plan_id,
			validity_start_date, validity_end_date, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s)""", 
			(meursing_table_plan_id, validity_start_date, validity_end_date, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("meursing table plan", operation, meursing_table_plan_id, transaction_id, message_id)
		cur.close()
