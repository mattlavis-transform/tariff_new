import psycopg2
import common.globals as g

class profile_25000_geographical_area(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date                      = app.getTimestamp()
		geographical_area_sid				= app.get_number_value(oMessage, ".//oub:geographical.area.sid", True)
		geographical_area_id				= app.get_value(oMessage, ".//oub:geographical.area.id", True)
		validity_start_date					= app.get_date_value(oMessage, ".//oub:validity.start.date", True)
		validity_end_date					= app.get_date_value(oMessage, ".//oub:validity.end.date", True)
		geographical_code					= app.get_value(oMessage, ".//oub:geographical.code", True)
		parent_geographical_area_group_sid	= app.get_number_value(oMessage, ".//oub:parent.geographical.area.group.sid", True)

		geographical_area_groups = g.app.get_geographical_area_groups()

		if g.app.perform_taric_validation == True:
			if validity_end_date != None:
				if validity_end_date < validity_start_date:
					g.app.add_load_error("GA2 - The start date of the geographical area must be less than or equal to the end date, when loading geographical area " + geographical_area_id)

			if parent_geographical_area_group_sid != None:
				if parent_geographical_area_group_sid not in geographical_area_groups:
					g.app.add_load_error("GA4 - The referenced parent geographical area group must be an existing geographical " \
					"area with area code = 1 (geographical area group), when loading geographical area " + geographical_area_id)

		
		if update_type == "1":		# UPDATE
			operation = "U"
			app.doprint ("Updating geographical_area " + str(geographical_area_sid))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting geographical_area " + str(geographical_area_sid))
		else:						# INSERT
			operation = "C"
			app.doprint ("Creating geographical_area " + str(geographical_area_sid))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO geographical_areas_oplog (geographical_area_sid, geographical_area_id,
			validity_start_date, validity_end_date, geographical_code,
			parent_geographical_area_group_sid, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", 
			(geographical_area_sid, geographical_area_id,
			validity_start_date, validity_end_date, geographical_code,
			parent_geographical_area_group_sid, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("geographical area", operation, geographical_area_sid, geographical_area_id, transaction_id, message_id)
		cur.close()

		if g.app.perform_taric_validation == True:
			g.app.geographical_area_sids = g.app.get_all_geographical_area_sids()
			g.app.geographical_areas = g.app.get_all_geographical_areas()
		
