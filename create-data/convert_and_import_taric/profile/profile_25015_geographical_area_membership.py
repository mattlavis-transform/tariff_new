import psycopg2
import common.globals as g

class profile_25015_geographical_area_membership(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date      = app.getTimestamp()
		geographical_area_sid		= app.getNumberValue(oMessage, ".//oub:geographical.area.sid", True)
		geographical_area_group_sid	= app.getNumberValue(oMessage, ".//oub:geographical.area.group.sid", True)
		validity_start_date			= app.getDateValue(oMessage, ".//oub:validity.start.date", True)
		validity_end_date			= app.getDateValue(oMessage, ".//oub:validity.end.date", True)

		geographical_area_groups = g.app.get_geographical_area_groups()
		countries_regions = g.app.get_countries_regions()

		if g.app.perform_taric_validation == True:
			if validity_end_date != None:
				if validity_end_date < validity_start_date:
					g.app.add_load_error("GA15 - The start date of the membership must be less than or equal to the end date, when loading geographical area " + str(geographical_area_sid))


			if geographical_area_group_sid != None:
				if geographical_area_group_sid not in geographical_area_groups:
					g.app.add_load_error("GA14 - The referenced geographical area group id must exist, when modifying " \
					"membership for " + str(geographical_area_sid) + " in group " + str(geographical_area_group_sid))


			if geographical_area_sid not in countries_regions:
				g.app.add_load_error("GA12 / GA13 - The referenced geographical area id (member) must exist. The referenced " \
				"geographical area id (member) can only be linked to a country or region (area code = 0 or 2) " \
				", when modifying membership for " + str(geographical_area_sid)	)
	

		if update_type == "1":	# Update
			operation = "U"
			app.doprint ("Updating geographical area membership " + str(geographical_area_sid))

		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting geographical area membership " + str(geographical_area_sid))

		else:					# INSERT
			operation = "C"
			app.doprint ("Creating geographical area membership " + str(geographical_area_sid))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO geographical_area_memberships_oplog (geographical_area_sid,
			geographical_area_group_sid, validity_start_date, validity_end_date, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s)""", 
			(geographical_area_sid,
			geographical_area_group_sid, validity_start_date, validity_end_date, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("geographical area membership", operation, geographical_area_sid, None, transaction_id, message_id)
		cur.close()
