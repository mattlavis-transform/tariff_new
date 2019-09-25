import psycopg2
import common.globals as g

class profile_30500_regulation_replacement(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date				= app.getTimestamp()
		replacing_regulation_role	= app.getValue(oMessage, ".//oub:replacing.regulation.role", True)
		replacing_regulation_id		= app.getValue(oMessage, ".//oub:replacing.regulation.id", True)
		replaced_regulation_role	= app.getValue(oMessage, ".//oub:replaced.regulation.role", True)
		replaced_regulation_id		= app.getValue(oMessage, ".//oub:replaced.regulation.id", True)
		measure_type_id				= app.getValue(oMessage, ".//oub:measure.type.id", True)
		geographical_area_id		= app.getValue(oMessage, ".//oub:geographical.area.id", True)
		chapter_heading				= app.getValue(oMessage, ".//oub:chapter.heading", True)

		if update_type == "1":	# Update
			operation = "U"
			app.doprint ("Updating regulation replacement " + str(replacing_regulation_id))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting regulation replacement " + str(replacing_regulation_id))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating regulation replacement " + str(replacing_regulation_id))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO regulation_replacements_oplog (replacing_regulation_role,
			replacing_regulation_id, replaced_regulation_role, replaced_regulation_id,
			measure_type_id, geographical_area_id, chapter_heading,
			operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""", 
			(replacing_regulation_role,
			replacing_regulation_id, replaced_regulation_role, replaced_regulation_id,
			measure_type_id, geographical_area_id, chapter_heading,
			operation, operation_date))
		except:
			g.app.log_error("regulation replacement", operation, None, replacing_regulation_id, transaction_id, message_id)
		app.conn.commit()
		cur.close()
