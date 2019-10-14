import psycopg2
import common.globals as g

class profile_15005_regulation_group_description(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date      = app.getTimestamp()
		regulation_group_id = app.get_value(oMessage, ".//oub:regulation.group.id", True)
		language_id			= app.get_value(oMessage, ".//oub:language.id", True)
		description			= app.get_value(oMessage, ".//oub:description", True)

		if update_type == "1":	    # UPDATE
			operation = "U"
			app.doprint ("Updating regulation group description " + str(regulation_group_id))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting regulation group description " + str(regulation_group_id))
		else:					    # INSERT
			operation = "C"
			app.doprint ("Creating regulation group description " + str(regulation_group_id))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO regulation_group_descriptions_oplog (regulation_group_id, language_id, description, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s)""", 
			(regulation_group_id, language_id, description, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("regulation group description", operation, None, regulation_group_id, transaction_id, message_id)
		cur.close()
