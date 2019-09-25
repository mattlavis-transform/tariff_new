import psycopg2
import common.globals as g

class profile_16005_regulation_role_type_description(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date          = app.getTimestamp()
		regulation_role_type_id = app.getValue(oMessage, ".//oub:regulation.role.type.id", True)
		language_id			    = app.getValue(oMessage, ".//oub:language.id", True)
		description			    = app.getValue(oMessage, ".//oub:description", True)

		if update_type == "1":	    # UPDATE
			operation = "U"
			app.doprint ("Updating regulation role type description " + str(regulation_role_type_id))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting regulation role type description " + str(regulation_role_type_id))
		else:					    # INSERT
			operation = "C"
			app.doprint ("Creating regulation role type description " + str(regulation_role_type_id))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO regulation_role_type_descriptions_oplog (regulation_role_type_id, language_id, description, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s)""", 
			(regulation_role_type_id, language_id, description, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("regulation role type description", operation, None, regulation_role_type_id, transaction_id, message_id)
		cur.close()
