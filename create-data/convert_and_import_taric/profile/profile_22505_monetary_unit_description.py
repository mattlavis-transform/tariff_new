import psycopg2
import common.globals as g

class profile_22505_monetary_unit_description(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date       = app.getTimestamp()
		monetary_unit_code   = app.getValue(oMessage, ".//oub:monetary.unit.code", True)
		language_id			 = app.getValue(oMessage, ".//oub:language.id", True)
		description			 = app.getValue(oMessage, ".//oub:description", True)

		if update_type == "1":	    # UPDATE
			operation = "U"
			app.doprint ("Updating monetary unit description " + str(monetary_unit_code))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting monetary unit description " + str(monetary_unit_code))
		else:					    # INSERT
			operation = "C"
			app.doprint ("Creating monetary unit description " + str(monetary_unit_code))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO monetary_unit_descriptions_oplog (monetary_unit_code, language_id, description, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s)""", 
			(monetary_unit_code, language_id, description, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("monetary unit description", operation, None, monetary_unit_code, transaction_id, message_id)
		cur.close()
