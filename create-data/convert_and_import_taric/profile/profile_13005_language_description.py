import psycopg2
import sys
from datetime import datetime
import common.globals as g

class profile_13005_language_description(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date		    = app.getTimestamp()
		language_code_id        = app.get_value(oMessage, ".//oub:language.code.id", True)
		language_id             = app.get_value(oMessage, ".//oub:language.id", True)
		description             = app.get_value(oMessage, ".//oub:description", True)

		if update_type == "1":	# UPDATE
			operation = "U"
			app.doprint ("Updating language description " + str(language_id))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting language description " + str(language_id))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating language description " + str(language_id))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO language_descriptions_oplog (language_code_id, language_id, description, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s)""", 
			(language_code_id, language_id, description, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("language description", operation, None, language_id, transaction_id, message_id)
		cur.close()
