import psycopg2
import sys
from datetime import datetime
import common.globals as g

class profile_13000_language(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date		    = app.getTimestamp()
		language_id             = app.get_value(oMessage, ".//oub:language.id", True)
		validity_start_date     = app.get_date_value(oMessage, ".//oub:validity.start.date", True)
		validity_end_date       = app.get_date_value(oMessage, ".//oub:validity.end.date", True)

		if update_type == "1":	# UPDATE
			operation = "U"
			app.doprint ("Updating language " + str(language_id))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting language " + str(language_id))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating language " + str(language_id))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO languages_oplog (language_id, validity_start_date,
			validity_end_date, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s)""", 
			(language_id, validity_start_date, validity_end_date, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("language", operation, None, language_id, transaction_id, message_id)
		cur.close()
