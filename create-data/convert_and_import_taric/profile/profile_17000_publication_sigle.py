import psycopg2
import sys
from datetime import datetime
import common.globals as g

class profile_17000_publication_sigle(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date		= app.getTimestamp()
		code_type_id		= app.get_value(oMessage, ".//oub:code.type.id", True)
		code				= app.get_value(oMessage, ".//oub:code", True)
		validity_start_date = app.get_date_value(oMessage, ".//oub:validity.start.date", True)
		validity_end_date	= app.get_date_value(oMessage, ".//oub:validity.end.date", True)
		publication_code	= app.get_value(oMessage, ".//oub:publication.code", True)
		publication_sigle	= app.get_value(oMessage, ".//oub:publication.sigle", True)

		if update_type == "1":	    # UPDATE
			operation = "U"
			app.doprint ("Updating publication sigle " + str(code_type_id))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting publication sigle " + str(code_type_id))
		else:					    # INSERT
			operation = "C"
			app.doprint ("Creating publication sigle " + str(code_type_id))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO publication_sigles_oplog (code_type_id, code, validity_start_date,
			validity_end_date, publication_code, publication_sigle, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", 
			(code_type_id, code, validity_start_date, validity_end_date, publication_code, publication_sigle, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("publication sigle", operation, None, code_type_id, transaction_id, message_id)
		cur.close()
