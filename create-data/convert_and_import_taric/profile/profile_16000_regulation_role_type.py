import psycopg2
import sys
from datetime import datetime
import common.globals as g

class profile_16000_regulation_role_type(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date		    = app.getTimestamp()
		regulation_role_type_id = app.getValue(oMessage, ".//oub:regulation.role.type.id", True)
		validity_start_date	    = app.getDateValue(oMessage, ".//oub:validity.start.date", True)
		validity_end_date	    = app.getDateValue(oMessage, ".//oub:validity.end.date", True)

		if update_type == "1":	    # UPDATE
			operation = "U"
			app.doprint ("Updating regulation role type " + str(regulation_role_type_id))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting regulation role type " + str(regulation_role_type_id))
		else:					    # INSERT
			operation = "C"
			app.doprint ("Creating regulation role type " + str(regulation_role_type_id))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO regulation_role_types_oplog (regulation_role_type_id, validity_start_date,
			validity_end_date, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s)""", 
			(regulation_role_type_id, validity_start_date, validity_end_date, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("regulation role type", operation, None, regulation_role_type_id, transaction_id, message_id)
		cur.close()
