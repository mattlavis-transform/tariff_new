import psycopg2
import sys
from datetime import datetime
import common.globals as g

class profile_10000_footnote_type(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date		= app.getTimestamp()
		footnote_type_id	= app.getValue(oMessage, ".//oub:footnote.type.id", True)
		validity_start_date	= app.getDateValue(oMessage, ".//oub:validity.start.date", True)
		validity_end_date	= app.getDateValue(oMessage, ".//oub:validity.end.date", True)
		application_code	= app.getValue(oMessage, ".//oub:application.code", True)

		footnote_types = g.app.get_footnote_types()

		#Check if national or not
		if footnote_type_id in ('01', '02', '03', '05', '05', '06'):
			national = True
		else:
			national = None

		if update_type == "1":		# UPDATE
			if g.app.perform_taric_validation == True:
				if footnote_type_id not in footnote_types:
					g.app.add_load_error("FOTx - The footnote type must exist for updates. Aborting while trying to update footnote type  " + footnote_type_id + ".")

			operation = "U"
			app.doprint ("Updating footnote type " + str(footnote_type_id))

		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting footnote type " + str(footnote_type_id))

		else:						# INSERT
			if g.app.perform_taric_validation == True:
				if footnote_type_id in footnote_types:
					g.app.add_load_error("FOT1 - The footnote type must be unique. Aborting while trying to insert footnote type " + footnote_type_id + ".")

			operation = "C"
			app.doprint ("Creating footnote type " + str(footnote_type_id))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO footnote_types_oplog (footnote_type_id, validity_start_date,
			validity_end_date, application_code, operation, operation_date, national)
			VALUES (%s, %s, %s, %s, %s, %s, %s)""", 
			(footnote_type_id, validity_start_date, validity_end_date, application_code, operation, operation_date, national))
			app.conn.commit()
		except:
			g.app.log_error("footnote_type", operation, None, footnote_type_id, transaction_id, message_id)
		cur.close()
