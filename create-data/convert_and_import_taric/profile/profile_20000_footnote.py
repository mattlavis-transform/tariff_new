import psycopg2, sys
import common.globals as g

class profile_20000_footnote(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date				= app.getTimestamp()
		footnote_type_id			= app.getValue(oMessage, ".//oub:footnote.type.id", True)
		footnote_id					= app.getValue(oMessage, ".//oub:footnote.id", True)
		code						= footnote_type_id + footnote_id
		validity_start_date			= app.getDateValue(oMessage, ".//oub:validity.start.date", True)
		validity_end_date			= app.getDateValue(oMessage, ".//oub:validity.end.date", True)

		if g.app.perform_taric_validation == True:
			if validity_end_date != None:
				if validity_end_date < validity_start_date:
					g.app.add_load_error("FO3 - The start date of the footnote must be less than or equal to the end date, when loading footnote " + code)


		footnote_types	= g.app.get_footnote_types()
		footnotes		= g.app.get_footnotes()

		if footnote_type_id in ('01', '02', '03', '05', '05', '06'):
			national = True
		else:
			national = None

		if g.app.perform_taric_validation == True:
			if footnote_type_id not in footnote_types: # This applies to all actions
				g.app.add_load_error("FO1 - Footnote type " + footnote_type_id + " does not exist when loading footnote " + code)

		if update_type == "1":	# UPDATE
			operation = "U"
			app.doprint ("Updating footnote " + footnote_type_id + str(footnote_id))

		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting footnote " + footnote_type_id + str(footnote_id))

		else:					# INSERT
			if g.app.perform_taric_validation == True:
				if code in footnotes:
					g.app.add_load_error("FO2 - The combination footnote type and code must be unique, when loading footnote " + footnote_type_id + footnote_id)

			operation = "C"
			app.doprint ("Creating footnote " + footnote_type_id + str(footnote_id))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO footnotes_oplog (footnote_type_id, footnote_id, validity_start_date,
			validity_end_date, operation, operation_date, national)
			VALUES (%s, %s, %s, %s, %s, %s, %s)""", 
			(footnote_type_id, footnote_id, validity_start_date, validity_end_date, operation, operation_date, national))
			app.conn.commit()
		except:
			g.app.log_error("footnote", operation, None, footnote_type_id + "|" + footnote_id, transaction_id, message_id)
		cur.close()
