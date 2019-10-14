import psycopg2
import common.globals as g

class profile_20005_footnote_description_period(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date				    = app.getTimestamp()
		footnote_description_period_sid = app.get_number_value(oMessage, ".//oub:footnote.description.period.sid", True)
		footnote_type_id			    = app.get_value(oMessage, ".//oub:footnote.type.id", True)
		footnote_id					    = app.get_value(oMessage, ".//oub:footnote.id", True)
		validity_start_date			    = app.get_date_value(oMessage, ".//oub:validity.start.date", True)

		footnote_types = g.app.get_footnote_types()

		if g.app.perform_taric_validation == True:
			if footnote_type_id not in footnote_types:
				g.app.add_load_error("FO1 - Footnote type " + footnote_type_id + " does not exist when loading footnote description period " + footnote_type_id + footnote_id)

		if footnote_description_period_sid < 0:
			national = True
		else:
			national = None
		if update_type == "1":		# UPDATE
			operation = "U"
			app.doprint ("Updating footnote description period " + str(footnote_id))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting footnote description period " + str(footnote_id))
		else:						# INSERT
			operation = "C"
			app.doprint ("Creating footnote description period " + str(footnote_id))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO footnote_description_periods_oplog (footnote_description_period_sid,
			footnote_type_id, footnote_id, validity_start_date, operation, operation_date, national)
			VALUES (%s, %s, %s, %s, %s, %s, %s)""", 
			(footnote_description_period_sid, footnote_type_id, footnote_id, validity_start_date, operation, operation_date, national))
			app.conn.commit()
		except:
			g.app.log_error("footnote_description_period", "D", footnote_description_period_sid, footnote_type_id + "|" + footnote_id, transaction_id, message_id)
		cur.close()
