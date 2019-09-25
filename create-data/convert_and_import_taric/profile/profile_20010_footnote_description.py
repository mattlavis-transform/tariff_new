import psycopg2
import common.globals as g

class profile_20010_footnote_description(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date                  = app.getTimestamp()
		footnote_description_period_sid = app.getNumberValue(oMessage, ".//oub:footnote.description.period.sid", True)
		language_id						= app.getValue(oMessage, ".//oub:language.id", True)
		footnote_type_id				= app.getValue(oMessage, ".//oub:footnote.type.id", True)
		footnote_id 				    = app.getValue(oMessage, ".//oub:footnote.id", True)
		description						= app.getValue(oMessage, ".//oub:description", True)

		footnote_types = g.app.get_footnote_types()

		if g.app.perform_taric_validation == True:
			if footnote_type_id not in footnote_types:
				g.app.add_load_error("FO1 - Footnote type " + footnote_type_id + " does not exist when loading footnote description " + footnote_type_id + footnote_id)

		if footnote_description_period_sid < 0:
			national = True
		else:
			national = None
		if update_type == "1":	# Update
			operation = "U"
			app.doprint ("Updating footnote description " + str(footnote_description_period_sid))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting footnote description " + str(footnote_description_period_sid))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating footnote description " + str(footnote_description_period_sid))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO footnote_descriptions_oplog (footnote_description_period_sid, language_id,
			footnote_type_id, footnote_id, description, operation, operation_date, national)
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", 
			(footnote_description_period_sid, language_id,
			footnote_type_id, footnote_id, description, operation, operation_date, national))
			app.conn.commit()
		except:
			g.app.log_error("footnote_description", operation, footnote_description_period_sid, footnote_type_id + "|" + footnote_id, transaction_id, message_id)
		cur.close()
