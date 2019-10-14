import psycopg2
import common.globals as g

class profile_29500_prorogation_regulation(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date						= app.getTimestamp()
		prorogation_regulation_role		= app.get_value(oMessage, ".//oub:prorogation.regulation.role", True)
		prorogation_regulation_id		= app.get_value(oMessage, ".//oub:prorogation.regulation.id", True)
		published_date		        	= app.get_date_value(oMessage, ".//oub:published.date", True)
		officialjournal_number			= app.get_value(oMessage, ".//oub:officialjournal.number", True)
		officialjournal_page	        = app.get_value(oMessage, ".//oub:officialjournal.page", True)
		replacement_indicator           = app.get_value(oMessage, ".//oub:replacement.indicator", True)
		information_text                = app.get_value(oMessage, ".//oub:information.text", True)
		approved_flag                  	= app.get_value(oMessage, ".//oub:approved.flag", True)

		if update_type == "1":	# Update
			operation = "U"
			app.doprint ("Updating prorogation regulation " + str(prorogation_regulation_id))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting prorogation regulation " + str(prorogation_regulation_id))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating prorogation regulation " + str(prorogation_regulation_id))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO prorogation_regulations_oplog (prorogation_regulation_role,
			prorogation_regulation_id, published_date, officialjournal_number,
			officialjournal_page, replacement_indicator, information_text, approved_flag,
			operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", 
			(prorogation_regulation_role,
			prorogation_regulation_id, published_date, officialjournal_number,
			officialjournal_page, replacement_indicator, information_text, approved_flag,
			operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("prorogation regulation", operation, None, prorogation_regulation_id, transaction_id, message_id)
		cur.close()
		
		if g.app.perform_taric_validation == True:
			g.app.all_regulations = g.app.get_all_regulations()
