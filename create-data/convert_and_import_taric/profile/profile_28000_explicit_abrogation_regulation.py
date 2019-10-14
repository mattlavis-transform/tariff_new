import psycopg2
import common.globals as g

class profile_28000_explicit_abrogation_regulation(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date						= app.getTimestamp()
		explicit_abrogation_regulation_role	= app.get_value(oMessage, ".//oub:explicit.abrogation.regulation.role", True)
		explicit_abrogation_regulation_id	= app.get_value(oMessage, ".//oub:explicit.abrogation.regulation.id", True)
		published_date		        		= app.get_date_value(oMessage, ".//oub:published.date", True)
		officialjournal_number			    = app.get_value(oMessage, ".//oub:officialjournal.number", True)
		officialjournal_page	            = app.get_value(oMessage, ".//oub:officialjournal.page", True)
		replacement_indicator               = app.get_value(oMessage, ".//oub:replacement.indicator", True)
		abrogation_date		        		= app.get_date_value(oMessage, ".//oub:abrogation.date", True)
		information_text                  	= app.get_value(oMessage, ".//oub:information.text", True)
		approved_flag                  		= app.get_value(oMessage, ".//oub:approved.flag", True)

		if update_type == "1":	# Update
			operation = "U"
			app.doprint ("Updating explicit abrogation regulation " + str(explicit_abrogation_regulation_id))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting explicit abrogation regulation " + str(explicit_abrogation_regulation_id))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating explicit abrogation regulation " + str(explicit_abrogation_regulation_id))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO explicit_abrogation_regulations_oplog (explicit_abrogation_regulation_role,
			explicit_abrogation_regulation_id, published_date, officialjournal_number,
			officialjournal_page, replacement_indicator, abrogation_date, information_text, approved_flag,
			operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", 
			(explicit_abrogation_regulation_role,
			explicit_abrogation_regulation_id, published_date, officialjournal_number,
			officialjournal_page, replacement_indicator, abrogation_date, information_text, approved_flag,
			operation, operation_date))
			app.conn.commit()
		except:
			print ("Error")
			g.app.log_error("explicit abrogation regulation", operation, None, explicit_abrogation_regulation_id, transaction_id, message_id)
		cur.close()

		if g.app.perform_taric_validation == True:
			g.app.all_regulations = g.app.get_all_regulations()
