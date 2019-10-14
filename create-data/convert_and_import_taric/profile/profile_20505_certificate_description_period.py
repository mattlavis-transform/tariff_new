import psycopg2
import common.globals as g

class profile_20505_certificate_description_period(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date				        = app.getTimestamp()
		certificate_description_period_sid  = app.get_number_value(oMessage, ".//oub:certificate.description.period.sid", True)
		certificate_type_code			    = app.get_value(oMessage, ".//oub:certificate.type.code", True)
		certificate_code				    = app.get_value(oMessage, ".//oub:certificate.code", True)
		validity_start_date			        = app.get_date_value(oMessage, ".//oub:validity.start.date", True)

		certificate_types = g.app.get_certificate_types()

		if update_type == "1":	# Update
			if g.app.perform_taric_validation == True:
				if certificate_type_code not in certificate_types:
					g.app.add_load_error("CE1 - The referenced certificate type must exist. Aborting while trying to update certificate description period " + certificate_type_code + certificate_code + ".")
			
			operation = "U"
			app.doprint ("Updating certificate description period " + str(certificate_description_period_sid))
		
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting certificate description period " + str(certificate_description_period_sid))
		
		else:					# INSERT
			if g.app.perform_taric_validation == True:
				if certificate_type_code not in certificate_types:
					g.app.add_load_error("CE1 - The referenced certificate type must exist. Aborting while trying to insert certificate description period " + certificate_type_code + certificate_code + ".")

			operation = "C"
			app.doprint ("Creating certificate description period " + str(certificate_description_period_sid))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO certificate_description_periods_oplog (certificate_description_period_sid,
			certificate_type_code, certificate_code, validity_start_date, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s)""", 
			(certificate_description_period_sid, certificate_type_code, certificate_code, validity_start_date, operation, operation_date))
		except:
			g.app.log_error("certificate description period", operation, certificate_description_period_sid, certificate_type_code + "|" + certificate_code, transaction_id, message_id)
		app.conn.commit()
		cur.close()
