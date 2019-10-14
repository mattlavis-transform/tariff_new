import psycopg2
import common.globals as g

class profile_11000_certificate_type(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date                      = app.getTimestamp()
		certificate_type_code				= app.get_value(oMessage, ".//oub:certificate.type.code", True)
		validity_start_date					= app.get_date_value(oMessage, ".//oub:validity.start.date", True)
		validity_end_date					= app.get_date_value(oMessage, ".//oub:validity.end.date", True)

		certificate_types = g.app.get_certificate_types()

		if update_type == "1":	# Update
			if g.app.perform_taric_validation == True:
				if certificate_type_code not in certificate_types:
					g.app.add_load_error("CETx - The certificate type must exist for updates. Aborting while trying to update certificate type  " + certificate_type_code + ".")
			operation = "U"
			app.doprint ("Updating certificate type " + str(certificate_type_code))

		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting certificate type " + str(certificate_type_code))

		else:					# INSERT
			if g.app.perform_taric_validation == True:
				if certificate_type_code in certificate_types:
					g.app.add_load_error("CET1 - The certificate type must be unique. Aborting while trying to insert certificate type " + certificate_type_code + ".")

			operation = "C"
			app.doprint ("Creating certificate type " + str(certificate_type_code))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO certificate_types_oplog (certificate_type_code, validity_start_date,
			validity_end_date, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s)""", 
			(certificate_type_code, validity_start_date,
			validity_end_date, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("certificate_type", operation, None, certificate_type_code, transaction_id, message_id)
		cur.close()
