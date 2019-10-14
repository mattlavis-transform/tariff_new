import psycopg2
import common.globals as g

class profile_20500_certificate(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date                      = app.getTimestamp()
		certificate_type_code				= app.get_value(oMessage, ".//oub:certificate.type.code", True)
		certificate_code					= app.get_value(oMessage, ".//oub:certificate.code", True)
		code 								= certificate_type_code + certificate_code
		validity_start_date					= app.get_date_value(oMessage, ".//oub:validity.start.date", True)
		validity_end_date					= app.get_date_value(oMessage, ".//oub:validity.end.date", True)

		certificate_types = g.app.get_certificate_types()

		if g.app.perform_taric_validation == True:
			if validity_end_date != None:
				if validity_end_date < validity_start_date:
					g.app.add_load_error("CE3  - The start date of the certificate must be less than or equal to the end date, when loading certificate " + code)

		if update_type == "1":	# UPDATE
			if g.app.perform_taric_validation == True:
				if certificate_type_code not in certificate_types:
					g.app.add_load_error("CE1 - The referenced certificate type must exist. Aborting while trying to update certificate " + certificate_type_code + certificate_code + ".")
			
				operation = "U"
			app.doprint ("Updating certificate " + str(certificate_code))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting certificate " + str(certificate_code))
		else:					# INSERT
			if g.app.perform_taric_validation == True:
				if certificate_type_code not in certificate_types:
					g.app.add_load_error("CE1 - The referenced certificate type must exist. Aborting while trying to insert certificate " + certificate_type_code + certificate_code + ".")
			
			operation = "C"
			app.doprint ("Creating certificate " + str(certificate_code))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO certificates_oplog (certificate_type_code, certificate_code,
			validity_start_date, validity_end_date, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s)""", 
			(certificate_type_code, certificate_code, validity_start_date, validity_end_date, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("certificate", operation, None, certificate_type_code + "|" + certificate_code, transaction_id, message_id)
		cur.close()
