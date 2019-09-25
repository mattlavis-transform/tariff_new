import psycopg2
import common.globals as g

class profile_11005_certificate_type_description(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date                  = app.getTimestamp()
		certificate_type_code			= app.getValue(oMessage, ".//oub:certificate.type.code", True)
		language_id						= app.getValue(oMessage, ".//oub:language.id", True)
		description						= app.getValue(oMessage, ".//oub:description", True)

		certificate_type_descriptions = g.app.get_certificate_type_descriptions()

		if update_type == "1":		# UPDATE
			if g.app.perform_taric_validation == True:
				if certificate_type_code not in certificate_type_descriptions:
					g.app.add_load_error("CETx - The footnote type must exist for updates. Aborting while trying to update certificate type description " + certificate_type_code + ".")
			operation = "U"
			app.doprint ("Updating certificate type description " + str(certificate_type_code))

		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting certificate type description " + str(certificate_type_code))

		else:						# INSERT
			if g.app.perform_taric_validation == True:
				if certificate_type_code in certificate_type_descriptions:
					g.app.add_load_error("CETx - The footnote type must be unique for inserts. Aborting while trying to insert certificate type description " + certificate_type_code + ".")
			operation = "C"
			app.doprint ("Creating certificate type description " + str(certificate_type_code))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO certificate_type_descriptions_oplog (certificate_type_code, language_id,
			description, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s)""", 
			(certificate_type_code, language_id,
			description, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("certificate_type_description", operation, None, certificate_type_code, transaction_id, message_id)
		cur.close()
