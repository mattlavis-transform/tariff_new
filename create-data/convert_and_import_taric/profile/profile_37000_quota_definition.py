import psycopg2
import common.globals as g

class profile_37000_quota_definition(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date                  = app.getTimestamp()
		quota_definition_sid			= app.get_number_value(oMessage, ".//oub:quota.definition.sid", True)
		quota_order_number_id			= app.get_value(oMessage, ".//oub:quota.order.number.id", True)
		quota_order_number_sid			= app.get_number_value(oMessage, ".//oub:quota.order.number.sid", True)
		validity_start_date				= app.get_date_value(oMessage, ".//oub:validity.start.date", True)
		validity_end_date				= app.get_date_value(oMessage, ".//oub:validity.end.date", True)
		volume					        = app.get_value(oMessage, ".//oub:volume", True)
		initial_volume					= app.get_value(oMessage, ".//oub:initial.volume", True)
		monetary_unit_code			    = app.get_value(oMessage, ".//oub:monetary.unit.code", True)
		measurement_unit_code			= app.get_value(oMessage, ".//oub:measurement.unit.code", True)
		measurement_unit_qualifier_code	= app.get_value(oMessage, ".//oub:measurement.unit.qualifier.code", True)
		maximum_precision	            = app.get_value(oMessage, ".//oub:maximum.precision", True)
		critical_state	                = app.get_value(oMessage, ".//oub:critical.state", True)
		critical_threshold          	= app.get_value(oMessage, ".//oub:critical.threshold", True)
		description	                    = app.get_value(oMessage, ".//oub:description", True)

		if g.app.perform_taric_validation == True:
			if validity_end_date == None:
				g.app.add_load_error("QDx - Quota definition must have an end date for " + str(quota_order_number_id))

			if validity_end_date != None:
				if validity_end_date < validity_start_date:
					g.app.add_load_error("QD2 - The start date of the quota definition must be less than or equal to the end date, when loading order number " + str(quota_order_number_id))



			found = False
			for item in g.app.all_quota_order_numbers:
				quota_order_number_id2 = item[0]
				quota_order_number_sid2 = item[1]
				validity_start_date2 = item[2]
				validity_end_date2 = item[3]
				if quota_order_number_sid == quota_order_number_sid2:
					found = True
					break

			if found == False:
				g.app.add_load_error("QD3 - The quota order number must exist, when loading definition for order number " + str(quota_order_number_id))

		
			if update_type in ("1", "3"):
				found = False
				for item in g.app.all_quota_definitions:
					quota_definition_sid2 	= item[0]
					quota_order_number_id2 	= item[1]
					validity_start_date2 	= item[2]
					validity_end_date2 		= item[3]
					if quota_order_number_id2 == quota_order_number_id:
						if validity_start_date2 == validity_start_date:
							if quota_definition_sid2 != quota_definition_sid:
								found = True
								break
							else:
								if update_type == "3":
									g.app.add_load_error("QDx - Quota definition is a duplicate " + str(quota_definition_sid))

				if found == True:
					g.app.add_load_error("QD1 - Quota order number id + start date must be unique, when loading quota definition " + str(quota_definition_sid))


		
		if update_type == "1":	# UPDATE
			operation = "U"
			app.doprint ("Updating quota definition " + str(quota_definition_sid))

		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting quota definition " + str(quota_definition_sid))

		else:					# INSERT
			#print (validity_end_date2)
			if g.app.perform_taric_validation == True:
				if found == True:
					if validity_end_date2 != None:
						if (validity_end_date > validity_end_date2) or (validity_start_date < validity_start_date2):
							g.app.add_load_error("ON8(a) - The validity period of the quota order number must span " \
							"the validity period of the referencing quota definition, when loading quota definition " \
							+ str(quota_definition_sid) + " for quota order number " + quota_order_number_id)
					else:
						if validity_start_date < validity_start_date2:
							g.app.add_load_error("ON8(b) - The validity period of the quota order number must span " \
							"the validity period of the referencing quota definition, when loading quota definition " \
							+ str(quota_definition_sid) + " for quota order number " + quota_order_number_id)
	
			operation = "C"
			app.doprint ("Creating quota definition " + str(quota_definition_sid))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO quota_definitions_oplog (quota_definition_sid, quota_order_number_id, validity_start_date,
			validity_end_date, volume, initial_volume, quota_order_number_sid,
			monetary_unit_code, measurement_unit_code, measurement_unit_qualifier_code,
			maximum_precision, critical_state, critical_threshold, description,
			operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", 
			(quota_definition_sid, quota_order_number_id, validity_start_date,
			validity_end_date, volume, initial_volume, quota_order_number_sid,
			monetary_unit_code, measurement_unit_code, measurement_unit_qualifier_code,
			maximum_precision, critical_state, critical_threshold, description,
			operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("quota definition", operation, quota_definition_sid, None, transaction_id, message_id)
		cur.close()

		if g.app.perform_taric_validation == True:
			g.app.quota_definitions		= g.app.get_quota_definitions()