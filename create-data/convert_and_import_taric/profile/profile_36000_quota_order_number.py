import psycopg2
from datetime import datetime
import common.globals as g

class profile_36000_quota_order_number(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date                      = app.getTimestamp()
		quota_order_number_sid				= app.getNumberValue(oMessage, ".//oub:quota.order.number.sid", True)
		quota_order_number_id				= app.getValue(oMessage, ".//oub:quota.order.number.id", True)
		validity_start_date					= app.getDateValue(oMessage, ".//oub:validity.start.date", True)
		validity_end_date					= app.getDateValue(oMessage, ".//oub:validity.end.date", True)

		if g.app.perform_taric_validation == True:
			if validity_end_date != None:
				if validity_end_date < validity_start_date:
					g.app.add_load_error("ON3 - The start date of the quota order number must be less than or equal to the end date, when loading order number " + str(quota_order_number_id))

			quota_order_numbers = g.app.get_quota_order_numbers()

		if update_type == "1":		# UPDATE
			operation = "U"
			app.doprint ("Updating quota order number " + str(quota_order_number_sid))

		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting quota order number " + str(quota_order_number_sid))

		else:					# INSERT
			if g.app.perform_taric_validation == True:
				for qon in g.app.quota_order_numbers:
					quota_order_number_id2	= qon[0]
					quota_order_number_sid2 = qon[1]
					validity_start_date2	= qon[2]
					validity_end_date2		= qon[3]

					if quota_order_number_id == quota_order_number_id2 and validity_start_date == validity_start_date2: 
						g.app.add_load_error("ON1 - Quota order number id + start date must be unique, on inserting order number " + quota_order_number_id)
						break

					if quota_order_number_sid == quota_order_number_sid2: 
						g.app.add_load_error("ONx - Quota order number sid already exists, on inserting order number " + quota_order_number_id)
						break

					if quota_order_number_id == quota_order_number_id2:
						if validity_end_date == None:
							if validity_end_date2 != None:
								if validity_start_date <= validity_end_date2:
									g.app.add_load_error("ON2(a) - There may be no overlap in time of two quota order numbers " \
									"with the same quota order number id, on inserting order number " + quota_order_number_id)
									break
							else:
								g.app.add_load_error("ON2(b) - There may be no overlap in time of two quota order numbers " \
								"with the same quota order number id, on inserting order number " + quota_order_number_id)
								break
						else:
							if validity_end_date2 != None:
								if (validity_start_date <= validity_end_date2 and validity_end_date >= validity_start_date2):
									g.app.add_load_error("ON2(c) - There may be no overlap in time of two quota order numbers " \
									"with the same quota order number id, on inserting order number " + quota_order_number_id)
									break
							else:
								#print (validity_start_date, validity_start_date2, validity_end_date, validity_end_date2)
								g.app.add_load_error("ON2(d) - There may be no overlap in time of two quota order numbers " \
								"with the same quota order number id, on inserting order number " + quota_order_number_id)
								break
			

			operation = "C"
			app.doprint ("Creating quota order number " + str(quota_order_number_sid))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO quota_order_numbers_oplog (quota_order_number_sid,
			quota_order_number_id, validity_start_date, validity_end_date, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s)""", 
			(quota_order_number_sid, quota_order_number_id, validity_start_date, validity_end_date, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("quota order number", operation, quota_order_number_sid, quota_order_number_id, transaction_id, message_id)
		cur.close()

		quota_order_numbers = g.app.get_quota_order_numbers()