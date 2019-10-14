import psycopg2
from datetime import datetime
import common.globals as g

class profile_36010_quota_order_number_origin(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date                      = app.getTimestamp()
		quota_order_number_origin_sid		= app.get_number_value(oMessage, ".//oub:quota.order.number.origin.sid", True)
		quota_order_number_sid				= app.get_number_value(oMessage, ".//oub:quota.order.number.sid", True)
		geographical_area_id				= app.get_value(oMessage, ".//oub:geographical.area.id", True)
		validity_start_date					= app.get_date_value(oMessage, ".//oub:validity.start.date", True)
		validity_end_date					= app.get_date_value(oMessage, ".//oub:validity.end.date", True)
		geographical_area_sid				= app.get_number_value(oMessage, ".//oub:geographical.area.sid", True)

		if g.app.perform_taric_validation == True:
			if geographical_area_sid not in g.app.geographical_area_sids:
				g.app.add_load_error("ON4(a) - The referenced geographical area must exist, when loading order number SID " + str(quota_order_number_sid))

			if geographical_area_id not in g.app.geographical_areas:
				g.app.add_load_error("ON4(b) - The referenced geographical area must exist, when loading order number ID " + str(quota_order_number_sid))

		if update_type == "1":	# UPDATE
			operation = "U"
			app.doprint ("Updating quota order number origin " + str(quota_order_number_origin_sid))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting quota order number origin " + str(quota_order_number_origin_sid))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating quota order number origin " + str(quota_order_number_origin_sid))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO quota_order_number_origins_oplog (quota_order_number_origin_sid,
			quota_order_number_sid, geographical_area_id, validity_start_date, validity_end_date,
			geographical_area_sid, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", 
			(quota_order_number_origin_sid,
			quota_order_number_sid, geographical_area_id, validity_start_date, validity_end_date,
			geographical_area_sid, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("quota order number origin", operation, quota_order_number_origin_sid, None, transaction_id, message_id)
		cur.close()
