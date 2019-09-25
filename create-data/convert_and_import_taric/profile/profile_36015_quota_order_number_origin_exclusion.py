import psycopg2
from datetime import datetime
import common.globals as g

class profile_36015_quota_order_number_origin_exclusion(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date                  = app.getTimestamp()
		quota_order_number_origin_sid	= app.getNumberValue(oMessage, ".//oub:quota.order.number.origin.sid", True)
		excluded_geographical_area_sid	= app.getNumberValue(oMessage, ".//oub:excluded.geographical.area.sid", True)

		if update_type == "1":	# Update
			operation = "U"
			app.doprint ("Updating quota order number origin exclusion on qo  " + str(quota_order_number_origin_sid))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting quota order number origin exclusion on qo  " + str(quota_order_number_origin_sid))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating quota order number origin exclusion on qon " + str(quota_order_number_origin_sid))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO quota_order_number_origin_exclusions_oplog (quota_order_number_origin_sid, excluded_geographical_area_sid, operation, operation_date)
			VALUES (%s, %s, %s, %s)""", 
			(quota_order_number_origin_sid, excluded_geographical_area_sid, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("quota order number origin exclusion", operation, excluded_geographical_area_sid, None, transaction_id, message_id)
		cur.close()
