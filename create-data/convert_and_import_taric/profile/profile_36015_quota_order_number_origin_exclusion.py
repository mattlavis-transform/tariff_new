from datetime import datetime
import common.globals as g


class profile_36015_quota_order_number_origin_exclusion(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        quota_order_number_origin_sid = app.get_number_value(omsg, ".//oub:quota.order.number.origin.sid", True)
        excluded_geographical_area_sid = app.get_number_value(omsg, ".//oub:excluded.geographical.area.sid", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "quota order number origin exclusion", quota_order_number_origin_sid)

        # Perform business rule validation
        if g.app.perform_taric_validation is True:
            # Check the origin exists
            sql = "select quota_order_number_origin_sid from quota_order_number_origins where quota_order_number_origin_sid = %s"
            params = [
                str(quota_order_number_origin_sid)
            ]
            cur = g.app.conn.cursor()
            cur.execute(sql, params)
            rows = cur.fetchall()
            if len(rows) == 0:
                g.app.record_business_rule_violation("DBFK", "The referenced quota order number origin must exist.", operation, transaction_id, message_id, record_code, sub_record_code, str(quota_order_number_origin_sid))

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO quota_order_number_origin_exclusions_oplog (quota_order_number_origin_sid, excluded_geographical_area_sid, operation, operation_date)
            VALUES (%s, %s, %s, %s)""",
            (quota_order_number_origin_sid, excluded_geographical_area_sid, operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, quota_order_number_origin_sid)
        cur.close()
