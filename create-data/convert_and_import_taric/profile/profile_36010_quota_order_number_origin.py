from datetime import datetime
import common.globals as g


class profile_36010_quota_order_number_origin(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        quota_order_number_origin_sid = app.get_number_value(omsg, ".//oub:quota.order.number.origin.sid", True)
        quota_order_number_sid = app.get_number_value(omsg, ".//oub:quota.order.number.sid", True)
        geographical_area_id = app.get_value(omsg, ".//oub:geographical.area.id", True)
        validity_start_date = app.get_date_value(omsg, ".//oub:validity.start.date", True)
        validity_end_date = app.get_date_value(omsg, ".//oub:validity.end.date", True)
        geographical_area_sid = app.get_number_value(omsg, ".//oub:geographical.area.sid", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "quota order number origin", str(quota_order_number_sid) + " / " + geographical_area_id)

        # Perform business rule validation
        if g.app.perform_taric_validation is True:
            if geographical_area_sid not in g.app.geographical_area_sids:
                g.app.record_business_rule_violation("ON4(a)", "The referenced geographical area must exist.", operation, transaction_id, message_id, record_code, sub_record_code, str(quota_order_number_sid))

            if geographical_area_id not in g.app.geographical_areas:
                g.app.record_business_rule_violation("ON4(b)", "The referenced geographical area must exist.", operation, transaction_id, message_id, record_code, sub_record_code, str(quota_order_number_sid))

        # Load data
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
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, str(quota_order_number_sid) + " / " + geographical_area_id)
        cur.close()
