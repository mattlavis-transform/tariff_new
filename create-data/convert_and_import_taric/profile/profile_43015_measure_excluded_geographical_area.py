import sys
import common.globals as g


class profile_43015_measure_excluded_geographical_area(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        measure_sid = app.get_number_value(omsg, ".//oub:measure.sid", True)
        excluded_geographical_area = app.get_value(omsg, ".//oub:excluded.geographical.area", True)
        geographical_area_sid = app.get_number_value(omsg, ".//oub:geographical.area.sid", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "excluded_geographical_area on measure_sid", str(measure_sid) + "/" + excluded_geographical_area)

        # Perform business rule validation
        # Check the measure exists before approving
        if g.app.perform_taric_validation is True:
            if update_type in ("1", "3"):
                sql = "select measure_sid from measures where measure_sid = %s limit 1"
                params = [
                    str(measure_sid)
                ]
                cur = g.app.conn.cursor()
                cur.execute(sql, params)
                rows = cur.fetchall()
                try:
                    row = rows[0]
                    measure_exists = True
                except:
                    measure_exists = False

                if measure_exists is False:
                    g.app.record_business_rule_violation("DBFK", "Measure must exist (excluded geographical area).", operation, transaction_id, message_id, record_code, sub_record_code, str(measure_sid))

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO measure_excluded_geographical_areas_oplog (measure_sid,
            excluded_geographical_area, geographical_area_sid, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s)""",
            (measure_sid, excluded_geographical_area, geographical_area_sid, operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, measure_sid)
        cur.close()
