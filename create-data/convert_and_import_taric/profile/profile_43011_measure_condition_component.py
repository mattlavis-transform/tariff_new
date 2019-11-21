import common.globals as g


class profile_43011_measure_condition_component(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        measure_condition_sid = app.get_number_value(omsg, ".//oub:measure.condition.sid", True)
        duty_expression_id = app.get_value(omsg, ".//oub:duty.expression.id", True)
        duty_amount = app.get_value(omsg, ".//oub:duty.amount", True)
        monetary_unit_code = app.get_value(omsg, ".//oub:monetary.unit.code", True)
        measurement_unit_code = app.get_value(omsg, ".//oub:measurement.unit.code", True)
        measurement_unit_qualifier_code = app.get_value(omsg, ".//oub:measurement.unit.qualifier.code", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "Measure condition component", measure_condition_sid)

        # Perform business rule validation
        if g.app.perform_taric_validation is True:
            if update_type in ("1", "3"):  # UPDATE or insert
                sql = "select measure_condition_sid from measure_conditions where measure_condition_sid = %s "
                params = [
                    str(measure_condition_sid)
                ]
                cur = g.app.conn.cursor()
                cur.execute(sql, params)
                rows = cur.fetchall()
                if len(rows) == 0:
                    g.app.record_business_rule_violation("DBFK", "Measure component must exist (excluded geographical area).", operation, transaction_id, message_id, record_code, sub_record_code, str(measure_condition_sid))

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO measure_condition_components_oplog (measure_condition_sid, duty_expression_id, duty_amount,
            monetary_unit_code, measurement_unit_code,
            measurement_unit_qualifier_code, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (measure_condition_sid, duty_expression_id, duty_amount,
            monetary_unit_code, measurement_unit_code,
            measurement_unit_qualifier_code, operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, measure_condition_sid)
        cur.close()
