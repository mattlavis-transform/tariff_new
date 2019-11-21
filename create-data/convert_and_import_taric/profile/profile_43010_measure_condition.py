import common.globals as g


class profile_43010_measure_condition(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        measure_condition_sid = app.get_number_value(omsg, ".//oub:measure.condition.sid", True)
        measure_sid = app.get_number_value(omsg, ".//oub:measure.sid", True)
        condition_code = app.get_value(omsg, ".//oub:condition.code", True)
        component_sequence_number = app.get_value(omsg, ".//oub:component.sequence.number", True)
        condition_duty_amount = app.get_value(omsg, ".//oub:condition.duty.amount", True)
        condition_monetary_unit_code = app.get_value(omsg, ".//oub:condition.monetary.unit.code", True)
        condition_measurement_unit_code = app.get_value(omsg, ".//oub:condition.measurement.unit.code", True)
        condition_measurement_unit_qualifier_code = app.get_value(omsg, ".//oub:condition.measurement.unit.qualifier.code", True)
        action_code = app.get_value(omsg, ".//oub:action.code", True)
        certificate_type_code = app.get_value(omsg, ".//oub:certificate.type.code", True)
        certificate_code = app.get_value(omsg, ".//oub:certificate.code", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "measure condition on measure_sid", measure_sid)

        # Perform business rule validation
        # A measure must exist for a measure condition to be inserted or updated (not deleted, as this may have happened in the same transaction)
        if g.app.perform_taric_validation is True:
            update_string = g.app.get_update_string(operation)
            if update_type in ("1", "3"):
                sql = "select count(measure_sid) from measures where measure_sid = %s"
                params = [
                    str(measure_sid)
                ]
                cur = g.app.conn.cursor()
                cur.execute(sql, params)
                row = cur.fetchone()
                if row[0] == 1:
                    g.app.record_business_rule_violation("DBFK", "Measure must exist (measure condition).", operation, transaction_id, message_id, record_code, sub_record_code, str(measure_sid))

        # You must not be able to insert a new measure condition if that measure condition already exists
        if g.app.perform_taric_validation is True:
            if update_type == "3":
                sql = "select count(measure_sid) from measure_conditions where measure_condition_sid = %s"
                params = [
                    str(measure_condition_sid)
                ]
                cur = g.app.conn.cursor()
                cur.execute(sql, params)
                row = cur.fetchone()
                if row[0] == 1:
                    g.app.record_business_rule_violation("DBFK", "Measure must exist (measure condition).", operation, transaction_id, message_id, record_code, sub_record_code, str(measure_sid))

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO measure_conditions_oplog (measure_condition_sid, measure_sid, condition_code,
            component_sequence_number, condition_duty_amount, condition_monetary_unit_code,
            condition_measurement_unit_code, condition_measurement_unit_qualifier_code, action_code,
            certificate_type_code, certificate_code, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (measure_condition_sid, measure_sid, condition_code,
            component_sequence_number, condition_duty_amount, condition_monetary_unit_code,
            condition_measurement_unit_code, condition_measurement_unit_qualifier_code, action_code,
            certificate_type_code, certificate_code, operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, measure_sid)
        cur.close()
