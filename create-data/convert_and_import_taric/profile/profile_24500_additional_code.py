import common.globals as g


class profile_24500_additional_code(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        additional_code_sid = app.get_number_value(omsg, ".//oub:additional.code.sid", True)
        additional_code_type_id = app.get_value(omsg, ".//oub:additional.code.type.id", True)
        additional_code = app.get_value(omsg, ".//oub:additional.code//oub:additional.code", True)
        code = additional_code_type_id + additional_code
        validity_start_date = app.get_date_value(omsg, ".//oub:validity.start.date", True)
        validity_end_date = app.get_date_value(omsg, ".//oub:validity.end.date", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "certificate type", code)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO additional_codes_oplog (additional_code_sid, additional_code_type_id, additional_code,
            validity_start_date, validity_end_date, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (additional_code_sid, additional_code_type_id, additional_code,
            validity_start_date, validity_end_date, operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, code)
        cur.close()
