import common.globals as g


class profile_12000_additional_code_type(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        additional_code_type_id = app.get_value(omsg, ".//oub:additional.code.type.id", True)
        validity_start_date = app.get_date_value(omsg, ".//oub:validity.start.date", True)
        validity_end_date = app.get_date_value(omsg, ".//oub:validity.end.date", True)
        application_code = app.get_value(omsg, ".//oub:application.code", True)
        meursing_table_plan_id = app.get_value(omsg, ".//oub:meursing.table.plan.id", True)

        additional_code_types = g.app.get_additional_code_types()

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "additional code type", additional_code_type_id)

        # Perform business rule validation
        if g.app.perform_taric_validation is True:
            if update_type == "1":  # UPDATE
                if additional_code_type_id not in additional_code_types:
                    g.app.record_business_rule_violation("DBFK", "The additional code type must exist.", operation, transaction_id, message_id, record_code, sub_record_code, additional_code_type_id)

            elif update_type == "3":  # INSERT
                if additional_code_type_id in additional_code_types:
                    g.app.record_business_rule_violation("CT1", "The additional code type must be unique", operation, transaction_id, message_id, record_code, sub_record_code, additional_code_type_id)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO additional_code_types_oplog (additional_code_type_id, validity_start_date,
            validity_end_date, application_code, meursing_table_plan_id, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (additional_code_type_id, validity_start_date,
            validity_end_date, application_code, meursing_table_plan_id, operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, additional_code_type_id)
        cur.close()
