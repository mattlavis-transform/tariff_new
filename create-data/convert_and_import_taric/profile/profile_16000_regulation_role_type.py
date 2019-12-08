import common.globals as g


class profile_16000_regulation_role_type(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        regulation_role_type_id = app.get_value(omsg, ".//oub:regulation.role.type.id", True)
        validity_start_date = app.get_date_value(omsg, ".//oub:validity.start.date", True)
        validity_end_date = app.get_date_value(omsg, ".//oub:validity.end.date", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "regulation role type", regulation_role_type_id)

        # Perform business rule validation
        if g.app.perform_taric_validation is True:
            if update_type in ("1", "3"):  # UPDATE or INSERT
                # Business rule RT5
                if validity_end_date is not None:
                    if validity_end_date < validity_start_date:
                        g.app.record_business_rule_violation("RT5", "The start date must be less than or equal to the end date.", operation, transaction_id, message_id, record_code, sub_record_code, regulation_role_type_id)

            elif update_type == "2":  # DELETE
                used_regulation_roles = g.app.get_used_regulation_roles()
                # Business rule DBFK
                if regulation_role_type_id in used_regulation_roles:
                    g.app.record_business_rule_violation("RT2", "The regulation role cannot be deleted if it is used for a regulation.", operation, transaction_id, message_id, record_code, sub_record_code, regulation_role_type_id)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO regulation_role_types_oplog (regulation_role_type_id, validity_start_date,
            validity_end_date, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s)""",
            (regulation_role_type_id, validity_start_date, validity_end_date, operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, regulation_role_type_id)
        cur.close()
