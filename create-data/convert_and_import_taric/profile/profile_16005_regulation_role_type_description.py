import common.globals as g


class profile_16005_regulation_role_type_description(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        regulation_role_type_id = app.get_value(omsg, ".//oub:regulation.role.type.id", True)
        language_id = app.get_value(omsg, ".//oub:language.id", True)
        description = app.get_value(omsg, ".//oub:description", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "regulation role type description", regulation_role_type_id)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO regulation_role_type_descriptions_oplog (regulation_role_type_id, language_id, description, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s)""",
            (regulation_role_type_id, language_id, description, operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, regulation_role_type_id)
        cur.close()
