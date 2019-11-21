import common.globals as g


class profile_10005_footnote_type_description(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        footnote_type_id = app.get_value(omsg, ".//oub:footnote.type.id", True)
        language_id = app.get_value(omsg, ".//oub:language.id", True)
        description = app.get_value(omsg, ".//oub:description", True)

        footnote_type_descriptions = g.app.get_footnote_type_descriptions()

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "footnote type description", footnote_type_id)

        # Perform business rule validation
        if g.app.perform_taric_validation is True:
            if update_type in ("1", "3"):  # UPDATE / INSERT
                if footnote_type_id not in footnote_type_descriptions:
                    g.app.record_business_rule_violation("DBFK", "The footnote type must be unique.", operation, transaction_id, message_id, record_code, sub_record_code, footnote_type_id)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO footnote_type_descriptions_oplog (footnote_type_id, language_id,
            description, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s)""",
            (footnote_type_id, language_id,
            description, operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, footnote_type_id)
        cur.close()
