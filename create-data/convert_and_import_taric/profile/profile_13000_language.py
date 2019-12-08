import common.globals as g


class profile_13000_language(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        language_id = app.get_value(omsg, ".//oub:language.id", True)
        validity_start_date = app.get_date_value(omsg, ".//oub:validity.start.date", True)
        validity_end_date = app.get_date_value(omsg, ".//oub:validity.end.date", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "language", language_id)

        # Perform business rule validation
        if g.app.perform_taric_validation is True:
            languages = g.app.get_languages()

            if update_type in ("1", "3"):  # UPDATE or INSERT
                # Business rule LA3
                if validity_end_date is not None:
                    if validity_end_date < validity_start_date:
                        g.app.record_business_rule_violation("LA3", "The start date must be less than or equal to the end date.", operation, transaction_id, message_id, record_code, sub_record_code, language_id)

            if update_type == "3":  # INSERT
                # Business rule LA1
                if language_id in languages:
                    g.app.record_business_rule_violation("LA1", "The language id must be unique.", operation, transaction_id, message_id, record_code, sub_record_code, language_id)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO languages_oplog (language_id, validity_start_date,
            validity_end_date, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s)""",
            (language_id, validity_start_date, validity_end_date, operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, language_id)
        cur.close()
