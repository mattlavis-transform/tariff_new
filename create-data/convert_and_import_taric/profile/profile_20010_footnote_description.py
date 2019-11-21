import common.globals as g


class profile_20010_footnote_description(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        footnote_description_period_sid = app.get_number_value(omsg, ".//oub:footnote.description.period.sid", True)
        language_id = app.get_value(omsg, ".//oub:language.id", True)
        footnote_type_id = app.get_value(omsg, ".//oub:footnote.type.id", True)
        footnote_id = app.get_value(omsg, ".//oub:footnote.id", True)
        code = footnote_type_id + footnote_id
        description = app.get_value(omsg, ".//oub:description", True)

        footnote_types = g.app.get_footnote_types()
        if footnote_description_period_sid < 0:
            national = True
        else:
            national = None

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "footnote description", footnote_description_period_sid)

        # Perform business rule validation
        if g.app.perform_taric_validation is True:
            if footnote_type_id not in footnote_types:
                g.app.record_business_rule_violation("FO1", "The referenced footnote type must exist.", operation, transaction_id, message_id, record_code, sub_record_code, code)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO footnote_descriptions_oplog (footnote_description_period_sid, language_id,
            footnote_type_id, footnote_id, description, operation, operation_date, national)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (footnote_description_period_sid, language_id,
            footnote_type_id, footnote_id, description, operation, operation_date, national))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, code)
        cur.close()
