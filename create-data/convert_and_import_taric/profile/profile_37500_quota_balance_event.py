import common.globals as g


class profile_37500_quota_balance_event(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        quota_definition_sid = app.get_number_value(omsg, ".//oub:quota.definition.sid", True)
        occurrence_timestamp = app.get_value(omsg, ".//oub:occurrence.timestamp", True)
        old_balance = app.get_value(omsg, ".//oub:old.balance", True)
        new_balance = app.get_value(omsg, ".//oub:new.balance", True)
        imported_amount = app.get_number_value(omsg, ".//oub:imported.amount", True)
        last_import_date_in_allocation = app.get_date_value(omsg, ".//oub:last.import.date.in.allocation", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "quota balance event for quota definition", quota_definition_sid)

        # Perform business rule validation
        if g.app.perform_taric_validation is True:
            if quota_definition_sid not in g.app.quota_definitions:
                g.app.record_business_rule_violation("DBFK", "Quota definition must exist.", operation, transaction_id, message_id, record_code, sub_record_code, str(quota_definition_sid))

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO quota_balance_events_oplog (
            quota_definition_sid, occurrence_timestamp, old_balance, new_balance, imported_amount, last_import_date_in_allocation, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (quota_definition_sid, occurrence_timestamp, old_balance, new_balance, imported_amount, last_import_date_in_allocation, operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, quota_definition_sid)
        cur.close()
