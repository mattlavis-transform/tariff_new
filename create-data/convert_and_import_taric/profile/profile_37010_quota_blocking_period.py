import common.globals as g


class profile_37010_quota_blocking_period(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        quota_blocking_period_sid = app.get_number_value(omsg, ".//oub:quota.blocking.period.sid", True)
        quota_definition_sid = app.get_number_value(omsg, ".//oub:quota.definition.sid", True)
        blocking_start_date = app.get_date_value(omsg, ".//oub:blocking.start.date", True)
        blocking_end_date = app.get_date_value(omsg, ".//oub:blocking.end.date", True)
        blocking_period_type = app.get_value(omsg, ".//oub:blocking.period.type", True)
        description = app.get_value(omsg, ".//oub:description", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "quota blocking period", quota_blocking_period_sid)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO quota_blocking_periods_oplog (quota_blocking_period_sid,
            quota_definition_sid, blocking_start_date, blocking_end_date, blocking_period_type,
            description, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (quota_blocking_period_sid,
            quota_definition_sid, blocking_start_date, blocking_end_date, blocking_period_type,
            description, operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, quota_definition_sid)
        cur.close()
