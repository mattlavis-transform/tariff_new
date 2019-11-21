import sys
import common.globals as g


class profile_15000_regulation_group(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        regulation_group_id = app.get_value(omsg, ".//oub:regulation.group.id", True)
        validity_start_date = app.get_date_value(omsg, ".//oub:validity.start.date", True)
        validity_end_date = app.get_date_value(omsg, ".//oub:validity.end.date", True)

        regulation_groups = g.app.get_regulation_groups()

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "regulation group", regulation_group_id)

        # Perform business rule validation
        if g.app.perform_taric_validation is True:
            if update_type == "1":  # UPDATE
                if regulation_group_id not in regulation_groups:
                    g.app.record_business_rule_violation("DBFK", "The regulation group must exist.", operation, transaction_id, message_id, record_code, sub_record_code, regulation_group_id)

            elif update_type == "2":  # DELETE
                if regulation_group_id not in regulation_groups:
                    g.app.record_business_rule_violation("DBFK", "The regulation group must exist.", operation, transaction_id, message_id, record_code, sub_record_code, regulation_group_id)

            elif update_type == "3":  # INSERT
                if regulation_group_id in regulation_groups:
                    g.app.record_business_rule_violation("RG1", "The Regulation group id must be unique.", operation, transaction_id, message_id, record_code, sub_record_code, regulation_group_id)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO regulation_groups_oplog (regulation_group_id, validity_start_date,
            validity_end_date, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s)""",
            (regulation_group_id, validity_start_date, validity_end_date, operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, regulation_group_id)
        cur.close()
