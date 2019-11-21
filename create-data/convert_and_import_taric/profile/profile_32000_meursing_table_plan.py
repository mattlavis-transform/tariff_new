import common.globals as g


class profile_32000_meursing_table_plan(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        meursing_table_plan_id = app.get_number_value(omsg, ".//oub:meursing.table.plan.id", True)
        validity_start_date = app.get_date_value(omsg, ".//oub:validity.start.date", True)
        validity_end_date = app.get_date_value(omsg, ".//oub:validity.end.date", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "Meursing table plan ID", meursing_table_plan_id)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO meursing_table_plans_oplog (meursing_table_plan_id,
            validity_start_date, validity_end_date, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s)""",
            (meursing_table_plan_id, validity_start_date, validity_end_date, operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, meursing_table_plan_id)
        cur.close()
