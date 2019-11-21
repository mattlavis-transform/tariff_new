import common.globals as g


class profile_25000_geographical_area(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        geographical_area_sid = app.get_number_value(omsg, ".//oub:geographical.area.sid", True)
        geographical_area_id = app.get_value(omsg, ".//oub:geographical.area.id", True)
        validity_start_date = app.get_date_value(omsg, ".//oub:validity.start.date", True)
        validity_end_date = app.get_date_value(omsg, ".//oub:validity.end.date", True)
        geographical_code = app.get_value(omsg, ".//oub:geographical.code", True)
        parent_geographical_area_group_sid = app.get_number_value(omsg, ".//oub:parent.geographical.area.group.sid", True)

        geographical_area_groups = g.app.get_geographical_area_groups()

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "geographical area", geographical_area_id)

        # Perform business rule validation
        if g.app.perform_taric_validation is True:
            if validity_end_date is not None:
                if validity_end_date < validity_start_date:
                    g.app.record_business_rule_violation("GA2", "The start date must be less than or equal to the end date.", operation, transaction_id, message_id, record_code, sub_record_code, geographical_area_id)

            if parent_geographical_area_group_sid is not None:
                if parent_geographical_area_group_sid not in geographical_area_groups:
                    g.app.record_business_rule_violation("GA4", " The referenced parent geographical area group must be an existing geographical "
                    "area with area code = 1 (geographical area group).", operation, transaction_id, message_id, record_code, sub_record_code, geographical_area_id)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO geographical_areas_oplog (geographical_area_sid, geographical_area_id,
            validity_start_date, validity_end_date, geographical_code,
            parent_geographical_area_group_sid, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (geographical_area_sid, geographical_area_id,
            validity_start_date, validity_end_date, geographical_code,
            parent_geographical_area_group_sid, operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, geographical_area_id)
        cur.close()

        if g.app.perform_taric_validation is True:
            g.app.geographical_area_sids = g.app.get_all_geographical_area_sids()
            g.app.geographical_areas = g.app.get_all_geographical_areas()
