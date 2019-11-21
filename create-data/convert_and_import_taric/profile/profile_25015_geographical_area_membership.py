import common.globals as g


class profile_25015_geographical_area_membership(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        geographical_area_sid = app.get_number_value(omsg, ".//oub:geographical.area.sid", True)
        geographical_area_group_sid = app.get_number_value(omsg, ".//oub:geographical.area.group.sid", True)
        validity_start_date = app.get_date_value(omsg, ".//oub:validity.start.date", True)
        validity_end_date = app.get_date_value(omsg, ".//oub:validity.end.date", True)

        geographical_area_groups = g.app.get_geographical_area_groups()
        countries_regions = g.app.get_countries_regions()

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "geographical area membership", geographical_area_group_sid)

        # Perform business rule validation
        if g.app.perform_taric_validation is True:
            if validity_end_date is not None:
                if validity_end_date < validity_start_date:
                    g.app.record_business_rule_violation("GA15", "The membership start date must be less than or equal to the membership end date.", operation, transaction_id, message_id, record_code, sub_record_code, str(geographical_area_sid))

            if geographical_area_group_sid is not None:
                if geographical_area_group_sid not in geographical_area_groups:
                    g.app.record_business_rule_violation("GA14", "The referenced geographical area group id must exist.", operation, transaction_id, message_id, record_code, sub_record_code, str(geographical_area_group_sid))

            if geographical_area_sid not in countries_regions:
                g.app.record_business_rule_violation("GA12", "The referenced geographical area id (member) must exist.", operation, transaction_id, message_id, record_code, sub_record_code, str(geographical_area_sid))

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO geographical_area_memberships_oplog (geographical_area_sid,
            geographical_area_group_sid, validity_start_date, validity_end_date, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s, %s)""",
            (geographical_area_sid,
            geographical_area_group_sid, validity_start_date, validity_end_date, operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, geographical_area_sid)
        cur.close()
