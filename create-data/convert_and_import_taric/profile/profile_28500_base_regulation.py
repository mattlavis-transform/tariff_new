import common.globals as g


class profile_28500_base_regulation(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        base_regulation_role = app.get_value(omsg, ".//oub:base.regulation.role", True)
        base_regulation_id = app.get_value(omsg, ".//oub:base.regulation.id", True)
        code = base_regulation_role + base_regulation_id
        published_date = app.get_date_value(omsg, ".//oub:published.date", True)
        officialjournal_number = app.get_value(omsg, ".//oub:officialjournal.number", True)
        officialjournal_page = app.get_value(omsg, ".//oub:officialjournal.page", True)
        validity_start_date = app.get_date_value(omsg, ".//oub:validity.start.date", True)
        validity_end_date = app.get_date_value(omsg, ".//oub:validity.end.date", True)
        effective_end_date = app.get_date_value(omsg, ".//oub:effective.end.date", True)
        community_code = app.get_value(omsg, ".//oub:community.code", True)
        regulation_group_id = app.get_value(omsg, ".//oub:regulation.group.id", True)
        antidumping_regulation_role = app.get_value(omsg, ".//oub:antidumping.regulation.role", True)
        related_antidumping_regulation_id = app.get_value(omsg, ".//oub:related.antidumping.regulation.id", True)
        complete_abrogation_regulation_role = app.get_value(omsg, ".//oub:complete.abrogation.regulation.role", True)
        complete_abrogation_regulation_id = app.get_value(omsg, ".//oub:complete.abrogation.regulation.id", True)
        explicit_abrogation_regulation_role = app.get_value(omsg, ".//oub:explicit.abrogation.regulation.role", True)
        explicit_abrogation_regulation_id = app.get_value(omsg, ".//oub:explicit.abrogation.regulation.id", True)
        replacement_indicator = app.get_value(omsg, ".//oub:replacement.indicator", True)
        stopped_flag = app.get_value(omsg, ".//oub:stopped.flag", True)
        information_text = app.get_value(omsg, ".//oub:information.text", True)
        approved_flag = app.get_value(omsg, ".//oub:approved.flag", True)

        # Set operation types and print load message to screen
        national = None
        operation = g.app.get_loading_message(update_type, "base regulation", base_regulation_id)

        # Perform business rule validation
        if g.app.perform_taric_validation is True:
            base_regulations = g.app.get_base_regulations()
            regulation_groups = g.app.get_regulation_groups()

            if validity_end_date is not None:
                if validity_end_date < validity_start_date:
                    g.app.record_business_rule_violation("ROIMB3", "The start date must be less than or equal to the end date.", operation, transaction_id, message_id, record_code, sub_record_code, base_regulation_id)

            if regulation_group_id not in regulation_groups:
                g.app.record_business_rule_violation("ROIMB4", "The referenced regulation group must exist.", operation, transaction_id, message_id, record_code, sub_record_code, base_regulation_id)

            if update_type == "3":  # INSERT
                if g.app.perform_taric_validation is True:
                    if code in base_regulations:
                        g.app.record_business_rule_violation("ROIMB1", "The (regulation id + role id) must be unique.", operation, transaction_id, message_id, record_code, sub_record_code, base_regulation_id)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO base_regulations_oplog (base_regulation_role,
            base_regulation_id, published_date, officialjournal_number, officialjournal_page,
            validity_start_date, validity_end_date, effective_end_date,
            community_code, regulation_group_id, antidumping_regulation_role,
            related_antidumping_regulation_id, complete_abrogation_regulation_role, complete_abrogation_regulation_id,
            explicit_abrogation_regulation_role, explicit_abrogation_regulation_id,
            replacement_indicator, stopped_flag, information_text, approved_flag,
            operation, operation_date, national)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (base_regulation_role,
            base_regulation_id, published_date, officialjournal_number, officialjournal_page,
            validity_start_date, validity_end_date, effective_end_date,
            community_code, regulation_group_id, antidumping_regulation_role,
            related_antidumping_regulation_id, complete_abrogation_regulation_role, complete_abrogation_regulation_id,
            explicit_abrogation_regulation_role, explicit_abrogation_regulation_id,
            replacement_indicator, stopped_flag, information_text, approved_flag,
            operation, operation_date, national))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, base_regulation_id)
        cur.close()

        if g.app.perform_taric_validation is True:
            g.app.all_regulations = g.app.get_all_regulations()
