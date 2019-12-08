import common.globals as g


class profile_14000_measure_type_series(object):
    def import_node(self, app, update_type, omsg, transaction_id, message_id, record_code, sub_record_code):
        g.app.message_count += 1
        operation_date = app.get_timestamp()
        measure_type_series_id = app.get_value(omsg, ".//oub:measure.type.series.id", True)
        validity_start_date = app.get_date_value(omsg, ".//oub:validity.start.date", True)
        validity_end_date = app.get_date_value(omsg, ".//oub:validity.end.date", True)
        measure_type_combination = app.get_value(omsg, ".//oub:measure.type.combination", True)

        # Set operation types and print load message to screen
        operation = g.app.get_loading_message(update_type, "measure type series", measure_type_series_id)

        # Perform business rule validation
        if g.app.perform_taric_validation is True:
            measure_type_series = g.app.get_measure_type_series()
            used_measure_type_series = g.app.get_used_measure_type_series()

            if update_type in ("1", "3"):  # UPDATE or INSERT
                # Business rule MTS3
                if validity_end_date is not None:
                    if validity_end_date < validity_start_date:
                        g.app.record_business_rule_violation("MTS3", "The start date must be less than or equal to the end date.", operation, transaction_id, message_id, record_code, sub_record_code, measure_type_series_id)

            if update_type == "3":  # INSERT
                # Business rule MTS1
                if measure_type_series_id in measure_type_series:
                    g.app.record_business_rule_violation("MTS1", "The measure type series must be unique.", operation, transaction_id, message_id, record_code, sub_record_code, measure_type_series_id)

            elif update_type == "2":  # DELETE
                # Business rule MTS2
                if measure_type_series_id in used_measure_type_series:
                    g.app.record_business_rule_violation("MTS2", "The measure type series cannot be deleted if it is associated with a measure type.", operation, transaction_id, message_id, record_code, sub_record_code, measure_type_series_id)

        # Load data
        cur = app.conn.cursor()
        try:
            cur.execute("""INSERT INTO measure_type_series_oplog (measure_type_series_id, validity_start_date,
            validity_end_date, measure_type_combination, operation, operation_date)
            VALUES (%s, %s, %s, %s, %s, %s)""",
            (measure_type_series_id, validity_start_date, validity_end_date, measure_type_combination, operation, operation_date))
            app.conn.commit()
        except:
            g.app.record_business_rule_violation("DB", "DB failure", operation, transaction_id, message_id, record_code, sub_record_code, measure_type_series_id)
        cur.close()
