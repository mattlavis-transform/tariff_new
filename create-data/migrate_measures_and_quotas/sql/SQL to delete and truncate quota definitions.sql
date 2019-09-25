SELECT * FROM quota_order_numbers WHERE validity_end_date IS NULL
ORDER BY quota_order_number_id

-- Get and kill all future associations
SELECT DISTINCT main_quota_definition_sid, sub_quota_definition_sid, relation_type, coefficient FROM quota_associations
WHERE
main_quota_definition_sid IN (SELECT quota_definition_sid FROM quota_definitions WHERE validity_start_date >= '2019-03-30')
OR 
sub_quota_definition_sid IN (SELECT quota_definition_sid FROM quota_definitions WHERE validity_start_date >= '2019-03-30')
ORDER BY 1, 2

-- Get and kill all future suspension events
SELECT quota_suspension_period_sid, quota_definition_sid, suspension_start_date, suspension_end_date
FROM quota_suspension_periods
WHERE quota_definition_sid IN (SELECT quota_definition_sid FROM quota_definitions WHERE validity_start_date >= '2019-03-30')


-- Get and kill all future definitions
SELECT quota_definition_sid, quota_order_number_id, validity_start_date, validity_end_date, quota_order_number_sid, volume,
initial_volume, measurement_unit_code, maximum_precision, critical_state, critical_threshold, monetary_unit_code, 
measurement_unit_qualifier_code, description
FROM quota_definitions WHERE validity_start_date >= '2019-03-30'
ORDER BY quota_order_number_id



SELECT * FROM quota_unsuspension_events WHERE quota_definition_sid IN (SELECT quota_definition_sid FROM quota_definitions WHERE validity_start_date >= '2019-03-30')
SELECT * FROM quota_balance_events WHERE quota_definition_sid IN (SELECT quota_definition_sid FROM quota_definitions WHERE validity_start_date >= '2019-03-30')



-- Get all straddling definitions
-- Get and kill all future definitions
SELECT quota_definition_sid, quota_order_number_id, validity_start_date, validity_end_date, quota_order_number_sid, volume,
initial_volume, measurement_unit_code, maximum_precision, critical_state, critical_threshold, monetary_unit_code, 
measurement_unit_qualifier_code, description
FROM quota_definitions WHERE validity_start_date <= '2019-03-29' AND validity_end_date >= '2019-03-30'
ORDER BY quota_order_number_id

