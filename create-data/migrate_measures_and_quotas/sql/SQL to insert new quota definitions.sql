-- Get all quota definitions
SELECT quota_definition_sid, quota_order_number_id, validity_start_date, validity_end_date, quota_order_number_sid, volume,
initial_volume, measurement_unit_code, maximum_precision, critical_state, critical_threshold, monetary_unit_code, 
measurement_unit_qualifier_code, description
FROM quota_definitions
WHERE validity_start_date >= '2018-01-01'
AND quota_order_number_id = '091104'
ORDER BY quota_order_number_id, validity_start_date;


-- Get all quota order numbers
SELECT quota_order_number_sid, quota_order_number_id, validity_start_date, validity_end_date FROM quota_order_numbers
WHERE (validity_end_date IS NULL OR validity_end_date  >= '2019-03-30')
AND quota_order_number_id = '090782'
ORDER BY 2


SELECT * FROM measures m WHERE ordernumber IN ('x091104', 'x091193', '092202') AND validity_end_date > CURRENT_DATE

SELECT * FROM quota_definitions WHERE quota_definition_sid = 14308
DELETE FROM ml.import_files

SELECT DISTINCT main_quota_definition_sid, sub_quota_definition_sid, relation_type, coefficient FROM quota_associations
WHERE main_quota_definition_sid IN (SELECT quota_definition_sid FROM quota_definitions WHERE validity_start_date >= '2018-01-01')
OR sub_quota_definition_sid IN (SELECT quota_definition_sid FROM quota_definitions WHERE validity_start_date >= '2018-01-01')