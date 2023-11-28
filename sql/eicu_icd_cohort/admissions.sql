SELECT
person_id,
visit_occurrence_id,
visit_start_date,
CASE WHEN visit_source_value = 'Expired' THEN 1 ELSE 0 END as Death
FROM
__schema_name__.cdm_visit_occurrence
;
