SELECT
person_id,
visit_occurrence_id,
visit_start_date,
CASE WHEN discharge_to_source_value = 'Death' THEN 1 ELSE 0 END
FROM
__schema_name__.cdm_visit_occurrence
;
