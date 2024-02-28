with stg_1 as
(
select
mmt.person_id as person_id,
mmt.visit_occurrence_id as visit_occurrence_id,
mmt.measurement_datetime as measurement_datetime,
mmt.measurement_concept_id as measurement_concept_id,
mmt.value_as_number as value_as_number
from
__schema_name__.cdm_measurement mmt
where load_table_id = 'chartevents'
)
, stg_2 AS
(
SELECT
    person_id
    , visit_occurrence_id
    , value_as_number
    , measurement_concept_id
    , measurement_datetime::date as measurement_date
FROM stg_1
)
, stg_3 AS
(
SELECT
    person_id
    , visit_occurrence_id
    , measurement_date
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '431314004' THEN value_as_number ELSE null END)) AS "Peripheral oxygen saturation"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '86290005' THEN value_as_number ELSE null END)) AS "Respiratory rate"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '246508008' THEN value_as_number ELSE null END)) AS "Temperature"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '271649006' THEN value_as_number ELSE null END)) AS "Systolic blood pressure"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '271650006' THEN value_as_number ELSE null END)) AS "Diastolic blood pressure"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '6797001' THEN value_as_number ELSE null END)) AS "Mean blood pressure"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '364075005' THEN value_as_number ELSE null END)) AS "Heart rate"
FROM stg_2
GROUP BY visit_occurrence_id, person_id, measurement_date
)
SELECT * FROM stg_3
;
