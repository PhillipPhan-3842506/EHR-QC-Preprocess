with stg_1 as
(
select
mmt.person_id as person_id,
mmt.visit_occurrence_id as visit_occurrence_id,
measurement_datetime::date as measurement_date,
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
    , measurement_date
    , row_number() over (partition by person_id, visit_occurrence_id, measurement_concept_id, measurement_date order by measurement_datetime ASC) as rn
FROM stg_1
)
, stg_3 AS
(
SELECT
    person_id
    , visit_occurrence_id
    , measurement_date
    , rn
    , COALESCE(MAX(CASE WHEN measurement_concept_id = '431314004' THEN value_as_number ELSE null END)) AS "Peripheral oxygen saturation"
    , COALESCE(MAX(CASE WHEN measurement_concept_id = '86290005' THEN value_as_number ELSE null END)) AS "Respiratory rate"
    , COALESCE(MAX(CASE WHEN measurement_concept_id = '246508008' THEN value_as_number ELSE null END)) AS "Temperature"
    , COALESCE(MAX(CASE WHEN measurement_concept_id = '271649006' THEN value_as_number ELSE null END)) AS "Systolic blood pressure"
    , COALESCE(MAX(CASE WHEN measurement_concept_id = '271650006' THEN value_as_number ELSE null END)) AS "Diastolic blood pressure"
    , COALESCE(MAX(CASE WHEN measurement_concept_id = '6797001' THEN value_as_number ELSE null END)) AS "Mean blood pressure"
    , COALESCE(MAX(CASE WHEN measurement_concept_id = '364075005' THEN value_as_number ELSE null END)) AS "Heart rate"
FROM stg_2
GROUP BY person_id, visit_occurrence_id, measurement_date, rn
)
SELECT
DISTINCT ON (person_id, visit_occurrence_id, measurement_date)
person_id
, visit_occurrence_id
, measurement_date
, "Peripheral oxygen saturation"
, "Respiratory rate"
, "Temperature"
, "Systolic blood pressure"
, "Diastolic blood pressure"
, "Mean blood pressure"
, "Heart rate"
FROM
stg_3
ORDER BY person_id, visit_occurrence_id, measurement_date, rn ASC NULLS LAST
;
