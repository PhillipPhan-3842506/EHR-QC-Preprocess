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
inner join omop_test_20220817.cohort_icd coh
on coh.person_id = mmt.person_id and coh.visit_occurrence_id = mmt.visit_occurrence_id
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
, COALESCE(AVG(CASE WHEN measurement_concept_id IN ('3024171', '3007469', '1175625') THEN value_as_number ELSE null END)) AS "Respiratory rate"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3027018' THEN value_as_number ELSE null END)) AS "Heart rate"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3025315' THEN value_as_number ELSE null END)) AS "Body weight"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '40762499' THEN value_as_number ELSE null END)) AS "Oxygen saturation in Arterial blood by Pulse oximetry"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3016335' THEN value_as_number ELSE null END)) AS "Glasgow coma score eye opening"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3009094' THEN value_as_number ELSE null END)) AS "Glasgow coma score verbal"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3008223' THEN value_as_number ELSE null END)) AS "Glasgow coma score motor"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3035206' THEN value_as_number ELSE null END)) AS "Physical mobility Braden scale"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3035816' THEN value_as_number ELSE null END)) AS "Nutrition intake pattern Braden scale"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3036098' THEN value_as_number ELSE null END)) AS "Sensory perception Braden scale"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3037318' THEN value_as_number ELSE null END)) AS "Physical activity Braden scale"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3037022' THEN value_as_number ELSE null END)) AS "Moisture exposure Braden scale"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3037347' THEN value_as_number ELSE null END)) AS "Friction and shear Braden scale"
, COALESCE(AVG(CASE WHEN measurement_concept_id IN ('21492241', '3027598') THEN value_as_number ELSE null END)) AS "Mean blood pressure"
, COALESCE(AVG(CASE WHEN measurement_concept_id IN ('21492239', '3004249') THEN value_as_number ELSE null END)) AS "Systolic blood pressure"
, COALESCE(AVG(CASE WHEN measurement_concept_id IN ('3012888', '21492240') THEN value_as_number ELSE null END)) AS "Diastolic blood pressure"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3020891' THEN value_as_number ELSE null END)) AS "Body temperature"
FROM stg_2
GROUP BY person_id, visit_occurrence_id, measurement_date, rn
)
SELECT
DISTINCT ON (person_id, visit_occurrence_id, measurement_date)
person_id
, visit_occurrence_id
, measurement_date
, "Respiratory rate"
, "Heart rate"
, "Body weight"
, "Oxygen saturation in Arterial blood by Pulse oximetry"
, "Glasgow coma score eye opening"
, "Glasgow coma score verbal"
, "Glasgow coma score motor"
, "Physical mobility Braden scale"
, "Nutrition intake pattern Braden scale"
, "Sensory perception Braden scale"
, "Physical activity Braden scale"
, "Moisture exposure Braden scale"
, "Friction and shear Braden scale"
, "Mean blood pressure"
, "Systolic blood pressure"
, "Diastolic blood pressure"
, "Body temperature"
FROM
stg_3
ORDER BY person_id, visit_occurrence_id, measurement_date, rn ASC NULLS LAST
;
