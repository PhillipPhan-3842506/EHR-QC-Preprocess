with vitals_stg_1 as
(
select
mmt.person_id as person_id,
mmt.measurement_datetime as measurement_datetime,
mmt.measurement_concept_id as measurement_concept_id,
mmt.value_as_number as value_as_number
from
__schema_name__.cdm_measurement mmt
where load_table_id = 'chartevents'
)
, vitals_stg_2 AS
(
SELECT
    person_id, value_as_number, measurement_concept_id
    , ROW_NUMBER() OVER (PARTITION BY person_id, measurement_concept_id ORDER BY measurement_datetime) AS rn
FROM vitals_stg_1
)
, vitals_stg_3 AS
(
SELECT
    person_id
    , rn
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '86290005' THEN value_as_number ELSE null END)) AS respiratory_rate
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '927981000000106' THEN value_as_number ELSE null END)) AS spo2
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '386725007' THEN value_as_number ELSE null END)) AS temperature
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '364075005' THEN value_as_number ELSE null END)) AS heart_rate
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '271649006' THEN value_as_number ELSE null END)) AS systolic_bp
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '271650006' THEN value_as_number ELSE null END)) AS diastolic_bp
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '6797001' THEN value_as_number ELSE null END)) AS mean_arterial_pressure
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '427081008' THEN value_as_number ELSE null END)) AS o2_flow_rate
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '8499008' THEN value_as_number ELSE null END)) AS pulse_rate
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '250774007' THEN value_as_number ELSE null END)) AS fio2
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '27113001' THEN value_as_number ELSE null END)) AS weight
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '71420008' THEN value_as_number ELSE null END)) AS central_venous_pressure
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '60621009' THEN value_as_number ELSE null END)) AS bmi
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '1153637007' THEN value_as_number ELSE null END)) AS height
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '165077006' THEN value_as_number ELSE null END)) AS intracranial_pressure
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '250846007' THEN value_as_number ELSE null END)) AS cerebral_perfusion_pressure
FROM vitals_stg_2
GROUP BY person_id, rn
)
, vitals_stg_4 AS
(
    SELECT
    person_id
    , avg(respiratory_rate) AS respiratory_rate
    , avg(spo2) AS spo2
    , avg(temperature) AS temperature
    , avg(heart_rate) AS heart_rate
    , avg(systolic_bp) AS systolic_bp
    , avg(diastolic_bp) AS diastolic_bp
    , avg(mean_arterial_pressure) AS mean_arterial_pressure
    , avg(o2_flow_rate) AS o2_flow_rate
    , avg(pulse_rate) AS pulse_rate
    , avg(fio2) AS fio2
    , avg(weight) AS weight
    , avg(central_venous_pressure) AS central_venous_pressure
    , avg(bmi) AS bmi
    , avg(height) AS height
    , avg(intracranial_pressure) AS intracranial_pressure
    , avg(cerebral_perfusion_pressure) AS cerebral_perfusion_pressure
    FROM vitals_stg_3
    GROUP BY person_id
)
SELECT * FROM vitals_stg_4
;
