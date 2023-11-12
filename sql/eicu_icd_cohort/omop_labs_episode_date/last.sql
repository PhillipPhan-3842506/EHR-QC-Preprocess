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
where load_table_id = 'labevents'
)
, stg_2 AS
(
SELECT
    person_id
    , visit_occurrence_id
    , value_as_number
    , measurement_concept_id
    , measurement_date
    , row_number() over (partition by person_id, visit_occurrence_id, measurement_concept_id, measurement_date order by measurement_datetime DESC) as rn
FROM stg_1
)
, stg_3 AS
(
SELECT
    person_id
    , visit_occurrence_id
    , measurement_date
    , rn
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '1020681000000107' THEN value_as_number ELSE null END)) AS "Sodium level"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '72341003' THEN value_as_number ELSE null END)) AS "Blood urea nitrogen"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '1032061000000108' THEN value_as_number ELSE null END)) AS "Creatinine level"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '1028441000000104' THEN value_as_number ELSE null END)) AS "Potassium level"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '50213009' THEN value_as_number ELSE null END)) AS "Chloride"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '142829008' THEN value_as_number ELSE null END)) AS "Hematocrit"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '1022431000000105' THEN value_as_number ELSE null END)) AS "Haemoglobin estimation"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '1022651000000100' THEN value_as_number ELSE null END)) AS "Platelet count"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '14089001' THEN value_as_number ELSE null END)) AS "Red blood cell count"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '1028041000000107' THEN value_as_number ELSE null END)) AS "Calcium level"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '1022491000000106' THEN value_as_number ELSE null END)) AS "MCV - Mean corpuscular volume"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '1022481000000109' THEN value_as_number ELSE null END)) AS "MCHC - Mean corpuscular haemoglobin concentration"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '52454007' THEN value_as_number ELSE null END)) AS "Albumin"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '1022471000000107' THEN value_as_number ELSE null END)) AS "MCH - Mean corpuscular haemoglobin"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '1000621000000104' THEN value_as_number ELSE null END)) AS "Serum alkaline phosphatase level"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '45896001' THEN value_as_number ELSE null END)) AS "Aspartate aminotransferase measurement"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '56935002' THEN value_as_number ELSE null END)) AS "Alanine aminotransferase"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '1026761000000106' THEN value_as_number ELSE null END)) AS "Total bilirubin level"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '767002' THEN value_as_number ELSE null END)) AS "White blood cell count"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '993501000000105' THEN value_as_number ELSE null END)) AS "Red blood cell distribution width"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '304383000' THEN value_as_number ELSE null END)) AS "Total protein measurement"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '1018851000000108' THEN value_as_number ELSE null END)) AS "Glucose level"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '1032311000000109' THEN value_as_number ELSE null END)) AS "Bicarbonate level"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '102637000' THEN value_as_number ELSE null END)) AS "Anion gap"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '83036002' THEN value_as_number ELSE null END)) AS "Lactate"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '38000004' THEN value_as_number ELSE null END)) AS "Lymph"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '993571000000102' THEN value_as_number ELSE null END)) AS "Infectious mononucleosis test"
FROM stg_2
GROUP BY person_id, visit_occurrence_id, measurement_date, rn
)
SELECT
DISTINCT ON (person_id, visit_occurrence_id, measurement_date)
person_id
, visit_occurrence_id
, measurement_date
, "Sodium level"
, "Blood urea nitrogen"
, "Creatinine level"
, "Potassium level"
, "Chloride"
, "Hematocrit"
, "Haemoglobin estimation"
, "Platelet count"
, "Red blood cell count"
, "Calcium level"
, "MCV - Mean corpuscular volume"
, "MCHC - Mean corpuscular haemoglobin concentration"
, "Albumin"
, "MCH - Mean corpuscular haemoglobin"
, "Serum alkaline phosphatase level"
, "Aspartate aminotransferase measurement"
, "Alanine aminotransferase"
, "Total bilirubin level"
, "White blood cell count"
, "Red blood cell distribution width"
, "Total protein measurement"
, "Glucose level"
, "Bicarbonate level"
, "Anion gap"
, "Lactate"
, "Lymph"
, "Infectious mononucleosis test"
FROM
stg_3
ORDER BY person_id, visit_occurrence_id, measurement_date, rn ASC NULLS LAST
;
