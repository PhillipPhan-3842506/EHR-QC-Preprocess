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
where load_table_id = 'labevents'
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
, COALESCE(MAX(CASE WHEN measurement_concept_id = '1020681000000107' THEN value_as_number ELSE null END)) AS "Sodium level"
, COALESCE(MAX(CASE WHEN measurement_concept_id = '72341003' THEN value_as_number ELSE null END)) AS "Blood urea nitrogen"
, COALESCE(MAX(CASE WHEN measurement_concept_id = '1032061000000108' THEN value_as_number ELSE null END)) AS "Creatinine level"
, COALESCE(MAX(CASE WHEN measurement_concept_id = '1028441000000104' THEN value_as_number ELSE null END)) AS "Potassium level"
, COALESCE(MAX(CASE WHEN measurement_concept_id = '50213009' THEN value_as_number ELSE null END)) AS "Chloride"
, COALESCE(MAX(CASE WHEN measurement_concept_id = '142829008' THEN value_as_number ELSE null END)) AS "Hematocrit"
, COALESCE(MAX(CASE WHEN measurement_concept_id = '1022431000000105' THEN value_as_number ELSE null END)) AS "Haemoglobin estimation"
, COALESCE(MAX(CASE WHEN measurement_concept_id = '1022651000000100' THEN value_as_number ELSE null END)) AS "Platelet count"
, COALESCE(MAX(CASE WHEN measurement_concept_id = '14089001' THEN value_as_number ELSE null END)) AS "Red blood cell count"
, COALESCE(MAX(CASE WHEN measurement_concept_id = '1028041000000107' THEN value_as_number ELSE null END)) AS "Calcium level"
, COALESCE(MAX(CASE WHEN measurement_concept_id = '1022491000000106' THEN value_as_number ELSE null END)) AS "MCV - Mean corpuscular volume"
, COALESCE(MAX(CASE WHEN measurement_concept_id = '1022481000000109' THEN value_as_number ELSE null END)) AS "MCHC - Mean corpuscular haemoglobin concentration"
, COALESCE(MAX(CASE WHEN measurement_concept_id = '52454007' THEN value_as_number ELSE null END)) AS "Albumin"
, COALESCE(MAX(CASE WHEN measurement_concept_id = '1022471000000107' THEN value_as_number ELSE null END)) AS "MCH - Mean corpuscular haemoglobin"
, COALESCE(MAX(CASE WHEN measurement_concept_id = '1000621000000104' THEN value_as_number ELSE null END)) AS "Serum alkaline phosphatase level"
, COALESCE(MAX(CASE WHEN measurement_concept_id = '45896001' THEN value_as_number ELSE null END)) AS "Aspartate aminotransferase measurement"
, COALESCE(MAX(CASE WHEN measurement_concept_id = '56935002' THEN value_as_number ELSE null END)) AS "Alanine aminotransferase"
, COALESCE(MAX(CASE WHEN measurement_concept_id = '1026761000000106' THEN value_as_number ELSE null END)) AS "Total bilirubin level"
, COALESCE(MAX(CASE WHEN measurement_concept_id = '767002' THEN value_as_number ELSE null END)) AS "White blood cell count"
, COALESCE(MAX(CASE WHEN measurement_concept_id = '993501000000105' THEN value_as_number ELSE null END)) AS "Red blood cell distribution width"
, COALESCE(MAX(CASE WHEN measurement_concept_id = '304383000' THEN value_as_number ELSE null END)) AS "Total protein measurement"
, COALESCE(MAX(CASE WHEN measurement_concept_id = '1018851000000108' THEN value_as_number ELSE null END)) AS "Glucose level"
, COALESCE(MAX(CASE WHEN measurement_concept_id = '1032311000000109' THEN value_as_number ELSE null END)) AS "Bicarbonate level"
, COALESCE(MAX(CASE WHEN measurement_concept_id = '102637000' THEN value_as_number ELSE null END)) AS "Anion gap"
, COALESCE(MAX(CASE WHEN measurement_concept_id = '83036002' THEN value_as_number ELSE null END)) AS "Lactate"
, COALESCE(MAX(CASE WHEN measurement_concept_id = '38000004' THEN value_as_number ELSE null END)) AS "Lymph"
, COALESCE(MAX(CASE WHEN measurement_concept_id = '993571000000102' THEN value_as_number ELSE null END)) AS "Infectious mononucleosis test"
    FROM stg_2
GROUP BY visit_occurrence_id, person_id, measurement_date
)
SELECT * FROM stg_3
;
