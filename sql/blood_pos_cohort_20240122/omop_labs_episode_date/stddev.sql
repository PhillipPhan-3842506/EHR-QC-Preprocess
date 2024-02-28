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
    , COALESCE(STDDEV(CASE WHEN measurement_concept_id = '767002' THEN value_as_number ELSE null END)) AS "White blood cell count"
    , COALESCE(STDDEV(CASE WHEN measurement_concept_id = '1022571000000108' THEN value_as_number ELSE null END)) AS "Basophil count"
    , COALESCE(STDDEV(CASE WHEN measurement_concept_id = '68615006' THEN value_as_number ELSE null END)) AS "Bicarbonate"
    , COALESCE(STDDEV(CASE WHEN measurement_concept_id = '1028281000000106' THEN value_as_number ELSE null END)) AS "Blood urea"
    , COALESCE(STDDEV(CASE WHEN measurement_concept_id = '50213009' THEN value_as_number ELSE null END)) AS "Chloride salt"
    , COALESCE(STDDEV(CASE WHEN measurement_concept_id = '15373003' THEN value_as_number ELSE null END)) AS "Creatinine"
    , COALESCE(STDDEV(CASE WHEN measurement_concept_id = '71960002' THEN value_as_number ELSE null END)) AS "Eosinophil count"
    , COALESCE(STDDEV(CASE WHEN measurement_concept_id = '1022431000000105' THEN value_as_number ELSE null END)) AS "Haemoglobin estimation"
    , COALESCE(STDDEV(CASE WHEN measurement_concept_id = '165418001' THEN value_as_number ELSE null END)) AS "Hematocrit"
    , COALESCE(STDDEV(CASE WHEN measurement_concept_id = '56972008' THEN value_as_number ELSE null END)) AS "Lymphocyte"
    , COALESCE(STDDEV(CASE WHEN measurement_concept_id = '1022481000000109' THEN value_as_number ELSE null END)) AS "MCHC - Mean corpuscular haemoglobin concentration"
    , COALESCE(STDDEV(CASE WHEN measurement_concept_id = '1022471000000107' THEN value_as_number ELSE null END)) AS "MCH - Mean corpuscular haemoglobin"
    , COALESCE(STDDEV(CASE WHEN measurement_concept_id = '1022491000000106' THEN value_as_number ELSE null END)) AS "MCV - Mean corpuscular volume"
    , COALESCE(STDDEV(CASE WHEN measurement_concept_id = '55918008' THEN value_as_number ELSE null END)) AS "Monocyte"
    , COALESCE(STDDEV(CASE WHEN measurement_concept_id = '30630007' THEN value_as_number ELSE null END)) AS "Neutrophil count"
    , COALESCE(STDDEV(CASE WHEN measurement_concept_id = '88480006' THEN value_as_number ELSE null END)) AS "Potassium"
    , COALESCE(STDDEV(CASE WHEN measurement_concept_id = '14089001' THEN value_as_number ELSE null END)) AS "Red blood cell count"
    , COALESCE(STDDEV(CASE WHEN measurement_concept_id = '993501000000105' THEN value_as_number ELSE null END)) AS "Red blood cell distribution width"
    , COALESCE(STDDEV(CASE WHEN measurement_concept_id = '39972003' THEN value_as_number ELSE null END)) AS "Sodium"
    , COALESCE(STDDEV(CASE WHEN measurement_concept_id = '16378004' THEN value_as_number ELSE null END)) AS "Platelet"
    , COALESCE(STDDEV(CASE WHEN measurement_concept_id = '56935002' THEN value_as_number ELSE null END)) AS "Alanine aminotransferase"
    , COALESCE(STDDEV(CASE WHEN measurement_concept_id = '1028091000000102' THEN value_as_number ELSE null END)) AS "GGT (gamma-glutamyl transferase) level"
    , COALESCE(STDDEV(CASE WHEN measurement_concept_id = '52454007' THEN value_as_number ELSE null END)) AS "Albumin"
    , COALESCE(STDDEV(CASE WHEN measurement_concept_id = '79706000' THEN value_as_number ELSE null END)) AS "Bilirubin"
    , COALESCE(STDDEV(CASE WHEN measurement_concept_id = '997611000000101' THEN value_as_number ELSE null END)) AS "Total alkaline phosphatase level"
    , COALESCE(STDDEV(CASE WHEN measurement_concept_id = '304383000' THEN value_as_number ELSE null END)) AS "Total protein measurement"
    , COALESCE(STDDEV(CASE WHEN measurement_concept_id = '81905004' THEN value_as_number ELSE null END)) AS "Globulin"
    , COALESCE(STDDEV(CASE WHEN measurement_concept_id = '5540006' THEN value_as_number ELSE null END)) AS "Calcium"
    , COALESCE(STDDEV(CASE WHEN measurement_concept_id = '104866001' THEN value_as_number ELSE null END)) AS "Phosphate measurement"
    , COALESCE(STDDEV(CASE WHEN measurement_concept_id = '38151008' THEN value_as_number ELSE null END)) AS "Magnesium measurement"
    FROM stg_2
GROUP BY visit_occurrence_id, person_id, measurement_date
)
SELECT * FROM stg_3
;
