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
omop_migration_etl_20231021.cdm_measurement mmt
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
    , COALESCE(MIN(CASE WHEN measurement_concept_id = '767002' THEN value_as_number ELSE null END)) AS "White blood cell count"
    , COALESCE(MIN(CASE WHEN measurement_concept_id = '1022571000000108' THEN value_as_number ELSE null END)) AS "Basophil count"
    , COALESCE(MIN(CASE WHEN measurement_concept_id = '68615006' THEN value_as_number ELSE null END)) AS "Bicarbonate"
    , COALESCE(MIN(CASE WHEN measurement_concept_id = '1028281000000106' THEN value_as_number ELSE null END)) AS "Blood urea"
    , COALESCE(MIN(CASE WHEN measurement_concept_id = '50213009' THEN value_as_number ELSE null END)) AS "Chloride salt"
    , COALESCE(MIN(CASE WHEN measurement_concept_id = '15373003' THEN value_as_number ELSE null END)) AS "Creatinine"
    , COALESCE(MIN(CASE WHEN measurement_concept_id = '71960002' THEN value_as_number ELSE null END)) AS "Eosinophil count"
    , COALESCE(MIN(CASE WHEN measurement_concept_id = '1022431000000105' THEN value_as_number ELSE null END)) AS "Haemoglobin estimation"
    , COALESCE(MIN(CASE WHEN measurement_concept_id = '165418001' THEN value_as_number ELSE null END)) AS "Hematocrit"
    , COALESCE(MIN(CASE WHEN measurement_concept_id = '56972008' THEN value_as_number ELSE null END)) AS "Lymphocyte"
    , COALESCE(MIN(CASE WHEN measurement_concept_id = '1022481000000109' THEN value_as_number ELSE null END)) AS "MCHC - Mean corpuscular haemoglobin concentration"
    , COALESCE(MIN(CASE WHEN measurement_concept_id = '1022471000000107' THEN value_as_number ELSE null END)) AS "MCH - Mean corpuscular haemoglobin"
    , COALESCE(MIN(CASE WHEN measurement_concept_id = '1022491000000106' THEN value_as_number ELSE null END)) AS "MCV - Mean corpuscular volume"
    , COALESCE(MIN(CASE WHEN measurement_concept_id = '55918008' THEN value_as_number ELSE null END)) AS "Monocyte"
    , COALESCE(MIN(CASE WHEN measurement_concept_id = '30630007' THEN value_as_number ELSE null END)) AS "Neutrophil count"
    , COALESCE(MIN(CASE WHEN measurement_concept_id = '88480006' THEN value_as_number ELSE null END)) AS "Potassium"
    , COALESCE(MIN(CASE WHEN measurement_concept_id = '14089001' THEN value_as_number ELSE null END)) AS "Red blood cell count"
    , COALESCE(MIN(CASE WHEN measurement_concept_id = '993501000000105' THEN value_as_number ELSE null END)) AS "Red blood cell distribution width"
    , COALESCE(MIN(CASE WHEN measurement_concept_id = '39972003' THEN value_as_number ELSE null END)) AS "Sodium"
    , COALESCE(MIN(CASE WHEN measurement_concept_id = '16378004' THEN value_as_number ELSE null END)) AS "Platelet"
    , COALESCE(MIN(CASE WHEN measurement_concept_id = '56935002' THEN value_as_number ELSE null END)) AS "Alanine aminotransferase"
    , COALESCE(MIN(CASE WHEN measurement_concept_id = '1028091000000102' THEN value_as_number ELSE null END)) AS "GGT (gamma-glutamyl transferase) level"
    , COALESCE(MIN(CASE WHEN measurement_concept_id = '52454007' THEN value_as_number ELSE null END)) AS "Albumin"
    , COALESCE(MIN(CASE WHEN measurement_concept_id = '79706000' THEN value_as_number ELSE null END)) AS "Bilirubin"
    , COALESCE(MIN(CASE WHEN measurement_concept_id = '997611000000101' THEN value_as_number ELSE null END)) AS "Total alkaline phosphatase level"
    , COALESCE(MIN(CASE WHEN measurement_concept_id = '304383000' THEN value_as_number ELSE null END)) AS "Total protein measurement"
    , COALESCE(MIN(CASE WHEN measurement_concept_id = '81905004' THEN value_as_number ELSE null END)) AS "Globulin"
    , COALESCE(MIN(CASE WHEN measurement_concept_id = '5540006' THEN value_as_number ELSE null END)) AS "Calcium"
    , COALESCE(MIN(CASE WHEN measurement_concept_id = '104866001' THEN value_as_number ELSE null END)) AS "Phosphate measurement"
    , COALESCE(MIN(CASE WHEN measurement_concept_id = '38151008' THEN value_as_number ELSE null END)) AS "Magnesium measurement"
FROM stg_2
GROUP BY person_id, visit_occurrence_id, measurement_date, rn
)
SELECT
DISTINCT ON (person_id, visit_occurrence_id, measurement_date)
person_id
, visit_occurrence_id
, measurement_date
, "White blood cell count"
, "Basophil count"
, "Bicarbonate"
, "Blood urea"
, "Chloride salt"
, "Creatinine"
, "Eosinophil count"
, "Haemoglobin estimation"
, "Hematocrit"
, "Lymphocyte"
, "MCHC - Mean corpuscular haemoglobin concentration"
, "MCH - Mean corpuscular haemoglobin"
, "MCV - Mean corpuscular volume"
, "Monocyte"
, "Neutrophil count"
, "Potassium"
, "Red blood cell count"
, "Red blood cell distribution width"
, "Sodium"
, "Platelet"
, "Alanine aminotransferase"
, "GGT (gamma-glutamyl transferase) level"
, "Albumin"
, "Bilirubin"
, "Total alkaline phosphatase level"
, "Total protein measurement"
, "Globulin"
, "Calcium"
, "Phosphate measurement"
, "Magnesium measurement"
FROM
stg_3
ORDER BY person_id, visit_occurrence_id, measurement_date, rn ASC NULLS LAST
;
