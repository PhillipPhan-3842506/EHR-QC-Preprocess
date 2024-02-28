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
    person_id, visit_occurrence_id, value_as_number, measurement_concept_id
    , ROW_NUMBER() OVER (PARTITION BY person_id, visit_occurrence_id, measurement_concept_id ORDER BY measurement_datetime) AS rn
FROM stg_1
)
, stg_3 AS
(
SELECT
    person_id
    , visit_occurrence_id
    , rn
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '767002' THEN value_as_number ELSE null END)) AS "White blood cell count"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '1022571000000108' THEN value_as_number ELSE null END)) AS "Basophil count"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '68615006' THEN value_as_number ELSE null END)) AS "Bicarbonate"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '1028281000000106' THEN value_as_number ELSE null END)) AS "Blood urea"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '50213009' THEN value_as_number ELSE null END)) AS "Chloride salt"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '15373003' THEN value_as_number ELSE null END)) AS "Creatinine"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '71960002' THEN value_as_number ELSE null END)) AS "Eosinophil count"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '1022431000000105' THEN value_as_number ELSE null END)) AS "Haemoglobin estimation"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '165418001' THEN value_as_number ELSE null END)) AS "Hematocrit"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '56972008' THEN value_as_number ELSE null END)) AS "Lymphocyte"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '1022481000000109' THEN value_as_number ELSE null END)) AS "MCHC - Mean corpuscular haemoglobin concentration"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '1022471000000107' THEN value_as_number ELSE null END)) AS "MCH - Mean corpuscular haemoglobin"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '1022491000000106' THEN value_as_number ELSE null END)) AS "MCV - Mean corpuscular volume"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '55918008' THEN value_as_number ELSE null END)) AS "Monocyte"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '30630007' THEN value_as_number ELSE null END)) AS "Neutrophil count"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '88480006' THEN value_as_number ELSE null END)) AS "Potassium"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '14089001' THEN value_as_number ELSE null END)) AS "Red blood cell count"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '993501000000105' THEN value_as_number ELSE null END)) AS "Red blood cell distribution width"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '39972003' THEN value_as_number ELSE null END)) AS "Sodium"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '16378004' THEN value_as_number ELSE null END)) AS "Platelet"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '56935002' THEN value_as_number ELSE null END)) AS "Alanine aminotransferase"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '1028091000000102' THEN value_as_number ELSE null END)) AS "GGT (gamma-glutamyl transferase) level"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '52454007' THEN value_as_number ELSE null END)) AS "Albumin"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '79706000' THEN value_as_number ELSE null END)) AS "Bilirubin"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '997611000000101' THEN value_as_number ELSE null END)) AS "Total alkaline phosphatase level"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '304383000' THEN value_as_number ELSE null END)) AS "Total protein measurement"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '81905004' THEN value_as_number ELSE null END)) AS "Globulin"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '5540006' THEN value_as_number ELSE null END)) AS "Calcium"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '104866001' THEN value_as_number ELSE null END)) AS "Phosphate measurement"
    , COALESCE(AVG(CASE WHEN measurement_concept_id = '38151008' THEN value_as_number ELSE null END)) AS "Magnesium measurement"
    FROM stg_2
GROUP BY person_id, visit_occurrence_id, rn
)
, stg_4 AS
(
    SELECT
    person_id
    , visit_occurrence_id
    , avg("White blood cell count") AS "White blood cell count"
    , avg("Basophil count") AS "Basophil count"
    , avg("Bicarbonate") AS "Bicarbonate"
    , avg("Blood urea") AS "Blood urea"
    , avg("Chloride salt") AS "Chloride salt"
    , avg("Creatinine") AS "Creatinine"
    , avg("Eosinophil count") AS "Eosinophil count"
    , avg("Haemoglobin estimation") AS "Haemoglobin estimation"
    , avg("Hematocrit") AS "Hematocrit"
    , avg("Lymphocyte") AS "Lymphocyte"
    , avg("MCHC - Mean corpuscular haemoglobin concentration") AS "MCHC - Mean corpuscular haemoglobin concentration"
    , avg("MCH - Mean corpuscular haemoglobin") AS "MCH - Mean corpuscular haemoglobin"
    , avg("MCV - Mean corpuscular volume") AS "MCV - Mean corpuscular volume"
    , avg("Monocyte") AS "Monocyte"
    , avg("Neutrophil count") AS "Neutrophil count"
    , avg("Potassium") AS "Potassium"
    , avg("Red blood cell count") AS "Red blood cell count"
    , avg("Red blood cell distribution width") AS "Red blood cell distribution width"
    , avg("Sodium") AS "Sodium"
    , avg("Platelet") AS "Platelet"
    , avg("Alanine aminotransferase") AS "Alanine aminotransferase"
    , avg("GGT (gamma-glutamyl transferase) level") AS "GGT (gamma-glutamyl transferase) level"
    , avg("Albumin") AS "Albumin"
    , avg("Bilirubin") AS "Bilirubin"
    , avg("Total alkaline phosphatase level") AS "Total alkaline phosphatase level"
    , avg("Total protein measurement") AS "Total protein measurement"
    , avg("Globulin") AS "Globulin"
    , avg("Calcium") AS "Calcium"
    , avg("Phosphate measurement") AS "Phosphate measurement"
    , avg("Magnesium measurement") AS "Magnesium measurement"
    FROM stg_3
    GROUP BY person_id, visit_occurrence_id
)
SELECT * FROM stg_4
;
