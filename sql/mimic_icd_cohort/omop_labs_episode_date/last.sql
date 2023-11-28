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
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3003282' THEN value_as_number ELSE null END)) AS "Leukocytes in Blood by Manual count"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3023314' THEN value_as_number ELSE null END)) AS "Hematocrit of Blood by Automated count"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3009744' THEN value_as_number ELSE null END)) AS "MCHC by Automated count"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3012030' THEN value_as_number ELSE null END)) AS "MCH by Automated count"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3019897' THEN value_as_number ELSE null END)) AS "Erythrocyte distribution width by Automated count"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3023599' THEN value_as_number ELSE null END)) AS "MCV by Automated count"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3020416' THEN value_as_number ELSE null END)) AS "Erythrocytes in Blood by Automated count"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3024929' THEN value_as_number ELSE null END)) AS "Platelets in Blood by Automated count"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3000963' THEN value_as_number ELSE null END)) AS "Hemoglobin in Blood"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3013682' THEN value_as_number ELSE null END)) AS "Urea nitrogen in Serum or Plasma"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3016723' THEN value_as_number ELSE null END)) AS "Creatinine in Serum or Plasma"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3023103' THEN value_as_number ELSE null END)) AS "Potassium in Serum or Plasma"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3014576' THEN value_as_number ELSE null END)) AS "Chloride in Serum or Plasma"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3016293' THEN value_as_number ELSE null END)) AS "Bicarbonate in Serum or Plasma"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3019550' THEN value_as_number ELSE null END)) AS "Sodium in Serum or Plasma"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3037278' THEN value_as_number ELSE null END)) AS "Anion gap 4 in Serum or Plasma"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3004501' THEN value_as_number ELSE null END)) AS "Glucose in Serum or Plasma"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3012095' THEN value_as_number ELSE null END)) AS "Magnesium in Serum or Plasma"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3015377' THEN value_as_number ELSE null END)) AS "Calcium in Serum or Plasma"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3011904' THEN value_as_number ELSE null END)) AS "Phosphate in Serum or Plasma"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3014886' THEN value_as_number ELSE null END)) AS "Neutrophils in Urine by Automated count"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3004327' THEN value_as_number ELSE null END)) AS "Lymphocytes in Blood by Automated count"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3013429' THEN value_as_number ELSE null END)) AS "Basophils in Blood by Automated count"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3028615' THEN value_as_number ELSE null END)) AS "Eosinophils in Blood by Automated count"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3033575' THEN value_as_number ELSE null END)) AS "Monocytes in Blood by Automated count"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3034426' THEN value_as_number ELSE null END)) AS "Prothrombin time (PT)"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3022217' THEN value_as_number ELSE null END)) AS "INR in Platelet poor plasma by Coagulation assay"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3013466' THEN value_as_number ELSE null END)) AS "aPTT in Blood by Coagulation assay"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3005897' THEN value_as_number ELSE null END)) AS "Protein in Urine by Test strip"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3000330' THEN value_as_number ELSE null END)) AS "Specific gravity of Urine by Test strip"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3022621' THEN value_as_number ELSE null END)) AS "pH of Urine by Test strip"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3024629' THEN value_as_number ELSE null END)) AS "Glucose in Urine by Test strip"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3023539' THEN value_as_number ELSE null END)) AS "Ketones in Urine by Test strip"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3037426' THEN value_as_number ELSE null END)) AS "Urobilinogen in Urine by Test strip"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3024128' THEN value_as_number ELSE null END)) AS "Bilirubin.total in Serum or Plasma"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3013721' THEN value_as_number ELSE null END)) AS "Aspartate aminotransferase in Serum or Plasma"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3006923' THEN value_as_number ELSE null END)) AS "Alanine aminotransferase in Serum or Plasma"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3035995' THEN value_as_number ELSE null END)) AS "Alkaline phosphatase in Serum or Plasma"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3035583' THEN value_as_number ELSE null END)) AS "Leukocytes in Urine sediment by Microscopy high power field"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3025255' THEN value_as_number ELSE null END)) AS "Bacteria in Urine sediment by Microscopy high power field"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3010189' THEN value_as_number ELSE null END)) AS "Epithelial cells in Urine sediment by Microscopy high power field"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3001494' THEN value_as_number ELSE null END)) AS "Erythrocytes in Urine sediment by Microscopy high power field"
, COALESCE(AVG(CASE WHEN measurement_concept_id = '3024561' THEN value_as_number ELSE null END)) AS "Albumin in Serum or Plasma"
FROM stg_2
GROUP BY person_id, visit_occurrence_id, measurement_date, rn
)
SELECT
DISTINCT ON (person_id, visit_occurrence_id, measurement_date)
person_id
, visit_occurrence_id
, measurement_date
, "Leukocytes in Blood by Manual count"
, "Hematocrit of Blood by Automated count"
, "MCHC by Automated count"
, "MCH by Automated count"
, "Erythrocyte distribution width by Automated count"
, "MCV by Automated count"
, "Erythrocytes in Blood by Automated count"
, "Platelets in Blood by Automated count"
, "Hemoglobin in Blood"
, "Urea nitrogen in Serum or Plasma"
, "Creatinine in Serum or Plasma"
, "Potassium in Serum or Plasma"
, "Chloride in Serum or Plasma"
, "Bicarbonate in Serum or Plasma"
, "Sodium in Serum or Plasma"
, "Anion gap 4 in Serum or Plasma"
, "Glucose in Serum or Plasma"
, "Magnesium in Serum or Plasma"
, "Calcium in Serum or Plasma"
, "Phosphate in Serum or Plasma"
, "Neutrophils in Urine by Automated count"
, "Lymphocytes in Blood by Automated count"
, "Basophils in Blood by Automated count"
, "Eosinophils in Blood by Automated count"
, "Monocytes in Blood by Automated count"
, "Prothrombin time (PT)"
, "INR in Platelet poor plasma by Coagulation assay"
, "aPTT in Blood by Coagulation assay"
, "Protein in Urine by Test strip"
, "Specific gravity of Urine by Test strip"
, "pH of Urine by Test strip"
, "Glucose in Urine by Test strip"
, "Ketones in Urine by Test strip"
, "Urobilinogen in Urine by Test strip"
, "Bilirubin.total in Serum or Plasma"
, "Aspartate aminotransferase in Serum or Plasma"
, "Alanine aminotransferase in Serum or Plasma"
, "Alkaline phosphatase in Serum or Plasma"
, "Leukocytes in Urine sediment by Microscopy high power field"
, "Bacteria in Urine sediment by Microscopy high power field"
, "Epithelial cells in Urine sediment by Microscopy high power field"
, "Erythrocytes in Urine sediment by Microscopy high power field"
, "Albumin in Serum or Plasma"
FROM
stg_3
ORDER BY person_id, visit_occurrence_id, measurement_date, rn ASC NULLS LAST
;
