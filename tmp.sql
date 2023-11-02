select
mmt.person_id as person_id,
mmt.visit_occurrence_id as visit_occurrence_id,
mmt.measurement_datetime as measurement_datetime,
mmt.measurement_concept_id as measurement_concept_id,
mmt.value_as_number as value_as_number
from
omop_migration_etl_20231021.cdm_measurement mmt
where load_table_id = 'labevents'
and mmt.person_id = '2084529' and mmt.visit_occurrence_id = '1112' and mmt.measurement_concept_id in ('1022571000000108', '767002')
;
