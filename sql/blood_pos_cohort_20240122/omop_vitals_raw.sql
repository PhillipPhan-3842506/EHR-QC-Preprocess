select
mmt.measurement_id as measurement_id,
mmt.visit_occurrence_id as visit_occurrence_id,
mmt.person_id as person_id,
mmt.measurement_datetime as measurement_datetime,
con.concept_name as concept_name,
mmt.value_as_number as value_as_number
from
__schema_name__.cdm_measurement mmt
inner join __schema_name__.concept con
on con.concept_code = mmt.measurement_concept_id
where mmt.load_table_id = 'chartevents'
;
