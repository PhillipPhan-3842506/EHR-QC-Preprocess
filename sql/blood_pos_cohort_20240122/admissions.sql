SELECT
distinct
cdm_vo.person_id,
cdm_vo.visit_occurrence_id,
src_adm.patient_id,
src_adm.episode_id,
src_adm.admittime,
src_adm.dischtime,
src_adm.deathtime
from
__schema_name__.cdm_visit_occurrence cdm_vo
inner join __schema_name__.src_admissions src_adm
on cdm_vo.person_id = src_adm.patient_id::int and cdm_vo.visit_occurrence_id = src_adm.episode_id::int
;
