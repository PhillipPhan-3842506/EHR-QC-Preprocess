SELECT
vo.person_id,
vo.visit_occurrence_id,
vo.visit_start_datetime,
de.death_datetime,
(CASE WHEN ((de.death_datetime is not null) and (de.death_datetime > (vo.visit_start_datetime +  INTERVAL '0 day')) and (de.death_datetime < (vo.visit_start_datetime +  INTERVAL '7 day'))) THEN 1 ELSE 0 END) as Death_0_7,
(CASE WHEN ((de.death_datetime is not null) and (de.death_datetime > (vo.visit_start_datetime +  INTERVAL '0 day')) and (de.death_datetime < (vo.visit_start_datetime +  INTERVAL '14 day'))) THEN 1 ELSE 0 END) as Death_0_14,
(CASE WHEN ((de.death_datetime is not null) and (de.death_datetime > (vo.visit_start_datetime +  INTERVAL '0 day')) and (de.death_datetime < (vo.visit_start_datetime +  INTERVAL '21 day'))) THEN 1 ELSE 0 END) as Death_0_21,
(CASE WHEN ((de.death_datetime is not null) and (de.death_datetime > (vo.visit_start_datetime +  INTERVAL '0 day')) and (de.death_datetime < (vo.visit_start_datetime +  INTERVAL '30 day'))) THEN 1 ELSE 0 END) as Death_0_30,
(CASE WHEN ((de.death_datetime is not null) and (de.death_datetime > (vo.visit_start_datetime +  INTERVAL '0 day')) and (de.death_datetime < (vo.visit_start_datetime +  INTERVAL '60 day'))) THEN 1 ELSE 0 END) as Death_0_60,
(CASE WHEN ((de.death_datetime is not null) and (de.death_datetime > (vo.visit_start_datetime +  INTERVAL '0 day')) and (de.death_datetime < (vo.visit_start_datetime +  INTERVAL '90 day'))) THEN 1 ELSE 0 END) as Death_0_90,
(CASE WHEN ((de.death_datetime is not null) and (de.death_datetime > (vo.visit_start_datetime +  INTERVAL '0 day')) and (de.death_datetime < (vo.visit_start_datetime +  INTERVAL '120 day'))) THEN 1 ELSE 0 END) as Death_0_120,
(CASE WHEN ((de.death_datetime is not null) and (de.death_datetime > (vo.visit_start_datetime +  INTERVAL '7 day')) and (de.death_datetime < (vo.visit_start_datetime +  INTERVAL '14 day'))) THEN 1 ELSE 0 END) as Death_7_14,
(CASE WHEN ((de.death_datetime is not null) and (de.death_datetime > (vo.visit_start_datetime +  INTERVAL '14 day')) and (de.death_datetime < (vo.visit_start_datetime +  INTERVAL '21 day'))) THEN 1 ELSE 0 END) as Death_14_21,
(CASE WHEN ((de.death_datetime is not null) and (de.death_datetime > (vo.visit_start_datetime +  INTERVAL '21 day')) and (de.death_datetime < (vo.visit_start_datetime +  INTERVAL '30 day'))) THEN 1 ELSE 0 END) as Death_21_30,
(CASE WHEN ((de.death_datetime is not null) and (de.death_datetime > (vo.visit_start_datetime +  INTERVAL '30 day')) and (de.death_datetime < (vo.visit_start_datetime +  INTERVAL '60 day'))) THEN 1 ELSE 0 END) as Death_30_60,
(CASE WHEN ((de.death_datetime is not null) and (de.death_datetime > (vo.visit_start_datetime +  INTERVAL '60 day')) and (de.death_datetime < (vo.visit_start_datetime +  INTERVAL '90 day'))) THEN 1 ELSE 0 END) as Death_60_90,
(CASE WHEN ((de.death_datetime is not null) and (de.death_datetime > (vo.visit_start_datetime +  INTERVAL '90 day')) and (de.death_datetime < (vo.visit_start_datetime +  INTERVAL '120 day'))) THEN 1 ELSE 0 END) as Death_90_120
FROM
__schema_name__.cdm_visit_occurrence vo
inner join omop_test_20220817.cohort_icd co
ON co.person_id = vo.person_id AND co.visit_occurrence_id = vo.visit_occurrence_id
left join __schema_name__.cdm_death de
ON de.person_id = vo.person_id
;
