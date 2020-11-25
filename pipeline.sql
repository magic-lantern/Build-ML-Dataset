

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c1c686ec-6a67-4278-8695-3dc25b69821e"),
    bestVisitPossible=Input(rid="ri.foundry.main.dataset.18864c06-114d-428e-8be9-170ebdc97729")
)
select visit_concept_name, count(1) as vt_count
from bestVisitPossible
group by visit_concept_name

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c47f0ebb-dcb0-472a-ab69-6dfcf40faeb1"),
    labs_from_inpatient_visits=Input(rid="ri.foundry.main.dataset.9cf45dff-b77e-4e52-bd3d-2209004983a2")
)
select * from 
(
    select mt, count(1) as count_recs
    from (
        SELECT SUBSTRING(measurement_time, 0, 5) as mt
        FROM labs_from_inpatient_visits 
        --WHERE measurement_time IS NOT NULL
    )
    group by mt
)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.2ae94403-e46c-4586-9863-470e06737fcc"),
    bestVisitPossible=Input(rid="ri.foundry.main.dataset.18864c06-114d-428e-8be9-170ebdc97729")
)
SELECT *
FROM bestVisitPossible
WHERE 1 = 1
AND visit_concept_name LIKE 'Inpatient%'
AND (visit_start_date <= visit_end_date
    OR visit_end_date IS NULL)

@transform_pandas(
    Output(rid="ri.vector.main.execute.7088f128-6b0d-4f7f-accf-20153d6d1777"),
    labs_from_inpatient_visits=Input(rid="ri.foundry.main.dataset.9cf45dff-b77e-4e52-bd3d-2209004983a2")
)
SELECT *
FROM labs_from_inpatient_visits
where visit_occurrence_id = 1182809160182337912

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9cf45dff-b77e-4e52-bd3d-2209004983a2"),
    Filterwithcodesetaliastable=Input(rid="ri.foundry.main.dataset.ff7e826a-1dbc-480e-86dc-d75aa802f9d8"),
    inpatient_bestVisitPossible=Input(rid="ri.foundry.main.dataset.2ae94403-e46c-4586-9863-470e06737fcc")
)
SELECT l.*
FROM Filterwithcodesetaliastable l
LEFT JOIN inpatient_bestVisitPossible v
on l.visit_occurrence_id = v.visit_occurrence_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c596f8f0-252d-4b78-8482-594d8f0b8981"),
    labs_from_inpatient_visits=Input(rid="ri.foundry.main.dataset.9cf45dff-b77e-4e52-bd3d-2209004983a2")
)
SELECT *
FROM labs_from_inpatient_visits
WHERE -- year(measurement_datetime) = 1900
    --OR
     measurement_datetime IS NULL

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.fa76c1c4-f05e-4583-a854-95617b81fd44"),
    Pt_table_w_derived_scores=Input(rid="ri.foundry.main.dataset.6c557303-95ef-4ba2-841a-dea8e553e127"),
    visit_problems=Input(rid="ri.foundry.main.dataset.8b112ce6-7e66-4752-b95a-bb17b1a64791")
)
SELECT severity_type, count(1) as cnt_sev
FROM (
    SELECT DISTINCT
        v.visit_occurrence_id,
        s.Severity_Type as severity_type
    FROM visit_problems v
    INNER JOIN Pt_table_w_derived_scores s
    ON v.visit_occurrence_id = s.visit_occurrence_id
)
GROUP BY severity_type

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.bdcd1f3c-5c7e-4297-a45d-1ed1011fb591"),
    Filterwithcodesetaliastable=Input(rid="ri.foundry.main.dataset.ff7e826a-1dbc-480e-86dc-d75aa802f9d8")
)
SELECT o.num_obs, mv.num_vist_w_obs, o.alias FROM
(SELECT count(1) as num_obs, Alias as alias
FROM Filterwithcodesetaliastable
group by Alias) o
LEFT JOIN (
    SELECT count(distinct visit_occurrence_id) as num_vist_w_obs, Alias as alias
    FROM Filterwithcodesetaliastable
    group by Alias) mv
on o.alias = mv.alias
ORDER BY o.alias

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c0c1354d-ab97-4c7c-af46-0c6912ea81ce"),
    Filterwithcodesetaliastable=Input(rid="ri.foundry.main.dataset.ff7e826a-1dbc-480e-86dc-d75aa802f9d8")
)
SELECT count(distinct person_id) AS result, 'count_pers' as stat
FROM Filterwithcodesetaliastable
UNION ALL
SELECT count(distinct visit_occurrence_id) AS result, 'count_visit' as stat
FROM Filterwithcodesetaliastable
UNION ALL
SELECT count(distinct macrovisit_id) AS result, 'count_macro_v' as stat
FROM Filterwithcodesetaliastable
UNION ALL
SELECT count(distinct Alias) AS result, 'count_labs' as stat
FROM Filterwithcodesetaliastable
UNION ALL
SELECT count(1) as result, 'num_null_datetime' as stat
FROM Filterwithcodesetaliastable
where measurement_datetime is NULL
UNION ALL
SELECT count(1) as result, 'num_1900_datetime' as stat
FROM Filterwithcodesetaliastable
where measurement_datetime = '1900-01-01T00:00:00.000Z'

@transform_pandas(
    Output(rid="ri.vector.main.execute.cc4066b9-d5c1-4cd5-af67-8b9f25e96d65"),
    inpatient_bestVisitPossible=Input(rid="ri.foundry.main.dataset.2ae94403-e46c-4586-9863-470e06737fcc")
)
SELECT count(1)
FROM inpatient_bestVisitPossible
WHERE visit_start_datetime IS NOT NULL

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8b112ce6-7e66-4752-b95a-bb17b1a64791"),
    bestVisitPossible=Input(rid="ri.foundry.main.dataset.18864c06-114d-428e-8be9-170ebdc97729")
)
SELECT *
FROM bestVisitPossible
where visit_start_date > visit_end_date

