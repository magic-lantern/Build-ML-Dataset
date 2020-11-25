

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.2ae94403-e46c-4586-9863-470e06737fcc"),
    bestVisitPossible=Input(rid="ri.foundry.main.dataset.18864c06-114d-428e-8be9-170ebdc97729")
)
SELECT *
FROM bestVisitPossible
where visit_concept_name like 'Inpatient%'

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

