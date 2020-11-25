

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.2ae94403-e46c-4586-9863-470e06737fcc"),
    bestVisitPossible=Input(rid="ri.foundry.main.dataset.18864c06-114d-428e-8be9-170ebdc97729")
)
SELECT *
FROM bestVisitPossible
where visit_concept_name = 'Inpatient'

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.58eea22d-1433-4072-952a-5ac6d69234dd"),
    Filterwithcodesetaliastable=Input(rid="ri.foundry.main.dataset.ff7e826a-1dbc-480e-86dc-d75aa802f9d8")
)
SELECT count(1) as result, 'num_null' as stat_col
FROM Filterwithcodesetaliastable
where measurement_datetime is NULL

UNION

SELECT count(1) as result, 'num_1900' as stat_col
FROM Filterwithcodesetaliastable
where measurement_datetime = '1900-01-01T00:00:00.000Z'

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
    Output(rid="ri.vector.main.execute.e971485e-0f1e-446b-99ad-68b2f21a8048"),
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

