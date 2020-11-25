

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

