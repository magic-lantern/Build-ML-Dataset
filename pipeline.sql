

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.94159c67-1ed6-4df1-881d-56d6d4b8451b"),
    inpatient_labs=Input(rid="ri.foundry.main.dataset.9cf45dff-b77e-4e52-bd3d-2209004983a2"),
    inpatient_worst_labs_full=Input(rid="ri.foundry.main.dataset.3548767f-6fe1-4ef8-b7c8-1851a0c67aa5")
)
select distinct alias from inpatient_labs
order by alias

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8aa129e9-71eb-4dd6-aff0-85775f04cb3d"),
    inpatient_ml_dataset=Input(rid="ri.foundry.main.dataset.07927bca-b175-4775-9c55-a371af481cc1")
)
SELECT blood_type, count(1) as rec_count
FROM inpatient_ml_dataset
GROUP BY blood_type

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c1c686ec-6a67-4278-8695-3dc25b69821e"),
    bestVisitPossible=Input(rid="ri.foundry.main.dataset.18864c06-114d-428e-8be9-170ebdc97729")
)
select visit_concept_name, count(1) as vt_count
from bestVisitPossible
group by visit_concept_name

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c47f0ebb-dcb0-472a-ab69-6dfcf40faeb1"),
    inpatient_labs=Input(rid="ri.foundry.main.dataset.9cf45dff-b77e-4e52-bd3d-2209004983a2")
)
select * from 
(
    select mt, count(1) as count_recs
    from (
        SELECT SUBSTRING(measurement_time, 0, 5) as mt
        FROM inpatient_labs 
        --WHERE measurement_time IS NOT NULL
    )
    group by mt
)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d854a95d-fd7a-4781-9b4b-eac1bd597afb"),
    test_lab_filter=Input(rid="ri.foundry.main.dataset.b67797ec-1918-43d6-9a25-321582987d38")
)
SELECT *
FROM test_lab_filter
where measurement_day_of_visit <= 1

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.007ebe6e-f7e6-4403-b207-e6743adc6cd0"),
    pt_table_drop_unaffected=Input(rid="ri.foundry.main.dataset.52e25a99-5ccd-40d2-a91b-56122e3174ce")
)
SELECT
    visit_concept_name, 
    count(1) AS num_visits,
    MIN(length_of_stay) AS min_los,
    MAX(length_of_stay) AS max_los,
    MEAN(length_of_stay) AS mean_los
FROM pt_table_drop_unaffected
GROUP BY visit_concept_name

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.53cba45b-388b-406a-bfbd-8f3623a7110b"),
    inpatient_worst_labs=Input(rid="ri.foundry.main.dataset.c1c6e3b9-83ff-421a-b5c6-75518beec801")
)
SELECT *
FROM inpatient_worst_labs
WHERE visit_occurrence_id = 2089553554350138032
OR visit_occurrence_id = 1000115681187938502

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b76d5a99-334b-49ca-bd0c-8684573c50ba"),
    inpatient_ml_dataset=Input(rid="ri.foundry.main.dataset.07927bca-b175-4775-9c55-a371af481cc1")
)
select count(1) as num_rec, year(visit_start_date) as s_year, month(visit_start_date) as s_month
from inpatient_ml_dataset
group by s_year, s_month
order by s_year, s_month

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
    Output(rid="ri.foundry.main.dataset.1da536da-5594-4df1-98cf-d364d2773b3e"),
    Pivot_on_charlson=Input(rid="ri.foundry.main.dataset.4a9afe05-3616-49ca-a9c3-73d462467053"),
    inpatients=Input(rid="ri.foundry.main.dataset.a773e078-3908-4189-83a2-2831a8f002f9")
)
SELECT
    c.person_id,
    -- filling in all null values with false
    CASE
        WHEN CHF IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS CHF,
    CASE
        WHEN Cancer IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS Cancer,
    CASE
        WHEN DM IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS DM,
    CASE
        WHEN DMcx IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS DMcx,
    CASE
        WHEN Dementia IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS Dementia,
    CASE
        WHEN HIV IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS HIV,
    CASE
        WHEN LiverMild IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS LiverMild,
    CASE
        WHEN LiverSevere IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS LiverSevere,
    CASE
        WHEN MI IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS MI,
    CASE
        WHEN Mets IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS Mets,
    CASE
        WHEN PUD IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS PUD,
    CASE
        WHEN PVD IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS PVD,
    CASE
        WHEN Paralysis IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS Paralysis,
    CASE
        WHEN Pulmonary IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS Pulmonary,
    CASE
        WHEN Renal IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS Renal,
    CASE
        WHEN Rheumatic IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS Rheumatic,
    CASE
        WHEN Stroke IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS Stroke
FROM Pivot_on_charlson c
LEFT JOIN inpatients v
    ON c.person_id = v.person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.43c5cbbb-374e-4e51-b1ea-a6246ef00e3a"),
    inpatient_ml_dataset=Input(rid="ri.foundry.main.dataset.07927bca-b175-4775-9c55-a371af481cc1")
)
select count(1) as num_rec, year(visit_end_date) as s_year, month(visit_end_date) as s_month
from inpatient_ml_dataset
group by s_year, s_month
order by s_year, s_month

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9cf45dff-b77e-4e52-bd3d-2209004983a2"),
    Filterwithcodesetaliastable=Input(rid="ri.foundry.main.dataset.ff7e826a-1dbc-480e-86dc-d75aa802f9d8"),
    inpatients=Input(rid="ri.foundry.main.dataset.a773e078-3908-4189-83a2-2831a8f002f9")
)
SELECT DISTINCT
    l.visit_occurrence_id,
    l.person_id,
    l.data_partner_id,
    v.visit_start_date,
    v.visit_end_date,
    measurement_date,
    measurement_datetime,
    measurement_time,
    value_as_number,
    value_as_concept_id,
    harmonized_value_as_number,
    measurement_julian_day,
    has_quantitative_scale,
    has_value_as_number,
    has_value_as_concept,
    has_value_as_concept_only,
    measurement_age_in_years_fraction,
    measurement_age_in_days,
    measurement_day_of_visit,
    Alias as alias
FROM Filterwithcodesetaliastable l
-- LEFT JOIN inpatient_bestVisitPossible v
LEFT JOIN inpatients v
    ON l.visit_occurrence_id = v.visit_occurrence_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ea22427f-b454-417f-a608-9ced2a96bf77"),
    inpatients=Input(rid="ri.foundry.main.dataset.a773e078-3908-4189-83a2-2831a8f002f9"),
    map2_visit_occurrence_payer_plan=Input(rid="ri.foundry.main.dataset.bc1ee09f-face-40da-8840-fa27e1b2e263")
)
SELECT DISTINCT
    p.visit_occurrence_id,
    p.person_id,
    i.visit_start_date,
    i.visit_end_date,
    p.payer_plan_period_start_date,
    p.payer_plan_period_end_date,
    p.data_partner_id,
    p.payer_concept_name
FROM map2_visit_occurrence_payer_plan p
INNER JOIN inpatients i
ON i.visit_occurrence_id = p.visit_occurrence_id
ORDER BY p.visit_occurrence_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a773e078-3908-4189-83a2-2831a8f002f9"),
    pt_table_drop_unaffected=Input(rid="ri.foundry.main.dataset.52e25a99-5ccd-40d2-a91b-56122e3174ce"),
    visit_problems=Input(rid="ri.foundry.main.dataset.8b112ce6-7e66-4752-b95a-bb17b1a64791")
)
SELECT
    -- manually specifying most columns as spark sql doesn't seem to support * EXCEPT () syntax
    person_id,
    data_partner_id,
    visit_concept_id,
    visit_start_date,
    visit_concept_name,
    visit_occurrence_id,
    age_at_visit_start_in_years_int,
    length_of_stay,
    gender_concept_name,
    Race,
    Ethnicity,
    smoking_status,
    blood_type,
    covid_status_name,
    Severity_Type,
    Q_Score,
    Testcount,
    DATE_ADD(visit_start_date, length_of_stay) AS visit_end_date,
    -- most boolean columns only have true and null; filling nulls with false
    CASE
        WHEN positive_covid_test IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS positive_covid_test,
    CASE
        WHEN negative_covid_test IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS negative_covid_test,
    CASE
        WHEN Suspected_COVID IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS suspected_covid,
    CASE
        WHEN in_death_table IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS in_death_table,
    CASE
        WHEN ECMO IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS ecmo,
    CASE
        WHEN AKI_in_hospital IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS aki_in_hospital,
    CASE
        WHEN Invasive_Ventilation IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS invasive_ventilation,
    CASE
        WHEN ECMO IS NOT NULL OR Invasive_Ventilation IS NOT NULL OR in_death_table = TRUE THEN TRUE
        ELSE FALSE
    END AS bad_outcome
FROM pt_table_drop_unaffected
WHERE 1 = 1
AND visit_concept_name LIKE 'Inpatient%'
AND visit_occurrence_id NOT IN (
    SELECT DISTINCT visit_occurrence_id
    FROM visit_problems
)
AND YEAR(DATE_ADD(visit_start_date, length_of_stay)) >= 2020
-- don't need these as switched to pt_table_drop_unaffected
-- AND data_partner_id != 411
-- AND data_partner_id != 224
-- AND data_partner_id != 787
-- what about under 18?
-- perhaps should just switch to Ptwithscores_drop_before_table_2

@transform_pandas(
    Output(rid="ri.vector.main.execute.7088f128-6b0d-4f7f-accf-20153d6d1777"),
    inpatient_labs=Input(rid="ri.foundry.main.dataset.9cf45dff-b77e-4e52-bd3d-2209004983a2")
)
SELECT *
FROM labs_from_inpatient_visits
where visit_occurrence_id = 1182809160182337912

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c596f8f0-252d-4b78-8482-594d8f0b8981"),
    inpatient_labs=Input(rid="ri.foundry.main.dataset.9cf45dff-b77e-4e52-bd3d-2209004983a2")
)
SELECT *
FROM inpatient_labs
WHERE -- year(measurement_datetime) = 1900
    --OR
     measurement_datetime IS NULL

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.5a911556-d9e5-47c6-a085-19ff97720222"),
    Charlson_for_all_covidpos=Input(rid="ri.foundry.main.dataset.51933503-15d5-4ee5-a543-964c351971b4"),
    inpatients=Input(rid="ri.foundry.main.dataset.a773e078-3908-4189-83a2-2831a8f002f9")
)
SELECT
    c.person_id,
    -- filling in all null values with false
    CASE
        WHEN CHF IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS CHF,
    CASE
        WHEN Cancer IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS Cancer,
    CASE
        WHEN DM IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS DM,
    CASE
        WHEN DMcx IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS DMcx,
    CASE
        WHEN Dementia IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS Dementia,
    CASE
        WHEN HIV IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS HIV,
    CASE
        WHEN LiverMild IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS LiverMild,
    CASE
        WHEN LiverSevere IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS LiverSevere,
    CASE
        WHEN MI IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS MI,
    CASE
        WHEN Mets IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS Mets,
    CASE
        WHEN PUD IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS PUD,
    CASE
        WHEN PVD IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS PVD,
    CASE
        WHEN Paralysis IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS Paralysis,
    CASE
        WHEN Pulmonary IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS Pulmonary,
    CASE
        WHEN Renal IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS Renal,
    CASE
        WHEN Rheumatic IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS Rheumatic,
    CASE
        WHEN Stroke IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS Stroke
FROM Charlson_for_all_covidpos c
INNER JOIN inpatients v
    ON c.person_id = v.person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c7376f23-515a-4a11-932a-2705f0d09f89"),
    Ptwithscores_drop_before_table_1=Input(rid="ri.foundry.main.dataset.d345497b-ebed-4055-90aa-48b38b346396")
)
SELECT *
FROM Ptwithscores_drop_before_table_1
WHERE age_at_visit_start_in_years_int IS NULL

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d42ff644-f715-461a-b9cb-ce271108c5ff"),
    pt_table_drop_unaffected=Input(rid="ri.foundry.main.dataset.52e25a99-5ccd-40d2-a91b-56122e3174ce")
)
SELECT *
FROM pt_table_drop_unaffected
WHERE age_at_visit_start_in_years_int IS NULL

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.88ac97c3-c364-42cb-8418-b9fe139e7fd3"),
    pt_table_drop_unaffected=Input(rid="ri.foundry.main.dataset.52e25a99-5ccd-40d2-a91b-56122e3174ce")
)
select count(1) AS num_rec, s_year, s_month
from (
    SELECT YEAR(DATE_ADD(visit_start_date, length_of_stay)) AS s_year, MONTH(DATE_ADD(visit_start_date, length_of_stay)) AS s_month
    FROM pt_table_drop_unaffected
)
group by s_year, s_month
order by s_year, s_month

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8c85cddc-c8b5-4d4a-ae6f-e8efac4e5b8d"),
    Ptwithscores_drop_before_table_1=Input(rid="ri.foundry.main.dataset.d345497b-ebed-4055-90aa-48b38b346396")
)
SELECT
    visit_concept_name, 
    count(1) AS num_visits,
    MIN(length_of_stay) AS min_los,
    MAX(length_of_stay) AS max_los,
    MEAN(length_of_stay) AS mean_los
FROM Ptwithscores_drop_before_table_1
GROUP BY visit_concept_name

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d5287a21-4a0f-423b-928f-5aad94234d08"),
    Collapse_smoking_by_person=Input(rid="ri.foundry.main.dataset.af27976f-9520-45b1-a0c1-8ea888ba23ef"),
    inpatient_ml_dataset=Input(rid="ri.foundry.main.dataset.07927bca-b175-4775-9c55-a371af481cc1")
)
SELECT i.*
FROM Collapse_smoking_by_person s
INNER JOIN inpatient_ml_dataset i
ON s.person_id = i.person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.41ad82d9-7dbd-4401-88cc-64571e1e09b8"),
    inpatient_worst_labs_full=Input(rid="ri.foundry.main.dataset.3548767f-6fe1-4ef8-b7c8-1851a0c67aa5")
)
SELECT *
FROM inpatient_worst_labs_full
where alias = 'SpO2'

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
SELECT 
    o.num_obs,
    mv.num_vist_w_obs,
    sv.min_value,
    sv.max_value,
    sv.mean_value,
    sv.median_value,
    o.alias
FROM
(SELECT count(1) as num_obs, Alias as alias
FROM Filterwithcodesetaliastable
group by Alias) o
LEFT JOIN (
    SELECT count(distinct visit_occurrence_id) as num_vist_w_obs, Alias as alias
    FROM Filterwithcodesetaliastable
    group by Alias) mv
on o.alias = mv.alias
LEFT JOIN (
    SELECT
        MIN(harmonized_value_as_number) AS min_value,
        MAX(harmonized_value_as_number) AS max_value,
        MEAN(harmonized_value_as_number) AS mean_value,
        percentile_approx(harmonized_value_as_number, 0.5) AS median_value,
        Alias as alias
    FROM Filterwithcodesetaliastable
    group by Alias) sv
on o.alias = sv.alias

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
    Output(rid="ri.foundry.main.dataset.b67797ec-1918-43d6-9a25-321582987d38"),
    inpatient_labs=Input(rid="ri.foundry.main.dataset.9cf45dff-b77e-4e52-bd3d-2209004983a2")
)
SELECT *
FROM inpatient_labs
WHERE visit_occurrence_id = 2089553554350138032
OR visit_occurrence_id = 1000115681187938502

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.bf1ffe47-e008-4cc2-886a-c71c6c9fbaf3"),
    inpatient_worst_labs_full=Input(rid="ri.foundry.main.dataset.3548767f-6fe1-4ef8-b7c8-1851a0c67aa5")
)
SELECT *
FROM inpatient_worst_labs_full
WHERE visit_occurrence_id = 2089553554350138032
OR visit_occurrence_id = 1000115681187938502

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

