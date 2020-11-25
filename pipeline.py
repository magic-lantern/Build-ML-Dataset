

@transform_pandas(
    Output(rid="ri.vector.main.execute.c50c04da-8d5e-42bb-8990-6b0752ff4ca6"),
    labs_from_inpatient_visits=Input(rid="ri.foundry.main.dataset.9cf45dff-b77e-4e52-bd3d-2209004983a2")
)
def datetime_histogram(labs_from_inpatient_visits):
    import seaborn as sns
    sns.histplot(labs_from_inpatient_visits.measurement_datetime)

