import seaborn as sns

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.96a61276-4442-4694-a0fd-7e5709c73dc9"),
    labs_from_inpatient_visits=Input(rid="ri.foundry.main.dataset.9cf45dff-b77e-4e52-bd3d-2209004983a2")
)
def datetime_histogram(labs_from_inpatient_visits):
    tempdf = labs_from_inpatient_visits.filter(labs_from_inpatient_visits.measurement_datetime.isNotNull()).limit(1000)
    df = tempdf.toPandas()
    pout = sns.histplot(df.measurement_datetime)
    pout.show()
    #return None
    #return labs_from_inpatient_visits.filter(labs_from_inpatient_visits.measurement_datetime.isNotNull()).limit(1000)

