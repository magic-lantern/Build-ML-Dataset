import seaborn as sns
import matplotlib.pyplot as plt

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d30362c9-a90a-4486-aba9-d67e40c25fd0"),
    inpatients=Input(rid="ri.foundry.main.dataset.a773e078-3908-4189-83a2-2831a8f002f9"),
    map2_visit_occurrence_payer_plan=Input(rid="ri.foundry.main.dataset.bc1ee09f-face-40da-8840-fa27e1b2e263")
)
def inpatient_payer( map2_visit_occurrence_payer_plan, inpatients):
    pdf = map2_visit_occurrence_payer_plan
    pdf = pdf.select("visit_occurrence_id", "person_id", "payer_plan_period_start_date", "payer_plan_period_end_date", "data_partner_id", "payer_concept_name")

    idf = inpatients
    idf = idf.select("visit_occurrence_id", "visit_start_date", "visit_end_date")
    idf = idf.withColumnRenamed("visit_occurrence_id", "v_id")

    df = pdf.join(idf, idf["v_id"] == pdf["visit_occurrence_id"], "inner")
    df = df.drop("v_id")
    df = df.groupby("visit_occurrence_id").pivot("payer_concept_name").count()

    #Cast each column to a boolean
    #df = df.select(*[F.col(c) if c == 'visit_occurrence_id' else F.col(c).cast('boolean') for c in df.columns])
    #df = df.drop("null")

    # do we need to do this?
    #dfTemp = joinSeverity.withColumn("payer_concept_name", regexp_replace("payer_concept_name", "/", "_"))
    #dfTemp = joinSeverity.withColumn("payer_concept_name", regexp_replace("payer_concept_name", " ", "_"))
    #dfTemp = joinSeverity.withColumn("payer_concept_name", regexp_replace("payer_concept_name", ",", ""))

    #Pivot by payer plan name
    return df

