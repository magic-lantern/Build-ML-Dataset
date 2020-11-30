import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import functions as F
from pyspark.sql.functions import max, mean, min, stddev, lit, regexp_replace, col

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
    df = df.withColumn("payer_concept_name", regexp_replace("payer_concept_name", "/", "_"))
    df = df.withColumn("payer_concept_name", regexp_replace("payer_concept_name", " ", "_"))
    df = df.withColumn("payer_concept_name", regexp_replace("payer_concept_name", ",", ""))
    df = df.withColumn("payer_concept_name", F.lower(col("payer_concept_name")))

    #Pivot by payer_concept_name and cast each new column to a boolean
    pivot_df = df.groupby('visit_occurrence_id').pivot("payer_concept_name").count()
    pivot_df = pivot_df.select(*[col(c) if c == 'visit_occurrence_id' else col(c).cast('boolean') for c in pivot_df.columns])
    pivot_df = pivot_df.drop("null")
    
    return pivot_df

@transform_pandas(
    Output(rid="ri.vector.main.execute.55b674c2-5d21-455a-b0bf-892e2d20db5a"),
    lab_filter=Input(rid="ri.vector.main.execute.9c392f6c-a758-4ccb-968b-59b9ed94fe1e")
)
def unnamed_1(lab_filter):
    

