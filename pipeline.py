import seaborn as sns
import matplotlib.pyplot as plt

@transform_pandas(
    Output(rid="ri.vector.main.execute.f7d52136-b6d3-4dcf-92c4-40ea90053277"),
    inpatient_payer_join=Input(rid="ri.foundry.main.dataset.ea22427f-b454-417f-a608-9ced2a96bf77")
)
def inpatient_payer(inpatient_payer_join):
    #dfTemp = joinSeverity.withColumn("payer_concept_name", regexp_replace("payer_concept_name", "/", "_"))
    #dfTemp = joinSeverity.withColumn("payer_concept_name", regexp_replace("payer_concept_name", " ", "_"))
    #dfTemp = joinSeverity.withColumn("payer_concept_name", regexp_replace("payer_concept_name", ",", ""))

    #Pivot by payer plan name
    return inpatient_payer_join.groupby("visit_occurrence_id").pivot("payer_concept_name")

