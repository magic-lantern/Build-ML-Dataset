import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import functions as F
from pyspark.sql.functions import max, mean, min, stddev, lit, regexp_replace, col
import numpy as np

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
    test_lab_filter=Input(rid="ri.foundry.main.dataset.b67797ec-1918-43d6-9a25-321582987d38")
)
def worst_lab(test_lab_filter):
    df = test_lab_filter

    # likely will want to adjust this window
    df.filter(df.measurement_day_of_visit <= 1)
    
    labs = {'ALT (SGPT), IU/L': 'high',
        'AST (SGOT), IU/L': 'high',
        'Blood type (ABO + Rh)': 'categorical',
        'BMI': 'high',
        'BNP, pg/mL': 'high',
        'Body weight': 'high',
        'BUN, mg/dL  ': 'high',
        'c-reactive protein CRP, mg/L': 'high',
        'Chloride, mmol/L': 'high',
        'Creatinine, mg/dL': 'high',
        'D-Dimer, mg/L FEU': 'high',
        'Diastolic blood pressure': 'low',
        'Erythrocyte Sed. Rate, mm/hr': 'high',
        'Ferritin, ng/mL': 'high',
        'Glasgow coma scale (GCS) Total': 'low',
        'Glucose, mg/dL': 'high',
        'Heart rate': 'high',
        'Hemoglobin A1c, %': 'high',
        'Hemoglobin, g/dL': 'low',
        'Lactate, mg/dL': 'high',
        'Lymphocytes (absolute),  x10E3/uL': 'high',
        'Neutrophils (absolute),  x10E3/uL': 'high',
        'NT pro BNP, pg/mL': 'high',
        'pH': 'low',
        'Platelet count, x10E3/uL': 'low',
        'Potassium, mmol/L': 'high',
        'Procalcitonin, ng/mL': 'high',
        'Respiratory rate': 'high',
        'Sodium, mmol/L': 'high',
        'SpO2': 'low',
        'Systolic blood pressure': 'low',
        'Temperature': 'high',
        'Troponin all types, ng/mL': 'high',
        'White blood cell count,  x10E3/uL': 'high'}
    
    df.sort(['visit_occurrence_id', 'alias', 'harmonized_value_as_number'])

    kept_rows = []
    for l in labs:
        tdf = df[df.alias == l]
        if labs[l] == 'high':
            kept_rows.append(tdf.groupby('visit_occurrence_id').tail(1))
        else:
            kept_rows.append(tdf.groupby('visit_occurrence_id').head(1))

    
    return pd.DataFrame(np.concatenate(kept_rows).flat)

