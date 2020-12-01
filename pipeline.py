import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import functions as F
from pyspark.sql.functions import max, mean, min, stddev, lit, regexp_replace, col
import numpy as np
import pandas as pd
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

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
    Output(rid="ri.foundry.main.dataset.3548767f-6fe1-4ef8-b7c8-1851a0c67aa5"),
    inpatient_labs=Input(rid="ri.foundry.main.dataset.9cf45dff-b77e-4e52-bd3d-2209004983a2")
)
def inpatient_worst_labs( inpatient_labs):
    df = inpatient_labs

    # likely will want to adjust this window
    df = df.filter((df.measurement_day_of_visit <= 1) & (df.harmonized_value_as_number.isNotNull()))

    labs = {'ALT (SGPT), IU/L': 'high',
        'AST (SGOT), IU/L': 'high',
        #'Blood type (ABO + Rh)': 'categorical', - only 39 total records in lab table - use inpatient table instead
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

    kept_rows = None
    for l in labs:
        ldf = df.filter(df['alias'] == l)
        if labs[l] == 'high':
            windowspec  = Window.partitionBy('visit_occurrence_id').orderBy(F.desc('harmonized_value_as_number'))
        else:
            windowspec  = Window.partitionBy('visit_occurrence_id').orderBy('harmonized_value_as_number')

        gldf = ldf.withColumn('row_number', row_number().over(windowspec)).filter(col('row_number') == 1).drop('row_number')

        if kept_rows is None:
            kept_rows = gldf
        else:
            kept_rows = gldf.union(kept_rows)

    return kept_rows

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c1c6e3b9-83ff-421a-b5c6-75518beec801"),
    inpatient_worst_labs=Input(rid="ri.foundry.main.dataset.3548767f-6fe1-4ef8-b7c8-1851a0c67aa5")
)
def inpatient_worst_labs_pivoted(inpatient_worst_labs):
    df = inpatient_worst_labs
    df = df.select('visit_occurrence_id', 'harmonized_value_as_number', 'alias')
    dfg = df.groupby("visit_occurrence_id").pivot("alias")
    return dfg

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.09859ea6-d0cc-448a-8fb8-141705a5e951"),
    inpatient_labs=Input(rid="ri.foundry.main.dataset.9cf45dff-b77e-4e52-bd3d-2209004983a2"),
    test_lab_filter=Input(rid="ri.foundry.main.dataset.b67797ec-1918-43d6-9a25-321582987d38")
)
def worst_lab_pd(test_lab_filter, inpatient_labs):
    df = test_lab_filter
    # df = inpatient_labs

    # likely will want to adjust this window
    df = df.filter(df.measurement_day_of_visit <= 1)

    labs = {'ALT (SGPT), IU/L': 'high',
        'AST (SGOT), IU/L': 'high',
        'Blood type (ABO + Rh)': 'categorical', # this is a unique case will need to investigate how to handle
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

    df = df.sort(['visit_occurrence_id', 'alias', 'harmonized_value_as_number'])

    kept_rows = []
    for l in labs:
        tdf = df.filter(df['alias'] == l).toPandas()
        if (len(tdf) > 0):
            if labs[l] == 'high':
                kept_rows.append(tdf.groupby('visit_occurrence_id', as_index=False).last().to_dict(orient="records"))
            else:
                kept_rows.append(tdf.groupby('visit_occurrence_id', as_index=False).first().to_dict(orient="records"))

    return pd.DataFrame(np.concatenate(kept_rows).flat)

