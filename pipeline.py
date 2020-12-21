import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import functions as F
from pyspark.sql.functions import max, mean, min, stddev, lit, regexp_replace, col
import numpy as np
import pandas as pd
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from sklearn.preprocessing import StandardScaler

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8e7b822f-fcd9-4807-8ddc-c20145ea6c03"),
    inpatient_encoded=Input(rid="ri.foundry.main.dataset.c5883466-0d3a-4934-876d-5f7748950566")
)
def data_by_site( inpatient_encoded):
    df = inpatient_encoded
    sites_with_values = df.groupby('data_partner_id').count()
    return sites_with_values.reset_index()

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4fb71073-2f35-4aa5-bf6f-b293ff9a5da9"),
    inpatient_encoded_all_cols=Input(rid="ri.foundry.main.dataset.7d21dd2e-f5a4-49eb-9c64-8ff5dc24eae4")
)
def data_by_site_all_cols( inpatient_encoded_all_cols):
    df = inpatient_encoded_all_cols
    sites_with_values = df.groupby('data_partner_id').count()
    return sites_with_values.reset_index()

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b9c3c4b5-1370-43da-8b3d-30c3f1928c32"),
    first_day_spo2=Input(rid="ri.foundry.main.dataset.e9f49cdc-7301-4e0d-9872-cf75f013921d")
)
def first_day_spo2_vs_outcome(first_day_spo2):
    #df = first_day_spo2.filter(all_spo2.bad_outcome == True).toPandas()
    df = first_day_spo2.toPandas()
    sns.histplot(data=df,
                 x="harmonized_value_as_number",
                 hue="bad_outcome",
                 bins=50,
                 multiple="dodge")
    plt.show()

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c5883466-0d3a-4934-876d-5f7748950566"),
    inpatient_ml_dataset=Input(rid="ri.foundry.main.dataset.07927bca-b175-4775-9c55-a371af481cc1")
)
def inpatient_encoded(inpatient_ml_dataset):
    inpatient_ml_dataset = inpatient_ml_dataset
    # get rid of ids, columns that are duplicates of other information,
    # or columns that are from the end of stay
    sdf = inpatient_ml_dataset
    sdf = sdf.drop('covid_status_name')
    sdf = sdf.drop('person_id')
    sdf = sdf.drop('visit_concept_id')
    sdf = sdf.drop('visit_concept_name')
    sdf = sdf.drop('in_death_table')
    sdf = sdf.drop('severity_type')
    sdf = sdf.drop('length_of_stay')
    sdf = sdf.drop('ecmo')
    sdf = sdf.drop('aki_in_hospital')
    sdf = sdf.drop('invasive_ventilation')
    sdf = sdf.drop('testcount')
    sdf = sdf.drop('bad_outcome')
    # before this have filtered to patients affected by COVID
    sdf = sdf.drop('positive_covid_test', 'negative_covid_test', 'suspected_covid')

    # these columns are 85% or greater NULL (insurance information)
    sdf = sdf.drop('miscellaneous_program', 'department_of_corrections', 'department_of_defense', 'other_government_federal_state_local_excluding_department_of_corrections', 'no_payment_from_an_organization_agency_program_private_payer_listed', 'medicaid', 'private_health_insurance', 'medicare', 'payer_no_matching_concept')
    # these ones are 100% present, but real values are rare
    sdf = sdf.drop('smoking_status') # only 0.2% have smoking
    sdf = sdf.drop('blood_type')     # only 9% have a value besides unknown
    
    df = sdf.toPandas()

    # actually decided that these columns are 'cheating'
    # fixing columns so they work with sklearn
    # df['visit_start'] = pd.to_datetime(df.visit_start_date).astype('int64')
    # df['visit_end'] = pd.to_datetime(df.visit_end_date).astype('int64')
    df = df.drop(columns=['visit_start_date', 'visit_end_date'])
    
    # remove site so analysis is more generalized across all COVID positive
    # df = pd.concat([df, pd.get_dummies(df.data_partner_id, prefix='site', drop_first=True)], axis=1)
    df = pd.concat([df.drop('gender_concept_name', axis=1), pd.get_dummies(df.gender_concept_name, prefix='gender', drop_first=True)], axis=1)
    df = pd.concat([df.drop('race', axis=1), pd.get_dummies(df.race, prefix='race', drop_first=True)], axis=1)
    df = pd.concat([df.drop('ethnicity', axis=1), pd.get_dummies(df.ethnicity, prefix='ethnicity', drop_first=True)], axis=1)
    #df = pd.concat([df.drop('smoking_status', axis=1), pd.get_dummies(df.smoking_status, prefix='smoking', drop_first=True)], axis=1)
    #df = pd.concat([df.drop('blood_type', axis=1), pd.get_dummies(df.blood_type, prefix='blood_type', drop_first=True)], axis=1)
    #df = pd.concat([df.drop('severity_type', axis=1), pd.get_dummies(df.severity_type, prefix='severity', drop_first=True)], axis=1)

    # these boolean coluumns aren't being treated as boolean
    charlson_cols = ['chf', 'cancer', 'dm', 'dmcx', 'dementia', 'hiv', 'livermild', 'liversevere', 'mi', 'mets', 'pud', 'pvd', 'paralysis', 'pulmonary', 'renal', 'rheumatic', 'stroke']
    df[charlson_cols] = df[charlson_cols].astype('bool')

    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.replace('/', '_')
    df.columns = df.columns.str.lower()
    
    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.7d21dd2e-f5a4-49eb-9c64-8ff5dc24eae4"),
    inpatient_ml_dataset=Input(rid="ri.foundry.main.dataset.07927bca-b175-4775-9c55-a371af481cc1")
)
def inpatient_encoded_all_cols(inpatient_ml_dataset):
    sdf = inpatient_ml_dataset
    df = sdf.toPandas()

    # fixing columns so they work with sklearn
    df['visit_start'] = pd.to_datetime(df.visit_start_date).astype('int64')
    df['visit_end'] = pd.to_datetime(df.visit_end_date).astype('int64')
    df = df.drop(columns=['visit_start_date', 'visit_end_date'])
    
    #dummy code these
    df = pd.concat([df.drop('covid_status_name', axis=1), pd.get_dummies(df.covid_status_name, prefix='cov_status', drop_first=True)], axis=1)
    df = pd.concat([df.drop('gender_concept_name', axis=1), pd.get_dummies(df.gender_concept_name, prefix='gender', drop_first=True)], axis=1)
    df = pd.concat([df.drop('race', axis=1), pd.get_dummies(df.race, prefix='race', drop_first=True)], axis=1)
    df = pd.concat([df.drop('ethnicity', axis=1), pd.get_dummies(df.ethnicity, prefix='ethnicity', drop_first=True)], axis=1)
    df = pd.concat([df.drop('smoking_status', axis=1), pd.get_dummies(df.smoking_status, prefix='smoking', drop_first=True)], axis=1)
    df = pd.concat([df.drop('blood_type', axis=1), pd.get_dummies(df.blood_type, prefix='blood_type', drop_first=True)], axis=1)
    df = pd.concat([df.drop('severity_type', axis=1), pd.get_dummies(df.severity_type, prefix='severity', drop_first=True)], axis=1)

    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.replace('/', '_')
    df.columns = df.columns.str.lower()
    
    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.02362acb-3a3b-4fd6-ad35-677c93bd57da"),
    inpatient_encoded=Input(rid="ri.foundry.main.dataset.c5883466-0d3a-4934-876d-5f7748950566")
)
def inpatient_encoded_w_imputation(inpatient_encoded):
    df = inpatient_encoded
    # remove data_partner_id as it was kept for viewing missing data by site. Not part of the rest of this analysis pipeline
    df = df.drop(columns='data_partner_id')

    df['bnp_pg_ml'] = df['bnp_pg_ml'].fillna(100)
    df['c-reactive_protein_crp_mg_l'] = df['c-reactive_protein_crp_mg_l'].fillna(10)
    df['erythrocyte_sed_rate_mm_hr'] = df['erythrocyte_sed_rate_mm_hr'].fillna(19)
    df['lactate_mm'] = df['lactate_mm'].fillna(13.5)
    df['nt_pro_bnp_pg_ml'] = df['nt_pro_bnp_pg_ml'].fillna(125)
    df['procalcitonin_ng_ml'] = df['procalcitonin_ng_ml'].fillna(0.02)
    df['troponin_all_types_ng_ml'] = df['troponin_all_types_ng_ml'].fillna(0.02)

    df.loc[(df.gender_male == True) & (df.ferritin_ng_ml.isna()), 'ferritin_ng_ml'] = 150
    df.loc[(df.gender_male == False) & (df.gender_other == False) & (df.ferritin_ng_ml.isna()), 'ferritin_ng_ml'] = 75
    
    # fill these with False - now dropped due to already dropping other insurance info
    # df['medicare'] = df['medicare'].fillna(False)
    # df['payer_no_matching_concept'] = df['payer_no_matching_concept'].fillna(False)

    # now fill the rest with the median
    df = df.fillna(df.median())

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.07927bca-b175-4775-9c55-a371af481cc1"),
    inpatient_charlson2=Input(rid="ri.foundry.main.dataset.ddd8560b-e059-42dc-89ed-1ad850bfcc82"),
    inpatient_payer=Input(rid="ri.foundry.main.dataset.d30362c9-a90a-4486-aba9-d67e40c25fd0"),
    inpatient_worst_labs=Input(rid="ri.foundry.main.dataset.c1c6e3b9-83ff-421a-b5c6-75518beec801"),
    inpatients=Input(rid="ri.foundry.main.dataset.a773e078-3908-4189-83a2-2831a8f002f9")
)
def inpatient_ml_dataset(inpatients, inpatient_charlson2, inpatient_worst_labs, inpatient_payer):
    df = inpatients
    cdf = inpatient_charlson2
    ldf = inpatient_worst_labs
    pdf = inpatient_payer
    
    cdf = cdf.withColumnRenamed('person_id', 'p_id')
    ldf = ldf.withColumnRenamed('visit_occurrence_id', 'v_id')
    pdf = pdf.withColumnRenamed('visit_occurrence_id', 'v_id')

    df = df.join(cdf, cdf.p_id == df.person_id, 'left_outer')
    df = df.drop('p_id')

    df = df.join(ldf, ldf.v_id == df.visit_occurrence_id, 'left_outer')
    df = df.drop('v_id')

    df = df.join(pdf, pdf.v_id == df.visit_occurrence_id, 'left_outer')
    df = df.drop('v_id')

    # change all column names to lowercase for easier use
    df = df.select([F.col(x).alias(x.lower()) for x in df.columns])

    return df

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
    pivot_df = df.groupby('visit_occurrence_id').pivot('payer_concept_name').count()
    pivot_df = pivot_df.select(*[col(c) if c == 'visit_occurrence_id' else col(c).cast('boolean') for c in pivot_df.columns])
    pivot_df = pivot_df.drop('null')
    # make this column name more unique and clear
    pivot_df = pivot_df.withColumnRenamed('no_matching_concept', 'payer_no_matching_concept')
    
    return pivot_df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.bc823c17-fcdc-4801-a389-c6f476ed6971"),
    inpatient_encoded_w_imputation=Input(rid="ri.foundry.main.dataset.02362acb-3a3b-4fd6-ad35-677c93bd57da")
)
def inpatient_scaled_w_imputation( inpatient_encoded_w_imputation):
    df = inpatient_encoded_w_imputation
    
    # this column should not be centered/scaled
    visit_occurrence_id = df['visit_occurrence_id']
    df = df.drop(columns='visit_occurrence_id')

    scaler = StandardScaler()
    scaler.fit(df)

    ret_df = pd.DataFrame(scaler.transform(df), columns=df.columns)
    ret_df['visit_occurrence_id'] = visit_occurrence_id
    return ret_df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c1c6e3b9-83ff-421a-b5c6-75518beec801"),
    inpatient_worst_labs_full=Input(rid="ri.foundry.main.dataset.3548767f-6fe1-4ef8-b7c8-1851a0c67aa5")
)
def inpatient_worst_labs(inpatient_worst_labs_full):
    df = inpatient_worst_labs_full
    df = df.select('visit_occurrence_id', 'harmonized_value_as_number', 'alias')
    df = df.withColumn("alias", regexp_replace("alias", "\s+", " "))
    df = df.withColumn("alias", regexp_replace("alias", "[/ ]", "_"))
    df = df.withColumn("alias", regexp_replace("alias", "[.(),]", ""))
    df = df.withColumn("alias", F.lower(col("alias")))
    return df.groupby("visit_occurrence_id").pivot("alias").mean()

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3548767f-6fe1-4ef8-b7c8-1851a0c67aa5"),
    inpatient_labs=Input(rid="ri.foundry.main.dataset.9cf45dff-b77e-4e52-bd3d-2209004983a2")
)
def inpatient_worst_labs_full( inpatient_labs):
    df = inpatient_labs

    # likely will want to adjust this window
    df = df.filter((df.measurement_day_of_visit <= 1) & (df.harmonized_value_as_number.isNotNull()))

    labs = {
        'Albumin (g/dL)': 'high',
        'ALT (SGPT), IU/L': 'high',
        'AST (SGOT), IU/L': 'high',
        'Bilirubin (Conjugated/Direct), mg/dL': 'high',
        'Bilirubin (total), mg/dL': 'high',
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
        'Glasgow coma scale (GCS) Total': 'low', # currently (Dec 15, 2020) has no harmonized values
        'Glucose, mg/dL': 'high',
        'Heart rate': 'high',
        'Hemoglobin A1c, %': 'high',
        'Hemoglobin, g/dL': 'low',
        'Lactate, mM': 'high',
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

    # https://sparkbyexamples.com/spark/spark-dataframe-how-to-select-the-first-row-of-each-group/
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
    Output(rid="ri.foundry.main.dataset.dc2883cb-cece-46a4-b4ce-c7cac6de202f"),
    inpatient_encoded=Input(rid="ri.foundry.main.dataset.c5883466-0d3a-4934-876d-5f7748950566"),
    missing_charlson=Input(rid="ri.foundry.main.dataset.3d4fe549-dc8b-4e60-a33a-3d0241371517"),
    missing_gen_eth_race=Input(rid="ri.foundry.main.dataset.67690235-78c7-45b2-b9e7-bc569d5c741e")
)
def missing_data_info(inpatient_encoded, missing_charlson, missing_gen_eth_race):
    df = inpatient_encoded
    cdf = missing_charlson.toPandas()
    odf = missing_gen_eth_race.toPandas()
    
    missing_df = df.isnull().sum().to_frame()
    missing_df = missing_df.rename(columns = {0:'null_count'})
    missing_df['pct_missing'] = missing_df['null_count'] / df.shape[0]
    missing_df['pct_present'] = round((1 - missing_df['null_count'] / df.shape[0]) * 100, 1)
    missing_df = missing_df.reset_index()
    missing_df = missing_df.rename(columns = {'index':'variable'})
    missing_df = missing_df.sort_values('pct_missing', ascending=False)

    # charlson items are treated as one group
    missing_df.drop(missing_df.variable.isin(['cancer', 'chf', 'dementia', 'dm', 'dmcx', 'gender_other', 'hiv', 'livermild', 'liversevere', 'mets', 'mi', 'paralysis', 'pud', 'pulmonary', 'pvd', 'q_score', 'renal', 'rheumatic', 'stroke']), inplace=True)
    temp_df = {'variable': 'charlson',
               'null_count': None,
               'pct_missing': cdf.shape[0] /  df.shape[0],
               'pct_present': 1 - (cdf.shape[0] /  df.shape[0])} 
    missing_df = dmissing_dff.append(temp_df, ignore_index = True)

    # calculate gender % missing
    pct_miss = odf[(odf.concept_name == 'gender_concept_name') & (odf.value == 'Other')].rec_count() /  df.shape[0]
    temp_df = {'variable': 'gender_other',
               'null_count': None,
               'pct_missing': pct_miss,
               'pct_present': 1 - pct_miss} 
    missing_df = dmissing_dff.append(temp_df, ignore_index = True)

    # calculate race % missing
    pct_miss = odf[(odf.concept_name == 'race') & (odf.value == 'Missing/Unknown')].rec_count() /  df.shape[0]
    temp_df = {'variable': 'race_missing_unknown',
               'null_count': None,
               'pct_missing': pct_miss,
               'pct_present': 1 - pct_miss} 
    missing_df = dmissing_dff.append(temp_df, ignore_index = True)

    # calculate ethnicity % missing
    pct_miss = odf[(odf.concept_name == 'ethnicity') & (odf.value == 'Missing/Unknown')].rec_count() /  df.shape[0]
    temp_df = {'variable': 'ethnicity_missing_unknown',
               'null_count': None,
               'pct_missing': pct_miss,
               'pct_present': 1 - pct_miss} 
    missing_df = dmissing_dff.append(temp_df, ignore_index = True)

    return missing_df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.59196ca8-6a20-4767-be0c-466bac5e44d8"),
    inpatient_encoded_all_cols=Input(rid="ri.foundry.main.dataset.7d21dd2e-f5a4-49eb-9c64-8ff5dc24eae4")
)
def missing_data_info_all_cols(inpatient_encoded_all_cols):
    df = inpatient_encoded_all_cols
    missing_df = df.isnull().sum().to_frame()
    missing_df = missing_df.rename(columns = {0:'null_count'})
    missing_df['pct_missing'] = missing_df['null_count'] / df.shape[0]
    missing_df = missing_df.reset_index()
    missing_df = missing_df.rename(columns = {'index':'variable'})
    missing_df = missing_df.sort_values('pct_missing', ascending=False)
    return missing_df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.75db6e21-1621-4a49-a3be-0eebb4992ff0"),
    inpatient_ml_dataset=Input(rid="ri.foundry.main.dataset.07927bca-b175-4775-9c55-a371af481cc1")
)
def missing_orig(inpatient_ml_dataset):
    inpatient_ml_dataset = inpatient_ml_dataset
    df = inpatient_ml_dataset.toPandas()
    missing_df = df.isnull().sum().to_frame()
    missing_df = missing_df.rename(columns = {0:'null_count'})
    missing_df['pct_missing'] = missing_df['null_count'] / df.shape[0]
    missing_df['pct_present'] = round((1 - missing_df['null_count'] / df.shape[0]) * 100, 1)
    missing_df = missing_df.reset_index()
    missing_df = missing_df.rename(columns = {'index':'variable'})
    missing_df = missing_df.sort_values('pct_missing', ascending=False)
    return missing_df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.349f1404-e60e-4a76-9a32-13fe06198cc1"),
    inpatient_ml_dataset=Input(rid="ri.foundry.main.dataset.07927bca-b175-4775-9c55-a371af481cc1")
)
def outcomes(inpatient_ml_dataset):
    df = inpatient_ml_dataset
    df = df.select('visit_occurrence_id',
                   'person_id',
                   'data_partner_id',
                   'visit_concept_name',
                   'covid_status_name',
                   'in_death_table',
                   'severity_type',
                   'length_of_stay',
                   'visit_start_date',
                   'visit_end_date',
                   'ecmo',
                   'aki_in_hospital',
                   'invasive_ventilation',
                   'testcount',
                   'bad_outcome')
    return df.toPandas()

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.76b6c65a-ff64-424e-b7cb-53c498d7d2fe"),
    all_spo2=Input(rid="ri.foundry.main.dataset.0a5b82ab-f317-4bf0-824a-87bebf4a4b3b")
)
def spo2_vs_good_outcome(all_spo2):
    df = all_spo2.filter(all_spo2.bad_outcome == False).toPandas()
    sns.histplot(data=df,
                 x="harmonized_value_as_number",
                 hue="bad_outcome",
                 bins=50)
    plt.show()

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.dabbe334-5116-4921-a794-e3f22bdadd8d"),
    all_spo2=Input(rid="ri.foundry.main.dataset.0a5b82ab-f317-4bf0-824a-87bebf4a4b3b")
)
def spo2_vs_outcome(all_spo2):
    #df = all_spo2.filter(all_spo2.bad_outcome == True).toPandas()
    df = all_spo2.toPandas()
    sns.histplot(data=df,
                 x="harmonized_value_as_number",
                 hue="bad_outcome",
                 bins=50,
                 multiple="dodge")
    plt.show()

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.09859ea6-d0cc-448a-8fb8-141705a5e951"),
    test_lab_filter=Input(rid="ri.foundry.main.dataset.b67797ec-1918-43d6-9a25-321582987d38")
)
def worst_lab_pd(test_lab_filter):
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

    #return spark.createDataFrame(pd.DataFrame(np.concatenate(kept_rows).flat))

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9e7caa2b-3453-41a4-a536-542e3ddeac3c"),
    inpatient_ml_dataset=Input(rid="ri.foundry.main.dataset.07927bca-b175-4775-9c55-a371af481cc1")
)
def worst_spo2_vs_outcome(inpatient_ml_dataset):
    df = inpatient_ml_dataset.toPandas()
    p = sns.histplot(data=df,
                     x="spo2",
                     hue="bad_outcome",
                     bins=50,
                     multiple="dodge")
    p.set_yscale("log")
    plt.show()

