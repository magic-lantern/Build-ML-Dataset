

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a360f84f-afcf-41b0-b7f3-9072858d7ad3"),
    Death_unique_persons=Input(rid="ri.foundry.main.dataset.2b2e338d-728b-4878-9c8a-05268426926f"),
    inpatients=Input(rid="ri.foundry.main.dataset.a773e078-3908-4189-83a2-2831a8f002f9")
)
inpatient_death <- function(Death_unique_persons, inpatients) {
    df <- inpatients
    dfDeath <- SparkR::select(death_unique_persons, c("person_id")) 
    dfDeath <- withColumn(dfDeath, "isHit", 'yes')

    # Select death event if present 
    dfDeath <- SparkR::withColumnRenamed(dfDeath, "person_id", "p_id")
    dfPositive <- SparkR::join(df, dfDeath, (df$person_id == dfDeath$p_id), "left_outer")
    dfPositive <- SparkR::drop(dfPositive, "p_id")

    # separate out the selected from the non selected
    dfNotPositive <- SparkR::filter(dfPositive, isNull(dfPositive$isHit) ) 
    dfPositive <- SparkR::filter(dfPositive, isNotNull(dfPositive$isHit) ) 
    
    # purge older visits from the people who died.
    dfOlderVisitsPurged <- dfPositive
    ws <- SparkR::orderBy(SparkR::windowPartitionBy("person_id"), "visit_start_date")

    # rank visits by start, pick the last visit, i.e. the one with the highest ranking (last) date
    dfOlderVisitsPurged <- dfOlderVisitsPurged %>%
        SparkR::mutate(visit_start_date_rank = SparkR::over(SparkR::row_number(), ws))%>%
        SparkR::filter(SparkR::column("visit_start_date_rank") == 1)

    dfPositivePurged <- dfOlderVisitsPurged %>%
        SparkR::drop("visit_start_date_rank")

    # combine and remove helper columns
    dfCombined <- SparkR::rbind(dfNotPositive, dfPositivePurged)
    #dfCombined <- SparkR::drop(dfCombined, "isHit")
    return(dfCombined)
}
}

