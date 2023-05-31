
# Time to Event -----------------------
timeToInitialTreatmentData <- function(executionSettings,
                                       treatmentHistory,
                                       eraCollapseSize,
                                       eventType,
                                       targetCohorts,
                                       strataNames,
                                       outputFolder) {


  ## Set variables
  if (executionSettings$connectionDetails$dbms == "snowflake") {
    writeSchema <- paste(executionSettings$writeDatabase, executionSettings$writeSchema, sep = ".")
    cdmSchema <- paste(executionSettings$cdmDatabase, executionSettings$cdmSchema, sep = ".")
  } else {
    writeSchema <- executionSettings$writeSchema
    cdmSchema <- executionSettings$cdmSchema
  }

  cohortTable <- paste(executionSettings$cohortTable, executionSettings$databaseId, sep = "_")
  strataTable <- paste(executionSettings$cohortTable, "strata", configBlock, sep = "_")
  targetCohortId <- as.double(targetCohorts$id)


  ## Get strata cohorts
  sql <- "SELECT * FROM @write_schema.@strata_table
          WHERE cohort_definition_id = @target_cohort_id;"  %>%
    SqlRender::render(
      write_schema = writeSchema,
      strata_table = strataTable,
      target_cohort_id = targetCohortId
    ) %>%
    SqlRender::translate(executionSettings$connectionDetails$dbms)

  strataTbl <-  DatabaseConnector::querySql(connection = con, sql = sql)

  colnames(strataTbl) <- tolower(colnames(strataTbl))

  strataTbl <- strataTbl %>%
    dplyr::mutate(person_id = as.double(subject_id)) %>%
    dplyr::select(-subject_id)


  ## Get target cohort
  sql <- "SELECT * FROM @write_schema.@cohort_table
          WHERE cohort_definition_id = @target_cohort_id;"  %>%
    SqlRender::render(
      write_schema = writeSchema,
      cohort_table = cohortTable,
      target_cohort_id = targetCohortId
    ) %>%
    SqlRender::translate(executionSettings$connectionDetails$dbms)

  targetTbl <-  DatabaseConnector::querySql(connection = con, sql = sql)

  colnames(targetTbl) <- tolower(colnames(targetTbl))

  targetTbl  <- targetTbl %>%
    dplyr::mutate(person_id = as.double(subject_id)) %>%
    dplyr::select(-subject_id)


  ## Get treatment history table
  treatmentHistory <- arrow::read_parquet(file = here::here("output", "06_treatmentHistory",
                                                            executionSettings$databaseId, targetCohorts$name,
                                                            paste0("th_", eventType, "_", eraCollapseSize, ".parquet"))) %>%
    tibble::as_tibble() %>%
    dplyr::filter(event_seq == 1) %>%
    dplyr::mutate(person_id = as.double(person_id))


  ## Get strata and stratum names
  strataManifest <- readr::read_csv(here::here("output", "02_buildStrata", executionSettings$databaseId, "strataManifest.csv"),
                                 show_col_types = FALSE) %>%
    dplyr::filter(cohort_definition_id == targetCohortId)

  strataNames <- unique(strataManifest$strata_name)

  ## Prepare tte list
  ttit <- targetTbl %>%
    dplyr::left_join(treatmentHistory, by = c("person_id"), multiple = "all") %>%
    dplyr::mutate(
      event = dplyr::if_else(cohort_end_date > event_start_date, 1L, 0L, 0L),
      time = dplyr::if_else(
        event == 1,
        event_start_date - cohort_start_date,
        cohort_end_date - cohort_start_date,
        cohort_end_date - cohort_start_date
      ),
      time = as.double(time),
      time_years = as.double(time) / 365.25,
      type = dplyr::if_else(
        grepl("\\+", event_cohort_name), "combo", event_cohort_name
      )
    ) %>%
    tidyr::replace_na(list(type = "none")) %>%
    dplyr::select(person_id, event, time, time_years, type) %>%
    dplyr::left_join(strataTbl, by = c("person_id"), multiple = "all") %>%
    dplyr::left_join(strataManifest, by = c("strata_id", "strata", "cohort_definition_id")) %>%
    dplyr::mutate(typeAll = paste0(type, ": ", stratumFullName)) %>%
    dplyr::group_by(strata_id) %>%
    dplyr::group_split() %>%
    purrr::set_names(nm = strataNames)


  ## Create directory
  outputFolder <- fs::path(outputFolder, targetCohorts$name)
  fs::dir_create(outputFolder)


  ## Save output - rds
  save_path <- fs::path(outputFolder, paste0("tte_", eventType, "_", eraCollapseSize, ".rds"))
  readr::write_rds(x = ttit, file = save_path)


  cli::cat_line("\nTime to initial treatment for cohort ", targetCohorts$name, " run at: ", Sys.time())
  cli::cat_line("\nSaved to: ", save_path)
  cli::cat_line("Connection Closed\n\n")

  return(ttit)

}
