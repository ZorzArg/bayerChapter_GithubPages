# Internals for Clinical Characteristics ------------------


## Demographics ---------------------

demographicBaseline <- function(executionSettings,
                                con,
                                targetCohorts,
                                outputFolder) {


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

  #create directory if it doesnt exist
  outputFolder <- fs::path(outputFolder, targetCohorts$name)
  fs::dir_create(outputFolder)

  tik <- Sys.time()

  # Create Demographic settings
  cov_settings <- FeatureExtraction::createCovariateSettings(
    useDemographicsGender = TRUE,
    useDemographicsAgeGroup = TRUE,
    useDemographicsRace = TRUE,
    useDemographicsIndexYear = TRUE
  )

  quietCov <- purrr::quietly(FeatureExtraction::getDbCovariateData)
  cov <- quietCov(
    connection = con,
    cdmDatabaseSchema = cdmSchema,
    cohortTable = cohortTable,
    cohortDatabaseSchema = writeSchema,
    cohortId = targetCohortId,
    covariateSettings = cov_settings
  )$result

  tok <- Sys.time()
  tdif <- tok - tik
  tok_format <- paste(scales::label_number(0.01)(as.numeric(tdif)), attr(tdif, "units"))
  cli::cat_line("\nDemographics Covariates built at: ", tok)
  cli::cat_line("\nCovariate build took: ", tok_format)

  ### get s
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
   dplyr::mutate(subject_id = as.double(subject_id)) %>%
   dplyr::filter(cohort_definition_id == targetCohortId) %>%
   dplyr::group_by(strata_id, strata) %>%
   dplyr::mutate(total = n()) %>%
   dplyr::ungroup()


  ### Get covariate table for race and gender
  covTblA <- cov$covariates %>%
    dplyr::left_join(cov$covariateRef, by = c("covariateId")) %>%
    dplyr::filter(analysisId %in% c(1,4)) %>%
    dplyr::mutate(rowId = as.double(rowId)) %>%
    dplyr::select(rowId, covariateValue, conceptId, analysisId) %>%
    dplyr::collect()

  ### Get covariate table for age group
  covTblB <- cov$covariates %>%
    dplyr::left_join(cov$covariateRef, by = c("covariateId")) %>%
    dplyr::filter(analysisId == 3) %>%
    dplyr::mutate(
      rowId = as.double(rowId),
      conceptId = dbplyr::sql("CAST(SUBSTRING(covariateId, 0, LENGTH(covariateId) - 4) AS INT)")
    ) %>%
    dplyr::select(rowId, covariateValue, conceptId, analysisId) %>%
    dplyr::collect()

  ### Get covariate table for year
  covTblC <- cov$covariates %>%
    dplyr::left_join(cov$covariateRef, by = c("covariateId")) %>%
    dplyr::filter(analysisId == 6) %>%
    dplyr::mutate(
      rowId = as.double(rowId),
      conceptId = dbplyr::sql("CAST(SUBSTRING(covariateId, 0, LENGTH(covariateId) - 4) AS INT)")
    ) %>%
    dplyr::select(rowId, covariateValue, conceptId, analysisId) %>%
    dplyr::collect()

  ### Bind all covariate tables
  covTbl <- dplyr::bind_rows(
    covTblA, covTblB, covTblC
  )

  ### Format output
  demoTbl <- covTbl %>%
    dplyr::left_join(strataTbl, by = c("rowId" = "subject_id"), multiple = "all") %>%
    dplyr::group_by(analysisId, conceptId, strata_id, strata, total) %>%
    dplyr::summarize(nn = sum(covariateValue, na.rm = TRUE)) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(
      window = "Baseline",
      pct = nn / total,
      conceptName = dplyr::case_when(
        conceptId == 8507 ~ "Male",
        conceptId == 8532 ~ "Female",
        conceptId == 1 ~ "5 - 9",
        conceptId == 2 ~ "10 - 14",
        conceptId == 3 ~ "15 - 19",
        conceptId == 4 ~ "20 - 24",
        conceptId == 5 ~ "25 - 29",
        conceptId == 6 ~ "30 - 34",
        conceptId == 7 ~ "35 - 39",
        conceptId == 8 ~ "40 - 44",
        conceptId == 9 ~ "45 - 49",
        conceptId == 10 ~ "50 - 54",
        conceptId == 11 ~ "55 - 59",
        conceptId == 12 ~ "60 - 64",
        conceptId == 13 ~ "65 - 69",
        conceptId == 14 ~ "70 - 74",
        conceptId == 15 ~ "75 - 79",
        conceptId == 16 ~ "80 - 84",
        conceptId == 17 ~ "85 - 89",
        conceptId == 18 ~ "90 - 94",
        conceptId == 19 ~ "95 - 99",
        conceptId == 20 ~ "100 - 104",
        conceptId == 21 ~ "105 - 109",
        conceptId == 22 ~ "110 - 114",
        conceptId == 23 ~ "115 - 119",
        conceptId == 8657 ~ "American Indian or Alaska Native",
        conceptId == 8515 ~ "Asian",
        conceptId == 8516 ~ "Black or African American",
        conceptId == 8557 ~ "Native Hawaiian or Other Pacific Islander",
        conceptId == 8527 ~ "White",
        TRUE ~ as.character(conceptId)
      ),
      categoryName = dplyr::case_when(
        analysisId == 1 ~ "Gender",
        analysisId == 3 ~ "5yr Age Group",
        analysisId == 4 ~ "Race",
        analysisId == 6 ~ "Index Year"
      ),
      categoryId = analysisId
    ) %>%
    dplyr::select(categoryId, categoryName, conceptId, conceptName, window, strata_id, strata, nn, total, pct) %>%
    dplyr::arrange(categoryId) %>%
    dplyr::mutate(database = executionSettings$databaseId,
                  cohort = targetCohorts$name)

  ### Save output
  save_path <- fs::path(outputFolder, "demographics_baseline.csv")
  readr::write_csv(demoTbl, file = save_path)
  cli::cat_line("\nDemographic Baseline run at: ", Sys.time())
  cli::cat_line("\nSaved to: ", save_path)
  return(demoTbl)

}


## Continuous ----------------

continuousBaseline <- function(executionSettings,
                               con,
                               targetCohorts,
                               outputFolder) {


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

  #create directory if it doesnt exist
  outputFolder <- fs::path(outputFolder, targetCohorts$name)
  fs::dir_create(outputFolder)

  #preset feature extraction for continuous
  cov_settings <- FeatureExtraction::createCovariateSettings(
    useDemographicsAge = TRUE,
    useDemographicsPriorObservationTime = TRUE,
    useDemographicsPostObservationTime = TRUE,
    useDemographicsTimeInCohort = TRUE,
    useDcsi = TRUE
  )

  #get covariate data
  tik <- Sys.time()

  quietCov <- purrr::quietly(FeatureExtraction::getDbCovariateData)
  cov <- quietCov(
    connection = con,
    cdmDatabaseSchema = cdmSchema,
    cohortTable = cohortTable,
    cohortDatabaseSchema = writeSchema,
    cohortId = targetCohortId,
    covariateSettings = cov_settings
  )$result

  tok <- Sys.time()
  tdif <- tok - tik
  tok_format <- paste(scales::label_number(0.01)(as.numeric(tdif)), attr(tdif, "units"))
  cli::cat_line("\nContinuous Covariates built at: ", tok)
  cli::cat_line("\nCovariate build took: ", tok_format)

  ### Get strata table
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
    dplyr::mutate(subject_id = as.double(subject_id)) %>%
    dplyr::filter(cohort_definition_id == targetCohortId) %>%
    dplyr::ungroup() %>%
    dplyr::collect()

  ### Handle Continuous Age
  covTbl <- cov$covariates %>%
    dplyr::left_join(cov$covariateRef, by = c("covariateId")) %>%
    dplyr::filter(analysisId %in% c(2, 8, 9, 10, 902)) %>%
    dplyr::collect()

  ### Format output
  ctsTbl <- covTbl %>%
    dplyr::left_join(strataTbl, by = c("rowId" = "subject_id"), multiple = "all") %>%
    dplyr::group_by(analysisId, conceptId, strata_id, strata) %>%
    dplyr::summarize(
      min = min(covariateValue),
      p25 = quantile(covariateValue, 0.25),
      sd = sd(covariateValue),
      mean = mean(covariateValue),
      median = median(covariateValue),
      IQR = quantile(covariateValue, 0.75) - quantile(covariateValue, 0.25),
      p75 = quantile(covariateValue, 0.75),
      max = max(covariateValue)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(
      window = "Baseline",
      categoryName = case_when(
        analysisId == 2 ~ "Age",
        analysisId == 8 ~ "Prior Observation Time",
        analysisId == 9 ~ "Post Observation Time",
        analysisId == 10 ~ "Time In Cohort",
        analysisId == 902 ~ "DSCI",
        TRUE ~ as.character(analysisId)),
      categoryId = analysisId
    ) %>%
    dplyr::select(categoryId, categoryName, strata_id, strata, window, min, p25, median, mean, sd, IQR, p75, max) %>%
    dplyr::arrange(categoryId) %>%
    dplyr::mutate(database = executionSettings$databaseId,
                  cohort = targetCohorts$name)

  ### Save output
  save_path <- fs::path(outputFolder, "continuous_baseline.csv")
  readr::write_csv(ctsTbl, file = save_path)
  cli::cat_line("\nContinuous Baseline run at: ", Sys.time())
  cli::cat_line("\nSaved to: ", save_path)

  return(ctsTbl)

}

## Drugs ----------------

drugsCovariates <- function(executionSettings,
                            con,
                            targetCohorts,
                            timeA,
                            timeB,
                            outputFolder) {


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

  #create directory if it doesnt exist
  outputFolder <- fs::path(outputFolder, targetCohorts$name)
  fs::dir_create(outputFolder)

  #preset feature extraction for continuous
  cov_settings <- FeatureExtraction::createCovariateSettings(
    useDrugGroupEraLongTerm = TRUE,
    excludedCovariateConceptIds = c(21600001, 21600959, 21601237, # Remove ATC 1st class
                                    21601907, 21602359, 21602681,
                                    21602795, 21601386, 21603931,
                                    21604180, 21604847, 21605007,
                                    21603550, 21605212),
    longTermStartDays = timeA,
    endDays = timeB
  )

  #get covariate data
  tik <- Sys.time()
  quietCov <- purrr::quietly(FeatureExtraction::getDbCovariateData)
  cov <- quietCov(
    connection = con,
    cdmDatabaseSchema = cdmSchema,
    cohortTable = cohortTable,
    cohortDatabaseSchema = writeSchema,
    cohortId = targetCohortId,
    covariateSettings = cov_settings
  )$result

  tok <- Sys.time()
  tdif <- tok - tik
  tok_format <- paste(scales::label_number(0.01)(as.numeric(tdif)), attr(tdif, "units"))
  cli::cat_line("\nDrug Covariates built at: ", tok)
  cli::cat_line("\nCovariate build took: ", tok_format)


  ### Get strata table
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
    dplyr::mutate(subject_id = as.double(subject_id)) %>%
    dplyr::filter(cohort_definition_id == targetCohortId) %>%
    dplyr::group_by(strata_id, strata) %>%
    dplyr::mutate(total = n()) %>%
    dplyr::ungroup() %>%
    dplyr::collect()


  ### Get covariate table for drugs
  covTbl <- cov$covariates %>%
    dplyr::left_join(cov$covariateRef, by = c("covariateId")) %>%
    dplyr::filter(analysisId == 410) %>%
    dplyr::mutate(rowId = as.double(rowId)) %>%
    dplyr::select(rowId, covariateValue, conceptId) %>%
    dplyr::collect()

  ### Get unique concept ids
  conceptIds <- cov$covariateRef %>%
    dplyr::filter(analysisId == 410) %>%
    dplyr::select(conceptId) %>%
    dplyr::mutate(conceptId = as.double(conceptId)) %>%
    dplyr::collect() %>%
    dplyr::pull()

  ### Find atc rollup

  drugSql <-
    "with drug as ( -- define disease categories similar to ICD10 Chapters
      select row_number() over (order by concept_code) as precedence, concept_name as category_name, concept_id as category_id
      from @cdmDatabaseSchema.concept
      where vocabulary_id='ATC' and concept_class_id='ATC 2nd'
    )
    select distinct -- get the disease category with the lowest (best fitting) precedence, or assign 'Other Condition'
      concept_id as drug_id, concept_name as drug_name,
      first_value(coalesce(category_id, 0)) over (partition by concept_id order by precedence nulls last) as category_id,
      first_value(coalesce(category_name, 'Other Drug')) over (partition by concept_id order by precedence nulls last) as category_name
    from @cdmDatabaseSchema.concept
    left join ( -- find the approprate drug category, if possible
      select descendant_concept_id, category_id, category_name, precedence
      from @cdmDatabaseSchema.concept_ancestor
      join drug on ancestor_concept_id=category_id
    ) d on descendant_concept_id=concept_id
    where concept_id in (@conceptIds) -- place here the concept_ids you want to roll up (have to be standard SNOMED)
    ;"

  cli::cat_line("\nRolling up drug concepts to ATC 2nd class using: ")
  cli::cat_line("\n", drugSql, "\n")


  atc_grp <- DatabaseConnector::renderTranslateQuerySql(
    connection = con,
    sql = drugSql,
    snakeCaseToCamelCase = TRUE,
    cdmDatabaseSchema = cdmSchema,
    conceptIds = conceptIds
  ) %>%
    dplyr::mutate(categoryName = stringr::str_to_title(categoryName)) %>%
    dplyr::rename(conceptId = drugId, conceptName = drugName)


  ### Format output
  drugTbl <- covTbl %>%
    dplyr::left_join(strataTbl, by = c("rowId" = "subject_id"), multiple = "all") %>%
    dplyr::group_by(conceptId, strata_id, strata, total) %>%
    dplyr::summarize(nn = sum(covariateValue, na.rm = TRUE)) %>%
    dplyr::ungroup() %>%
    dplyr::left_join(atc_grp, by = c("conceptId")) %>%
    dplyr::mutate(
      window = paste(timeA, timeB, sep = " : "),
      pct = nn / total) %>%
    dplyr::select(categoryId, categoryName, conceptId, conceptName, strata_id, strata, nn, total, pct, window) %>%
    dplyr::arrange(categoryId) %>%
    dplyr::mutate(database = executionSettings$databaseId,
                  cohort = targetCohorts$name)

  ### Save output
  fname <- paste("drugs", abs(timeA), abs(timeB), sep = "_")
  save_path <- fs::path(outputFolder, fname , ext = "csv")
  readr::write_csv(drugTbl, file = save_path)
  cli::cat_line("\nDrug Covariates at window ", paste0(timeA, " to ", timeB),  " run at: ", Sys.time())
  cli::cat_line("\nSaved to: ", save_path)

  return(drugTbl)

}



## Cohort Covariates ---------------
cohortCovariates <- function(executionSettings,
                             con,
                             targetCohorts,
                             covariateKey,
                             timeA,
                             timeB,
                             outputFolder,
                             printSql = FALSE) {

  cli::cat_rule()
  cli::cat_line("Generating Cohort Covariates")
  id_print <- paste(covariateKey$id, collapse = ",")
  t_print <- paste0(timeA, " to " , timeB)
  cli::cat_line("\tCohort name: ", targetCohorts$name)
  cli::cat_line("\tUsing covariate ids: ", id_print)
  cli::cat_line("\tAt window ", t_print)


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

  #create directory if it doesnt exist
  outputFolder <- fs::path(outputFolder, targetCohorts$name)
  fs::dir_create(outputFolder)


  cohortSql <- "
  WITH T1 AS (
    SELECT *
    FROM @writeSchema.@cohortTable
    WHERE cohort_definition_id = @cohortId
  ),
  T2 AS (
    SELECT
      T1.*,
      rhs.strata_id,
      rhs.strata,
      rhs.total
    FROM T1
    LEFT JOIN (
      SELECT
      *,
      COUNT(*) OVER (PARTITION BY strata_id, strata) AS total
      FROM (
        SELECT *
        FROM @writeSchema.@strataTable
        WHERE cohort_definition_id = @cohortId
      ) AS tt
    ) rhs
    ON (T1.subject_id = rhs.subject_id AND T1.cohort_definition_id = rhs.cohort_definition_id)
  ),
  T3 AS (
    SELECT
      T2.*,
      rhs.cohort_definition_id AS cohort_covariate_id,
      rhs.cohort_start_date AS covariate_start_date
    FROM T2
    INNER JOIN (
      SELECT *
      FROM @writeSchema.@cohortTable
      WHERE cohort_definition_id IN (@covariateIds)
    ) rhs
    ON (T2.subject_id = rhs.subject_id)
  ),
  T4 AS (
    SELECT
      *,
      DATEADD(day, @timeA, cohort_start_date) AS a,
      DATEADD(day, @timeB, cohort_start_date) AS b
    FROM T3
  ),
  T5 AS (
    SELECT
      *,
      (CASE WHEN covariate_start_date BETWEEN a AND b THEN 1 ELSE 0 END) AS hit
    FROM T4
  )
  SELECT
    cohort_covariate_id,
    strata_id,
    strata,
    total,
    SUM(hit) AS nn
  FROM T5
  GROUP BY
    cohort_covariate_id,
    strata_id,
    strata,
    total;"

  if (printSql) {
    cli::cat_line("With sql:\n", cohortSql, "\n")
  }


  tik <- Sys.time()

  covTbl <- SqlRender::render(
    cohortSql,
    writeSchema = writeSchema,
    cohortTable = cohortTable,
    strataTable = strataTable,
    cohortId = targetCohortId,
    covariateIds = covariateKey$id,
    timeA = timeA,
    timeB = timeB) %>%
    SqlRender::translate(targetDialect = executionSettings$connectionDetails$dbms) %>%
    DatabaseConnector::querySql(connection = con, sql = .) %>%
    tibble::as_tibble() %>%
    dplyr::rename_with(~tolower(.x)) %>%
    dplyr::mutate(
      pct = nn / total
    )

  tok <- Sys.time()
  tdif <- tok - tik
  tok_format <- paste(scales::label_number(0.01)(as.numeric(tdif)), attr(tdif, "units"))
  cli::cat_line("\nCohort Covariates built at: ", tok)
  cli::cat_line("\nCovariate build took: ", tok_format)


  ### Format output
  formatted_tbl <- covTbl %>%
    dplyr::left_join(covariateKey, by = c("cohort_covariate_id" = "id")) %>%
    dplyr::mutate(
      cohortCovariateId = as.double(cohort_covariate_id),
      cohortCovariateName = name,
      strataId = as.double(strata_id),
      strata = as.double(strata),
      window = paste(timeA, timeB, sep = " : ")
    ) %>%
    dplyr::select(cohortCovariateId, cohortCovariateName, strataId, strata, window, nn, total, pct) %>%
    dplyr::mutate(database = executionSettings$databaseId,
                  cohort = targetCohorts$name)


  ### Save output for all cohort covariates
  fname <- paste("cohort_covariates", abs(timeA), abs(timeB), sep = "_")
  save_path <- fs::path(outputFolder, fname , ext = "csv")
  readr::write_csv(formatted_tbl, file = save_path)
  cli::cat_line("\nCohort Covariates at window ", paste0(timeA, " to ", timeB),  " run at: ", Sys.time())
  cli::cat_line("\nSaved to: ", save_path)

  return(formatted_tbl)

}


## Conditions ---------------------------
conditionCovariates <- function(executionSettings,
                                con,
                                targetCohorts,
                                timeA,
                                timeB,
                                outputFolder) {


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

  #create directory if it doesnt exist
  outputFolder <- fs::path(outputFolder, targetCohorts$name)
  fs::dir_create(outputFolder)

  #preset feature extraction for conditions
  cov_settings <- FeatureExtraction::createCovariateSettings(
    useConditionGroupEraLongTerm = TRUE,
    longTermStartDays = timeA,
    endDays = timeB
  )

  #get covariate data
  tik <- Sys.time()
  quietCov <- purrr::quietly(FeatureExtraction::getDbCovariateData)
  cov <- quietCov(
    connection = con,
    cdmDatabaseSchema = cdmSchema,
    cohortTable = cohortTable,
    cohortDatabaseSchema = writeSchema,
    cohortId = targetCohortId,
    covariateSettings = cov_settings
  )$result

  tok <- Sys.time()
  tdif <- tok - tik
  tok_format <- paste(scales::label_number(0.01)(as.numeric(tdif)), attr(tdif, "units"))
  cli::cat_line("\nCondition Covariates built at: ", tok)
  cli::cat_line("\nCovariate build took: ", tok_format)


  ### Get strata table
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


  ## Get strata cohorts
  strataTbl <- strataTbl %>%
    dplyr::mutate(subject_id = as.double(subject_id)) %>%
    dplyr::filter(cohort_definition_id == targetCohortId) %>%
    dplyr::group_by(strata_id, strata) %>%
    dplyr::mutate(total = n()) %>%
    dplyr::ungroup()

  ### Get covariate table for drugs
  covTbl <- cov$covariates %>%
    dplyr::left_join(cov$covariateRef, by = c("covariateId")) %>%
    dplyr::filter(analysisId == 210) %>%
    dplyr::mutate(rowId = as.double(rowId)) %>%
    dplyr::select(rowId, covariateValue, conceptId) %>%
    dplyr::collect()

  ### Get unique concept ids
  conceptIds <- cov$covariateRef %>%
    dplyr::filter(analysisId == 210) %>%
    dplyr::select(conceptId) %>%
    dplyr::mutate(conceptId = as.double(conceptId)) %>%
    dplyr::collect() %>%
    dplyr::pull()


  ## Condition Rollup
  conditionSql <-
    "with disease as ( -- define disease categories similar to ICD10 Chapters
    select 1 as precedence, 'Blood disease' as category_name, 440371 as category_id union
    select 1, 'Blood disease', 443723 union
    select 2, 'Injury and poisoning', 432795 union
    select 2, 'Injury and poisoning', 442562 union
    select 2, 'Injury and poisoning', 444363 union
    select 3, 'Congenital disease', 440508 union
    select 4, 'Pregnancy or childbirth disease', 435875 union
    select 4, 'Pregnancy or childbirth disease', 4088927 union
    select 4, 'Pregnancy or childbirth disease', 4154314 union
    select 4, 'Pregnancy or childbirth disease', 4136529 union
    select 5, 'Perinatal disease', 441406 union
    select 6, 'Infection', 432250 union
    select 7, 'Neoplasm', 438112 union
    select 8, 'Endocrine or metabolic disease', 31821 union
    select 8, 'Endocrine or metabolic disease', 4090739 union
    select 8, 'Endocrine or metabolic disease', 436670 union
    select 9, 'Mental disease', 432586 union
    select 10, 'Nerve disease and pain', 376337 union
    select 10, 'Nerve disease and pain', 4011630 union
    select 11, 'Eye disease', 4038502 union
    select 12, 'ENT disease', 4042836 union
    select 13, 'Cardiovascular disease', 134057 union
    select 14, 'Respiratory disease', 320136 union
    select 15, 'Digestive disease', 4302537 union
    select 16, 'Skin disease', 4028387 union
    select 17, 'Soft tissue or bone disease', 4244662 union
    select 17, 'Soft tissue or bone disease', 433595 union
    select 17, 'Soft tissue or bone disease', 4344497 union
    select 17, 'Soft tissue or bone disease', 40482430 union
    select 17, 'Soft tissue or bone disease', 4027384 union
    select 18, 'Genitourinary disease', 4041285 union
    select 19, 'Iatrogenic condition', 4105886 union
    select 19, 'Iatrogenic condition', 4053838
  )
  select distinct -- get the disease category with the lowest (best fitting) precedence, or assign 'Other Condition'
    concept_id as condition_id, concept_name as condition_name,
    first_value(coalesce(category_id, 0)) over (partition by concept_id order by precedence nulls last) as category_id,
    first_value(coalesce(category_name, 'Other Condition')) over (partition by concept_id order by precedence nulls last) as category_name
  from @cdmDatabaseSchema.concept
  left join ( -- find the approprate disease category, if possible
    select descendant_concept_id, category_id, category_name, precedence
    from @cdmDatabaseSchema.concept_ancestor
    join disease on ancestor_concept_id=category_id
  ) d on descendant_concept_id=concept_id
  where concept_id in (@conceptIds) -- place here the concept_ids you want to roll up (have to be standard SNOMED)
  ;"

  cli::cat_line("\nRolling up condition concepts to ICD10 Chapters class using: ")
  cli::cat_line("\n", conditionSql, "\n")


  ## Split concept set into two vectors to avoid Snowflake error:
  ## maximum number of expressions in a list exceeded, expected at most 16,384, got 17,979

  chunk_length <- length(conceptIds)/2

  conceptIdList <- split(conceptIds,
                         ceiling(seq_along(conceptIds) / chunk_length))

  conceptIdsNo1 <- unlist(conceptIdList[1])
  conceptIdsNo2 <- unlist(conceptIdList[2])


  ## Get condition Rollup
  icd_chpNo1 <- DatabaseConnector::renderTranslateQuerySql(
    connection = con,
    sql = conditionSql,
    snakeCaseToCamelCase = TRUE,
    cdmDatabaseSchema = cdmSchema,
    conceptIds = conceptIdsNo1
  )

  icd_chpNo2 <- DatabaseConnector::renderTranslateQuerySql(
    connection = con,
    sql = conditionSql,
    snakeCaseToCamelCase = TRUE,
    cdmDatabaseSchema = cdmSchema,
    conceptIds = conceptIdsNo2
  )

  icd_chp <- rbind(icd_chpNo1, icd_chpNo2)

  icd_chp <- icd_chp %>%
    dplyr::rename(conceptId = conditionId, conceptName = conditionName, categoryCode = categoryId) %>%
    dplyr::mutate(
      categoryId = dplyr::case_when(
        categoryName == "Other Condition" ~ 0,
        categoryName == "Blood disease" ~ 1,
        categoryName == "Injury and poisoning" ~ 2,
        categoryName == "Congenital disease" ~ 3,
        categoryName == "Pregnancy or childbirth disease" ~ 4,
        categoryName == "Perinatal disease" ~ 5,
        categoryName == "Infection" ~ 6,
        categoryName == "Neoplasm" ~ 7,
        categoryName == "Endocrine or metabolic disease" ~ 8,
        categoryName == "Mental disease" ~ 9,
        categoryName == "Nerve disease and pain" ~ 10,
        categoryName == "Eye disease" ~ 11,
        categoryName == "ENT disease" ~ 12,
        categoryName == "Cardiovascular disease" ~ 13,
        categoryName == "Respiratory disease" ~ 14,
        categoryName == "Digestive disease" ~ 15,
        categoryName == "Skin disease" ~ 16,
        categoryName == "Soft tissue or bone disease" ~ 17,
        categoryName == "Genitourinary disease" ~ 18,
        categoryName == "Iatrogenic condition" ~ 19
      )
    ) %>%
    dplyr::select(categoryId, categoryCode, categoryName, conceptId, conceptName) %>%
    dplyr::arrange(categoryId, conceptId)

  gc()

  ### Format output for conditions
  covStrata <- covTbl %>%
    dplyr::left_join(strataTbl, by = c("rowId" = "subject_id"), multiple = "all") %>%
    dplyr::group_by(conceptId, strata_id, strata, total) %>%
    dplyr::summarize(nn = sum(covariateValue, na.rm = TRUE))

  condTbl <- covStrata %>%
    dplyr::left_join(icd_chp, by = c("conceptId")) %>%
    dplyr::mutate(
      window = paste(timeA, timeB, sep = " : "),
      pct = nn / total) %>%
    dplyr::select(categoryId, categoryName, conceptId, conceptName, window, strata_id, strata, nn, total, pct) %>%
    dplyr::arrange(categoryId) %>%
    dplyr::mutate(database = executionSettings$databaseId,
                  cohort = targetCohorts$name)

  ### Save output for all conditions
  fname <- paste("conditions", abs(timeA), abs(timeB), sep = "_")
  save_path <- fs::path(outputFolder, fname , ext = "csv")
  readr::write_csv(condTbl, file = save_path)
  cli::cat_line("\nCondition Covariates at window ", paste0(timeA, " to ", timeB), " run at: ", Sys.time())
  cli::cat_line("\nConditions Baseline run at: ", Sys.time())
  cli::cat_line("\nSaved to: ", save_path)


  gc()

  ### Format output for icd chapters

  condTbl2 <- covTbl %>%
     dplyr::left_join(icd_chp, by = c("conceptId")) %>%
     dplyr::left_join(strataTbl, by = c("rowId" = "subject_id"), multiple = "all")

  condTblCh <- condTbl2 %>%
    dplyr::distinct(rowId, categoryId, categoryCode, categoryName, strata_id, strata, total) %>%
    dplyr::group_by(categoryId, categoryCode, categoryName, strata_id, strata, total) %>%
    dplyr::count(name = "nn") %>%
    dplyr::ungroup() %>%
    dplyr::mutate(
      window = paste(timeA, timeB, sep = " : "),
      nn = as.double(nn),
      pct = nn / total) %>%
    dplyr::select(categoryId, categoryCode, categoryName, strata_id, strata, nn, total, pct, window) %>%
    dplyr::arrange(categoryId) %>%
    dplyr::mutate(database = executionSettings$databaseId,
                  cohort = targetCohorts$name)


  ### Save output for all conditions
  fname <- paste("condition_chapters", abs(timeA), abs(timeB), sep = "_")
  save_path <- fs::path(outputFolder, fname , ext = "csv")
  readr::write_csv(condTblCh, file = save_path)
  cli::cat_line("\nCondition Chapter Covariates at window ", paste0(timeA, " to ", timeB),  " run at: ", Sys.time())
  cli::cat_line("\nSaved to: ", save_path)

  return(condTbl)

}




## Map functions --------------------
cohortCovariatesMap <- function(executionSettings,
                             con,
                             targetCohortsName,
                             targetCohortsId,
                             covariateKey,
                             timeA,
                             timeB,
                             outputFolder,
                             printSql = FALSE) {

  cli::cat_rule()
  cli::cat_line("Generating Cohort Covariates")
  id_print <- paste(covariateKey$id, collapse = ",")
  t_print <- paste0(timeA, " to " , timeB)
  cli::cat_line("\tCohort name: ", targetCohortsName)
  cli::cat_line("\tUsing covariate ids: ", id_print)
  cli::cat_line("\tAt window ", t_print)


  if (executionSettings$connectionDetails$dbms == "snowflake") {

    writeSchema <- paste(executionSettings$writeDatabase, executionSettings$writeSchema, sep = ".")
    cdmSchema <- paste(executionSettings$cdmDatabase, executionSettings$cdmSchema, sep = ".")

  } else {

    writeSchema <- executionSettings$writeSchema
    cdmSchema <- executionSettings$cdmSchema

  }

  cohortTable <- paste(executionSettings$cohortTable, executionSettings$databaseId, sep = "_")
  strataTable <- paste(executionSettings$cohortTable, "strata", configBlock, sep = "_")
  targetCohortId <- as.double(targetCohortsId)

  #create directory if it doesnt exist
  outputFolder <- fs::path(outputFolder, targetCohortsName)
  fs::dir_create(outputFolder)


  cohortSql <- "
  WITH T1 AS (
    SELECT *
    FROM @writeSchema.@cohortTable
    WHERE cohort_definition_id = @cohortId
  ),
  T2 AS (
    SELECT
      T1.*,
      rhs.strata_id,
      rhs.strata,
      rhs.total
    FROM T1
    LEFT JOIN (
      SELECT
      *,
      COUNT(*) OVER (PARTITION BY strata_id, strata) AS total
      FROM (
        SELECT *
        FROM @writeSchema.@strataTable
        WHERE cohort_definition_id = @cohortId
      ) AS tt
    ) rhs
    ON (T1.subject_id = rhs.subject_id AND T1.cohort_definition_id = rhs.cohort_definition_id)
  ),
  T3 AS (
    SELECT
      T2.*,
      rhs.cohort_definition_id AS cohort_covariate_id,
      rhs.cohort_start_date AS covariate_start_date
    FROM T2
    INNER JOIN (
      SELECT *
      FROM @writeSchema.@cohortTable
      WHERE cohort_definition_id IN (@covariateIds)
    ) rhs
    ON (T2.subject_id = rhs.subject_id)
  ),
  T4 AS (
    SELECT
      *,
      DATEADD(day, @timeA, cohort_start_date) AS a,
      DATEADD(day, @timeB, cohort_start_date) AS b
    FROM T3
  ),
  T5 AS (
    SELECT
      *,
      (CASE WHEN covariate_start_date BETWEEN a AND b THEN 1 ELSE 0 END) AS hit
    FROM T4
  )
  SELECT
    cohort_covariate_id,
    strata_id,
    strata,
    total,
    SUM(hit) AS nn
  FROM T5
  GROUP BY
    cohort_covariate_id,
    strata_id,
    strata,
    total;"

  if (printSql) {
    cli::cat_line("With sql:\n", cohortSql, "\n")
  }


  tik <- Sys.time()

  covTbl <- SqlRender::render(
    cohortSql,
    writeSchema = writeSchema,
    cohortTable = cohortTable,
    strataTable = strataTable,
    cohortId = targetCohortId,
    covariateIds = covariateKey$id,
    timeA = timeA,
    timeB = timeB) %>%
    SqlRender::translate(targetDialect = executionSettings$connectionDetails$dbms) %>%
    DatabaseConnector::querySql(connection = con, sql = .) %>%
    tibble::as_tibble() %>%
    dplyr::rename_with(~tolower(.x)) %>%
    dplyr::mutate(
      pct = nn / total
    )

  tok <- Sys.time()
  tdif <- tok - tik
  tok_format <- paste(scales::label_number(0.01)(as.numeric(tdif)), attr(tdif, "units"))
  cli::cat_line("\nCohort Covariates built at: ", tok)
  cli::cat_line("\nCovariate build took: ", tok_format)


  ### Format output
  formatted_tbl <- covTbl %>%
    dplyr::left_join(covariateKey, by = c("cohort_covariate_id" = "id")) %>%
    dplyr::mutate(
      cohortCovariateId = as.double(cohort_covariate_id),
      cohortCovariateName = name,
      strataId = as.double(strata_id),
      strata = as.double(strata),
      window = paste(timeA, timeB, sep = " : ")
    ) %>%
    dplyr::select(cohortCovariateId, cohortCovariateName, strataId, strata, window, nn, total, pct) %>%
    dplyr::mutate(database = executionSettings$databaseId,
                  cohort = targetCohortsName)


  ### Save output for all cohort covariates
  fname <- paste("cohort_covariates", abs(timeA), abs(timeB), sep = "_")
  save_path <- fs::path(outputFolder, fname , ext = "csv")
  readr::write_csv(formatted_tbl, file = save_path)
  cli::cat_line("\nCohort Covariates at window ", paste0(timeA, " to ", timeB),  " run at: ", Sys.time())
  cli::cat_line("\nSaved to: ", save_path)

  return(formatted_tbl)

}



demographicBaselineMap <- function(executionSettings,
                                con,
                                targetCohortsName,
                                targetCohortsId,
                                outputFolder) {


  if (executionSettings$connectionDetails$dbms == "snowflake") {
    writeSchema <- paste(executionSettings$writeDatabase, executionSettings$writeSchema, sep = ".")
    cdmSchema <- paste(executionSettings$cdmDatabase, executionSettings$cdmSchema, sep = ".")
  } else {
    writeSchema <- executionSettings$writeSchema
    cdmSchema <- executionSettings$cdmSchema
  }

  cohortTable <- paste(executionSettings$cohortTable, executionSettings$databaseId, sep = "_")
  strataTable <- paste(executionSettings$cohortTable, "strata", configBlock, sep = "_")
  targetCohortId <- as.double(targetCohortsId)

  #create directory if it doesnt exist
  outputFolder <- fs::path(outputFolder, targetCohortsName)
  fs::dir_create(outputFolder)

  tik <- Sys.time()

  # Create Demographic settings
  cov_settings <- FeatureExtraction::createCovariateSettings(
    useDemographicsGender = TRUE,
    useDemographicsAgeGroup = TRUE,
    useDemographicsRace = TRUE,
    useDemographicsIndexYear = TRUE
  )

  quietCov <- purrr::quietly(FeatureExtraction::getDbCovariateData)
  cov <- quietCov(
    connection = con,
    cdmDatabaseSchema = cdmSchema,
    cohortTable = cohortTable,
    cohortDatabaseSchema = writeSchema,
    cohortId = targetCohortId,
    covariateSettings = cov_settings
  )$result

  tok <- Sys.time()
  tdif <- tok - tik
  tok_format <- paste(scales::label_number(0.01)(as.numeric(tdif)), attr(tdif, "units"))
  cli::cat_line("\nDemographics Covariates built at: ", tok)
  cli::cat_line("\nCovariate build took: ", tok_format)

  ### get s
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
    dplyr::mutate(subject_id = as.double(subject_id)) %>%
    dplyr::filter(cohort_definition_id == targetCohortId) %>%
    dplyr::group_by(strata_id, strata) %>%
    dplyr::mutate(total = n()) %>%
    dplyr::ungroup()


  ### Get covariate table for race and gender
  covTblA <- cov$covariates %>%
    dplyr::left_join(cov$covariateRef, by = c("covariateId")) %>%
    dplyr::filter(analysisId %in% c(1,4)) %>%
    dplyr::mutate(rowId = as.double(rowId)) %>%
    dplyr::select(rowId, covariateValue, conceptId, analysisId) %>%
    dplyr::collect()

  ### Get covariate table for age group
  covTblB <- cov$covariates %>%
    dplyr::left_join(cov$covariateRef, by = c("covariateId")) %>%
    dplyr::filter(analysisId == 3) %>%
    dplyr::mutate(
      rowId = as.double(rowId),
      conceptId = dbplyr::sql("CAST(SUBSTRING(covariateId, 0, LENGTH(covariateId) - 4) AS INT)")
    ) %>%
    dplyr::select(rowId, covariateValue, conceptId, analysisId) %>%
    dplyr::collect()

  ### Get covariate table for year
  covTblC <- cov$covariates %>%
    dplyr::left_join(cov$covariateRef, by = c("covariateId")) %>%
    dplyr::filter(analysisId == 6) %>%
    dplyr::mutate(
      rowId = as.double(rowId),
      conceptId = dbplyr::sql("CAST(SUBSTRING(covariateId, 0, LENGTH(covariateId) - 4) AS INT)")
    ) %>%
    dplyr::select(rowId, covariateValue, conceptId, analysisId) %>%
    dplyr::collect()

  ### Bind all covariate tables
  covTbl <- dplyr::bind_rows(
    covTblA, covTblB, covTblC
  )

  ### Format output
  demoTbl <- covTbl %>%
    dplyr::left_join(strataTbl, by = c("rowId" = "subject_id"), multiple = "all") %>%
    dplyr::group_by(analysisId, conceptId, strata_id, strata, total) %>%
    dplyr::summarize(nn = sum(covariateValue, na.rm = TRUE)) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(
      window = "Baseline",
      pct = nn / total,
      conceptName = dplyr::case_when(
        conceptId == 8507 ~ "Male",
        conceptId == 8532 ~ "Female",
        conceptId == 1 ~ "5 - 9",
        conceptId == 2 ~ "10 - 14",
        conceptId == 3 ~ "15 - 19",
        conceptId == 4 ~ "20 - 24",
        conceptId == 5 ~ "25 - 29",
        conceptId == 6 ~ "30 - 34",
        conceptId == 7 ~ "35 - 39",
        conceptId == 8 ~ "40 - 44",
        conceptId == 9 ~ "45 - 49",
        conceptId == 10 ~ "50 - 54",
        conceptId == 11 ~ "55 - 59",
        conceptId == 12 ~ "60 - 64",
        conceptId == 13 ~ "65 - 69",
        conceptId == 14 ~ "70 - 74",
        conceptId == 15 ~ "75 - 79",
        conceptId == 16 ~ "80 - 84",
        conceptId == 17 ~ "85 - 89",
        conceptId == 18 ~ "90 - 94",
        conceptId == 19 ~ "95 - 99",
        conceptId == 20 ~ "100 - 104",
        conceptId == 21 ~ "105 - 109",
        conceptId == 22 ~ "110 - 114",
        conceptId == 23 ~ "115 - 119",
        conceptId == 8657 ~ "American Indian or Alaska Native",
        conceptId == 8515 ~ "Asian",
        conceptId == 8516 ~ "Black or African American",
        conceptId == 8557 ~ "Native Hawaiian or Other Pacific Islander",
        conceptId == 8527 ~ "White",
        TRUE ~ as.character(conceptId)
      ),
      categoryName = dplyr::case_when(
        analysisId == 1 ~ "Gender",
        analysisId == 3 ~ "5yr Age Group",
        analysisId == 4 ~ "Race",
        analysisId == 6 ~ "Index Year"
      ),
      categoryId = analysisId
    ) %>%
    dplyr::select(categoryId, categoryName, conceptId, conceptName, window, strata_id, strata, nn, total, pct) %>%
    dplyr::arrange(categoryId) %>%
    dplyr::mutate(database = executionSettings$databaseId,
                  cohort = targetCohortsName)

  ### Save output
  save_path <- fs::path(outputFolder, "demographics_baseline.csv")
  readr::write_csv(demoTbl, file = save_path)
  cli::cat_line("\nDemographic Baseline run at: ", Sys.time())
  cli::cat_line("\nSaved to: ", save_path)
  return(demoTbl)

}




continuousBaselineMap <- function(executionSettings,
                                 con,
                                 targetCohortsName,
                                 targetCohortsId,
                                 outputFolder) {


  if (executionSettings$connectionDetails$dbms == "snowflake") {
    writeSchema <- paste(executionSettings$writeDatabase, executionSettings$writeSchema, sep = ".")
    cdmSchema <- paste(executionSettings$cdmDatabase, executionSettings$cdmSchema, sep = ".")
  } else {
    writeSchema <- executionSettings$writeSchema
    cdmSchema <- executionSettings$cdmSchema
  }

  cohortTable <- paste(executionSettings$cohortTable, executionSettings$databaseId, sep = "_")
  strataTable <- paste(executionSettings$cohortTable, "strata", configBlock, sep = "_")
  targetCohortId <- as.double(targetCohortsId)

  #create directory if it doesnt exist
  outputFolder <- fs::path(outputFolder, targetCohortsName)
  fs::dir_create(outputFolder)

  #preset feature extraction for continuous
  cov_settings <- FeatureExtraction::createCovariateSettings(
    useDemographicsAge = TRUE,
    useDemographicsPriorObservationTime = TRUE,
    useDemographicsPostObservationTime = TRUE,
    useDemographicsTimeInCohort = TRUE,
    useDcsi = TRUE
  )

  #get covariate data
  tik <- Sys.time()

  quietCov <- purrr::quietly(FeatureExtraction::getDbCovariateData)
  cov <- quietCov(
    connection = con,
    cdmDatabaseSchema = cdmSchema,
    cohortTable = cohortTable,
    cohortDatabaseSchema = writeSchema,
    cohortId = targetCohortId,
    covariateSettings = cov_settings
  )$result

  tok <- Sys.time()
  tdif <- tok - tik
  tok_format <- paste(scales::label_number(0.01)(as.numeric(tdif)), attr(tdif, "units"))
  cli::cat_line("\nContinuous Covariates built at: ", tok)
  cli::cat_line("\nCovariate build took: ", tok_format)

  ### Get strata table
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
    dplyr::mutate(subject_id = as.double(subject_id)) %>%
    dplyr::filter(cohort_definition_id == targetCohortId) %>%
    dplyr::ungroup() %>%
    dplyr::collect()

  ### Handle Continuous Age
  covTbl <- cov$covariates %>%
    dplyr::left_join(cov$covariateRef, by = c("covariateId")) %>%
    dplyr::filter(analysisId %in% c(2, 8, 9, 10, 902)) %>%
    dplyr::collect()

  ### Format output
  ctsTbl <- covTbl %>%
    dplyr::left_join(strataTbl, by = c("rowId" = "subject_id"), multiple = "all") %>%
    dplyr::group_by(analysisId, conceptId, strata_id, strata) %>%
    dplyr::summarize(
      min = min(covariateValue),
      p25 = quantile(covariateValue, 0.25),
      sd = sd(covariateValue),
      mean = mean(covariateValue),
      median = median(covariateValue),
      IQR = quantile(covariateValue, 0.75) - quantile(covariateValue, 0.25),
      p75 = quantile(covariateValue, 0.75),
      max = max(covariateValue)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(
      window = "Baseline",
      categoryName = case_when(
        analysisId == 2 ~ "Age",
        analysisId == 8 ~ "Prior Observation Time",
        analysisId == 9 ~ "Post Observation Time",
        analysisId == 10 ~ "Time In Cohort",
        analysisId == 902 ~ "DSCI",
        TRUE ~ as.character(analysisId)),
      categoryId = analysisId
    ) %>%
    dplyr::select(categoryId, categoryName, strata_id, strata, window, min, p25, median, mean, sd, IQR, p75, max) %>%
    dplyr::arrange(categoryId) %>%
    dplyr::mutate(database = executionSettings$databaseId,
                  cohort = targetCohortsName)

  ### Save output
  save_path <- fs::path(outputFolder, "continuous_baseline.csv")
  readr::write_csv(ctsTbl, file = save_path)
  cli::cat_line("\nContinuous Baseline run at: ", Sys.time())
  cli::cat_line("\nSaved to: ", save_path)

  return(ctsTbl)

}


drugsCovariatesMap <- function(executionSettings,
                            con,
                            targetCohortsName,
                            targetCohortsId,
                            timeA,
                            timeB,
                            outputFolder) {


  if (executionSettings$connectionDetails$dbms == "snowflake") {
    writeSchema <- paste(executionSettings$writeDatabase, executionSettings$writeSchema, sep = ".")
    cdmSchema <- paste(executionSettings$cdmDatabase, executionSettings$cdmSchema, sep = ".")
  } else {
    writeSchema <- executionSettings$writeSchema
    cdmSchema <- executionSettings$cdmSchema
  }

  cohortTable <- paste(executionSettings$cohortTable, executionSettings$databaseId, sep = "_")
  strataTable <- paste(executionSettings$cohortTable, "strata", configBlock, sep = "_")
  targetCohortId <- as.double(targetCohortsId)

  #create directory if it doesnt exist
  outputFolder <- fs::path(outputFolder, targetCohortsName)
  fs::dir_create(outputFolder)

  #preset feature extraction for continuous
  cov_settings <- FeatureExtraction::createCovariateSettings(
    useDrugGroupEraLongTerm = TRUE,
    excludedCovariateConceptIds = c(21600001, 21600959, 21601237, # Remove ATC 1st class
                                    21601907, 21602359, 21602681,
                                    21602795, 21601386, 21603931,
                                    21604180, 21604847, 21605007,
                                    21603550, 21605212),
    longTermStartDays = timeA,
    endDays = timeB
  )

  #get covariate data
  tik <- Sys.time()
  quietCov <- purrr::quietly(FeatureExtraction::getDbCovariateData)
  cov <- quietCov(
    connection = con,
    cdmDatabaseSchema = cdmSchema,
    cohortTable = cohortTable,
    cohortDatabaseSchema = writeSchema,
    cohortId = targetCohortId,
    covariateSettings = cov_settings
  )$result

  tok <- Sys.time()
  tdif <- tok - tik
  tok_format <- paste(scales::label_number(0.01)(as.numeric(tdif)), attr(tdif, "units"))
  cli::cat_line("\nDrug Covariates built at: ", tok)
  cli::cat_line("\nCovariate build took: ", tok_format)


  ### Get strata table
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
    dplyr::mutate(subject_id = as.double(subject_id)) %>%
    dplyr::filter(cohort_definition_id == targetCohortId) %>%
    dplyr::group_by(strata_id, strata) %>%
    dplyr::mutate(total = n()) %>%
    dplyr::ungroup() %>%
    dplyr::collect()


  ### Get covariate table for drugs
  covTbl <- cov$covariates %>%
    dplyr::left_join(cov$covariateRef, by = c("covariateId")) %>%
    dplyr::filter(analysisId == 410) %>%
    dplyr::mutate(rowId = as.double(rowId)) %>%
    dplyr::select(rowId, covariateValue, conceptId) %>%
    dplyr::collect()

  ### Get unique concept ids
  conceptIds <- cov$covariateRef %>%
    dplyr::filter(analysisId == 410) %>%
    dplyr::select(conceptId) %>%
    dplyr::mutate(conceptId = as.double(conceptId)) %>%
    dplyr::collect() %>%
    dplyr::pull()

  ### Find atc rollup

  drugSql <-
    "with drug as ( -- define disease categories similar to ICD10 Chapters
      select row_number() over (order by concept_code) as precedence, concept_name as category_name, concept_id as category_id
      from @cdmDatabaseSchema.concept
      where vocabulary_id='ATC' and concept_class_id='ATC 2nd'
    )
    select distinct -- get the disease category with the lowest (best fitting) precedence, or assign 'Other Condition'
      concept_id as drug_id, concept_name as drug_name,
      first_value(coalesce(category_id, 0)) over (partition by concept_id order by precedence nulls last) as category_id,
      first_value(coalesce(category_name, 'Other Drug')) over (partition by concept_id order by precedence nulls last) as category_name
    from @cdmDatabaseSchema.concept
    left join ( -- find the approprate drug category, if possible
      select descendant_concept_id, category_id, category_name, precedence
      from @cdmDatabaseSchema.concept_ancestor
      join drug on ancestor_concept_id=category_id
    ) d on descendant_concept_id=concept_id
    where concept_id in (@conceptIds) -- place here the concept_ids you want to roll up (have to be standard SNOMED)
    ;"

  cli::cat_line("\nRolling up drug concepts to ATC 2nd class using: ")
  cli::cat_line("\n", drugSql, "\n")


  atc_grp <- DatabaseConnector::renderTranslateQuerySql(
    connection = con,
    sql = drugSql,
    snakeCaseToCamelCase = TRUE,
    cdmDatabaseSchema = cdmSchema,
    conceptIds = conceptIds
  ) %>%
    dplyr::mutate(categoryName = stringr::str_to_title(categoryName)) %>%
    dplyr::rename(conceptId = drugId, conceptName = drugName)


  ### Format output
  drugTbl <- covTbl %>%
    dplyr::left_join(strataTbl, by = c("rowId" = "subject_id"), multiple = "all") %>%
    dplyr::group_by(conceptId, strata_id, strata, total) %>%
    dplyr::summarize(nn = sum(covariateValue, na.rm = TRUE)) %>%
    dplyr::ungroup() %>%
    dplyr::left_join(atc_grp, by = c("conceptId")) %>%
    dplyr::mutate(
      window = paste(timeA, timeB, sep = " : "),
      pct = nn / total) %>%
    dplyr::select(categoryId, categoryName, conceptId, conceptName, strata_id, strata, nn, total, pct, window) %>%
    dplyr::arrange(categoryId) %>%
    dplyr::mutate(database = executionSettings$databaseId,
                  cohort = targetCohortsName)

  ### Save output
  fname <- paste("drugs", abs(timeA), abs(timeB), sep = "_")
  save_path <- fs::path(outputFolder, fname , ext = "csv")
  readr::write_csv(drugTbl, file = save_path)
  cli::cat_line("\nDrug Covariates at window ", paste0(timeA, " to ", timeB),  " run at: ", Sys.time())
  cli::cat_line("\nSaved to: ", save_path)

  return(drugTbl)

}



conditionCovariatesGroupMap <- function(executionSettings,
                                  con,
                                  targetCohortsName,
                                  targetCohortsId,
                                  timeA,
                                  timeB,
                                  outputFolder) {


  if (executionSettings$connectionDetails$dbms == "snowflake") {
    writeSchema <- paste(executionSettings$writeDatabase, executionSettings$writeSchema, sep = ".")
    cdmSchema <- paste(executionSettings$cdmDatabase, executionSettings$cdmSchema, sep = ".")
  } else {
    writeSchema <- executionSettings$writeSchema
    cdmSchema <- executionSettings$cdmSchema
  }

  cohortTable <- paste(executionSettings$cohortTable, executionSettings$databaseId, sep = "_")
  strataTable <- paste(executionSettings$cohortTable, "strata", configBlock, sep = "_")
  targetCohortId <- as.double(targetCohortsId)

  #create directory if it doesnt exist
  outputFolder <- fs::path(outputFolder, targetCohortsName)
  fs::dir_create(outputFolder)

  #preset feature extraction for conditions
  cov_settings <- FeatureExtraction::createCovariateSettings(
    useConditionGroupEraLongTerm = TRUE,
    longTermStartDays = timeA,
    endDays = timeB
  )

  #get covariate data
  tik <- Sys.time()
  quietCov <- purrr::quietly(FeatureExtraction::getDbCovariateData)
  cov <- quietCov(
    connection = con,
    cdmDatabaseSchema = cdmSchema,
    cohortTable = cohortTable,
    cohortDatabaseSchema = writeSchema,
    cohortId = targetCohortId,
    covariateSettings = cov_settings
  )$result

  tok <- Sys.time()
  tdif <- tok - tik
  tok_format <- paste(scales::label_number(0.01)(as.numeric(tdif)), attr(tdif, "units"))
  cli::cat_line("\nCondition Covariates built at: ", tok)
  cli::cat_line("\nCovariate build took: ", tok_format)


  ### Get strata table
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


  ## Get strata cohorts
  strataTbl <- strataTbl %>%
    dplyr::mutate(subject_id = as.double(subject_id)) %>%
    dplyr::filter(cohort_definition_id == targetCohortId) %>%
    dplyr::group_by(strata_id, strata) %>%
    dplyr::mutate(total = n()) %>%
    dplyr::ungroup()

  ### Get covariate table for drugs
  covTbl <- cov$covariates %>%
    dplyr::left_join(cov$covariateRef, by = c("covariateId")) %>%
    dplyr::filter(analysisId == 210) %>%
    dplyr::mutate(rowId = as.double(rowId)) %>%
    dplyr::select(rowId, covariateValue, conceptId) %>%
    dplyr::collect()

  ### Get unique concept ids
  conceptIds <- cov$covariateRef %>%
    dplyr::filter(analysisId == 210) %>%
    dplyr::select(conceptId) %>%
    dplyr::mutate(conceptId = as.double(conceptId)) %>%
    dplyr::collect() %>%
    dplyr::pull()


  ## Condition Rollup
  conditionSql <-
    "with disease as ( -- define disease categories similar to ICD10 Chapters
    select 1 as precedence, 'Blood disease' as category_name, 440371 as category_id union
    select 1, 'Blood disease', 443723 union
    select 2, 'Injury and poisoning', 432795 union
    select 2, 'Injury and poisoning', 442562 union
    select 2, 'Injury and poisoning', 444363 union
    select 3, 'Congenital disease', 440508 union
    select 4, 'Pregnancy or childbirth disease', 435875 union
    select 4, 'Pregnancy or childbirth disease', 4088927 union
    select 4, 'Pregnancy or childbirth disease', 4154314 union
    select 4, 'Pregnancy or childbirth disease', 4136529 union
    select 5, 'Perinatal disease', 441406 union
    select 6, 'Infection', 432250 union
    select 7, 'Neoplasm', 438112 union
    select 8, 'Endocrine or metabolic disease', 31821 union
    select 8, 'Endocrine or metabolic disease', 4090739 union
    select 8, 'Endocrine or metabolic disease', 436670 union
    select 9, 'Mental disease', 432586 union
    select 10, 'Nerve disease and pain', 376337 union
    select 10, 'Nerve disease and pain', 4011630 union
    select 11, 'Eye disease', 4038502 union
    select 12, 'ENT disease', 4042836 union
    select 13, 'Cardiovascular disease', 134057 union
    select 14, 'Respiratory disease', 320136 union
    select 15, 'Digestive disease', 4302537 union
    select 16, 'Skin disease', 4028387 union
    select 17, 'Soft tissue or bone disease', 4244662 union
    select 17, 'Soft tissue or bone disease', 433595 union
    select 17, 'Soft tissue or bone disease', 4344497 union
    select 17, 'Soft tissue or bone disease', 40482430 union
    select 17, 'Soft tissue or bone disease', 4027384 union
    select 18, 'Genitourinary disease', 4041285 union
    select 19, 'Iatrogenic condition', 4105886 union
    select 19, 'Iatrogenic condition', 4053838
  )
  select distinct -- get the disease category with the lowest (best fitting) precedence, or assign 'Other Condition'
    concept_id as condition_id, concept_name as condition_name,
    first_value(coalesce(category_id, 0)) over (partition by concept_id order by precedence nulls last) as category_id,
    first_value(coalesce(category_name, 'Other Condition')) over (partition by concept_id order by precedence nulls last) as category_name
  from @cdmDatabaseSchema.concept
  left join ( -- find the approprate disease category, if possible
    select descendant_concept_id, category_id, category_name, precedence
    from @cdmDatabaseSchema.concept_ancestor
    join disease on ancestor_concept_id=category_id
  ) d on descendant_concept_id=concept_id
  where concept_id in (@conceptIds) -- place here the concept_ids you want to roll up (have to be standard SNOMED)
  ;"

  # cli::cat_line("\nRolling up condition concepts to ICD10 Chapters class using: ")
  # cli::cat_line("\n", conditionSql, "\n")


  ## Split concept set into two vectors to avoid Snowflake error:
  ## maximum number of expressions in a list exceeded, expected at most 16,384, got 17,979

  chunk_length <- length(conceptIds)/2

  conceptIdList <- split(conceptIds,
                         ceiling(seq_along(conceptIds) / chunk_length))

  conceptIdsNo1 <- unlist(conceptIdList[1])
  conceptIdsNo2 <- unlist(conceptIdList[2])


  ## Get condition Rollup
  icd_chpNo1 <- DatabaseConnector::renderTranslateQuerySql(
    connection = con,
    sql = conditionSql,
    snakeCaseToCamelCase = TRUE,
    cdmDatabaseSchema = cdmSchema,
    conceptIds = conceptIdsNo1
  )

  icd_chpNo2 <- DatabaseConnector::renderTranslateQuerySql(
    connection = con,
    sql = conditionSql,
    snakeCaseToCamelCase = TRUE,
    cdmDatabaseSchema = cdmSchema,
    conceptIds = conceptIdsNo2
  )

  icd_chp <- rbind(icd_chpNo1, icd_chpNo2)

  icd_chp <- icd_chp %>%
    dplyr::rename(conceptId = conditionId, conceptName = conditionName, categoryCode = categoryId) %>%
    dplyr::mutate(
      categoryId = dplyr::case_when(
        categoryName == "Other Condition" ~ 0,
        categoryName == "Blood disease" ~ 1,
        categoryName == "Injury and poisoning" ~ 2,
        categoryName == "Congenital disease" ~ 3,
        categoryName == "Pregnancy or childbirth disease" ~ 4,
        categoryName == "Perinatal disease" ~ 5,
        categoryName == "Infection" ~ 6,
        categoryName == "Neoplasm" ~ 7,
        categoryName == "Endocrine or metabolic disease" ~ 8,
        categoryName == "Mental disease" ~ 9,
        categoryName == "Nerve disease and pain" ~ 10,
        categoryName == "Eye disease" ~ 11,
        categoryName == "ENT disease" ~ 12,
        categoryName == "Cardiovascular disease" ~ 13,
        categoryName == "Respiratory disease" ~ 14,
        categoryName == "Digestive disease" ~ 15,
        categoryName == "Skin disease" ~ 16,
        categoryName == "Soft tissue or bone disease" ~ 17,
        categoryName == "Genitourinary disease" ~ 18,
        categoryName == "Iatrogenic condition" ~ 19
      )
    ) %>%
    dplyr::select(categoryId, categoryCode, categoryName, conceptId, conceptName) %>%
    dplyr::arrange(categoryId, conceptId)


  ### Format output for icd chapters
  condTbl2 <- covTbl %>%
    dplyr::left_join(icd_chp, by = c("conceptId")) %>%
    dplyr::left_join(strataTbl, by = c("rowId" = "subject_id"), multiple = "all")

  condTblCh <- condTbl2 %>%
    dplyr::distinct(rowId, categoryId, categoryCode, categoryName, strata_id, strata, total) %>%
    dplyr::group_by(categoryId, categoryCode, categoryName, strata_id, strata, total) %>%
    dplyr::count(name = "nn") %>%
    dplyr::ungroup() %>%
    dplyr::mutate(
      window = paste(timeA, timeB, sep = " : "),
      nn = as.double(nn),
      pct = nn / total) %>%
    dplyr::select(categoryId, categoryCode, categoryName, strata_id, strata, nn, total, pct, window) %>%
    dplyr::arrange(categoryId) %>%
    dplyr::mutate(database = executionSettings$databaseId,
                  cohort = targetCohortsName)


  ### Save output for all conditions
  fname <- paste("condition_chapters", abs(timeA), abs(timeB), sep = "_")
  save_path <- fs::path(outputFolder, fname , ext = "csv")
  readr::write_csv(condTblCh, file = save_path)
  cli::cat_line("\nCondition Chapter Covariates at window ", paste0(timeA, " to ", timeB),  " run at: ", Sys.time())
  cli::cat_line("\nSaved to: ", save_path)

  return(condTblCh)

}


conditionCovariatesMap <- function(executionSettings,
                                   con,
                                   targetCohortsName,
                                   targetCohortsId,
                                   timeA,
                                   timeB,
                                   outputFolder) {


  if (executionSettings$connectionDetails$dbms == "snowflake") {
    writeSchema <- paste(executionSettings$writeDatabase, executionSettings$writeSchema, sep = ".")
    cdmSchema <- paste(executionSettings$cdmDatabase, executionSettings$cdmSchema, sep = ".")
  } else {
    writeSchema <- executionSettings$writeSchema
    cdmSchema <- executionSettings$cdmSchema
  }

  cohortTable <- paste(executionSettings$cohortTable, executionSettings$databaseId, sep = "_")
  strataTable <- paste(executionSettings$cohortTable, "strata", configBlock, sep = "_")
  targetCohortId <- as.double(targetCohortsId)

  #create directory if it doesnt exist
  outputFolder <- fs::path(outputFolder, targetCohortsName)
  fs::dir_create(outputFolder)

  #preset feature extraction for conditions
  cov_settings <- FeatureExtraction::createCovariateSettings(
    useConditionGroupEraLongTerm = TRUE,
    longTermStartDays = timeA,
    endDays = timeB
  )

  #get covariate data
  tik <- Sys.time()
  quietCov <- purrr::quietly(FeatureExtraction::getDbCovariateData)
  cov <- quietCov(
    connection = con,
    cdmDatabaseSchema = cdmSchema,
    cohortTable = cohortTable,
    cohortDatabaseSchema = writeSchema,
    cohortId = targetCohortId,
    covariateSettings = cov_settings
  )$result

  tok <- Sys.time()
  tdif <- tok - tik
  tok_format <- paste(scales::label_number(0.01)(as.numeric(tdif)), attr(tdif, "units"))
  cli::cat_line("\nCondition Covariates built at: ", tok)
  cli::cat_line("\nCovariate build took: ", tok_format)


  ### Get strata table
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


  ## Get strata cohorts
  strataTbl <- strataTbl %>%
    dplyr::mutate(subject_id = as.double(subject_id)) %>%
    dplyr::filter(cohort_definition_id == targetCohortId) %>%
    dplyr::group_by(strata_id, strata) %>%
    dplyr::mutate(total = n()) %>%
    dplyr::ungroup()

  ### Get covariate table for drugs
  covTbl <- cov$covariates %>%
    dplyr::left_join(cov$covariateRef, by = c("covariateId")) %>%
    dplyr::filter(analysisId == 210) %>%
    dplyr::mutate(rowId = as.double(rowId)) %>%
    dplyr::select(rowId, covariateValue, conceptId) %>%
    dplyr::collect()

  ### Get unique concept ids
  conceptIds <- cov$covariateRef %>%
    dplyr::filter(analysisId == 210) %>%
    dplyr::select(conceptId) %>%
    dplyr::mutate(conceptId = as.double(conceptId)) %>%
    dplyr::collect() %>%
    dplyr::pull()


  ## Condition Rollup
  conditionSql <-
    "with disease as ( -- define disease categories similar to ICD10 Chapters
    select 1 as precedence, 'Blood disease' as category_name, 440371 as category_id union
    select 1, 'Blood disease', 443723 union
    select 2, 'Injury and poisoning', 432795 union
    select 2, 'Injury and poisoning', 442562 union
    select 2, 'Injury and poisoning', 444363 union
    select 3, 'Congenital disease', 440508 union
    select 4, 'Pregnancy or childbirth disease', 435875 union
    select 4, 'Pregnancy or childbirth disease', 4088927 union
    select 4, 'Pregnancy or childbirth disease', 4154314 union
    select 4, 'Pregnancy or childbirth disease', 4136529 union
    select 5, 'Perinatal disease', 441406 union
    select 6, 'Infection', 432250 union
    select 7, 'Neoplasm', 438112 union
    select 8, 'Endocrine or metabolic disease', 31821 union
    select 8, 'Endocrine or metabolic disease', 4090739 union
    select 8, 'Endocrine or metabolic disease', 436670 union
    select 9, 'Mental disease', 432586 union
    select 10, 'Nerve disease and pain', 376337 union
    select 10, 'Nerve disease and pain', 4011630 union
    select 11, 'Eye disease', 4038502 union
    select 12, 'ENT disease', 4042836 union
    select 13, 'Cardiovascular disease', 134057 union
    select 14, 'Respiratory disease', 320136 union
    select 15, 'Digestive disease', 4302537 union
    select 16, 'Skin disease', 4028387 union
    select 17, 'Soft tissue or bone disease', 4244662 union
    select 17, 'Soft tissue or bone disease', 433595 union
    select 17, 'Soft tissue or bone disease', 4344497 union
    select 17, 'Soft tissue or bone disease', 40482430 union
    select 17, 'Soft tissue or bone disease', 4027384 union
    select 18, 'Genitourinary disease', 4041285 union
    select 19, 'Iatrogenic condition', 4105886 union
    select 19, 'Iatrogenic condition', 4053838
  )
  select distinct -- get the disease category with the lowest (best fitting) precedence, or assign 'Other Condition'
    concept_id as condition_id, concept_name as condition_name,
    first_value(coalesce(category_id, 0)) over (partition by concept_id order by precedence nulls last) as category_id,
    first_value(coalesce(category_name, 'Other Condition')) over (partition by concept_id order by precedence nulls last) as category_name
  from @cdmDatabaseSchema.concept
  left join ( -- find the approprate disease category, if possible
    select descendant_concept_id, category_id, category_name, precedence
    from @cdmDatabaseSchema.concept_ancestor
    join disease on ancestor_concept_id=category_id
  ) d on descendant_concept_id=concept_id
  where concept_id in (@conceptIds) -- place here the concept_ids you want to roll up (have to be standard SNOMED)
  ;"

  # cli::cat_line("\nRolling up condition concepts to ICD10 Chapters class using: ")
  # cli::cat_line("\n", conditionSql, "\n")


  ## Split concept set into two vectors to avoid Snowflake error:
  ## maximum number of expressions in a list exceeded, expected at most 16,384, got 17,979

  chunk_length <- length(conceptIds)/2

  conceptIdList <- split(conceptIds,
                         ceiling(seq_along(conceptIds) / chunk_length))

  conceptIdsNo1 <- unlist(conceptIdList[1])
  conceptIdsNo2 <- unlist(conceptIdList[2])


  ## Get condition Rollup
  icd_chpNo1 <- DatabaseConnector::renderTranslateQuerySql(
    connection = con,
    sql = conditionSql,
    snakeCaseToCamelCase = TRUE,
    cdmDatabaseSchema = cdmSchema,
    conceptIds = conceptIdsNo1
  )

  icd_chpNo2 <- DatabaseConnector::renderTranslateQuerySql(
    connection = con,
    sql = conditionSql,
    snakeCaseToCamelCase = TRUE,
    cdmDatabaseSchema = cdmSchema,
    conceptIds = conceptIdsNo2
  )

  icd_chp <- rbind(icd_chpNo1, icd_chpNo2)

  icd_chp <- icd_chp %>%
    dplyr::rename(conceptId = conditionId, conceptName = conditionName, categoryCode = categoryId) %>%
    dplyr::mutate(
      categoryId = dplyr::case_when(
        categoryName == "Other Condition" ~ 0,
        categoryName == "Blood disease" ~ 1,
        categoryName == "Injury and poisoning" ~ 2,
        categoryName == "Congenital disease" ~ 3,
        categoryName == "Pregnancy or childbirth disease" ~ 4,
        categoryName == "Perinatal disease" ~ 5,
        categoryName == "Infection" ~ 6,
        categoryName == "Neoplasm" ~ 7,
        categoryName == "Endocrine or metabolic disease" ~ 8,
        categoryName == "Mental disease" ~ 9,
        categoryName == "Nerve disease and pain" ~ 10,
        categoryName == "Eye disease" ~ 11,
        categoryName == "ENT disease" ~ 12,
        categoryName == "Cardiovascular disease" ~ 13,
        categoryName == "Respiratory disease" ~ 14,
        categoryName == "Digestive disease" ~ 15,
        categoryName == "Skin disease" ~ 16,
        categoryName == "Soft tissue or bone disease" ~ 17,
        categoryName == "Genitourinary disease" ~ 18,
        categoryName == "Iatrogenic condition" ~ 19
      )
    ) %>%
    dplyr::select(categoryId, categoryCode, categoryName, conceptId, conceptName) %>%
    dplyr::arrange(categoryId, conceptId)


  ### Format output for conditions
  covStrata <- covTbl %>%
    dplyr::left_join(strataTbl, by = c("rowId" = "subject_id"), multiple = "all") %>%
    dplyr::group_by(conceptId, strata_id, strata, total) %>%
    dplyr::summarize(nn = sum(covariateValue, na.rm = TRUE))

  condTbl <- covStrata %>%
    dplyr::left_join(icd_chp, by = c("conceptId")) %>%
    dplyr::mutate(
      window = paste(timeA, timeB, sep = " : "),
      pct = nn / total) %>%
    dplyr::select(categoryId, categoryName, conceptId, conceptName, window, strata_id, strata, nn, total, pct) %>%
    dplyr::arrange(categoryId) %>%
    dplyr::mutate(database = executionSettings$databaseId,
                  cohort = targetCohortsName)

  ### Save output for all conditions
  fname <- paste("conditions", abs(timeA), abs(timeB), sep = "_")
  save_path <- fs::path(outputFolder, fname , ext = "csv")
  readr::write_csv(condTbl, file = save_path)
  cli::cat_line("\nCondition Covariates at window ", paste0(timeA, " to ", timeB), " run at: ", Sys.time())
  cli::cat_line("\nConditions Baseline run at: ", Sys.time())
  cli::cat_line("\nSaved to: ", save_path)


  return(condTbl)

}

