## Setup  --------------------
source("analysis/R/_executionSettings.R")
source("analysis/R/_clinicalChar.R")
options(connectionObserver = NULL)
options(dplyr.summarise.inform = FALSE)
library(tidyverse, quietly = TRUE)


## Set variables -------------
configBlock <- "mrktscan"

outputFolder <- here::here("output", "03_baselineCharacteristics", configBlock)
outputPath <- fs::path(outputFolder)
fs::dir_create(outputPath)

cohortsToCreate <- readr::read_csv(here::here("output", "01_buildCohorts", configBlock, "cohortManifest.csv"),
                                   show_col_types = FALSE)

targetCohorts <- cohortsToCreate %>%
  dplyr::filter(type == "studyPop") %>%
  dplyr::select(id, name)

timeA <- c(-365L, -9999L)
timeB <- -1L

baselineSettings <- tidyr::expand_grid(targetCohorts, timeA) %>%
  dplyr::mutate(timeB = rep(timeB, 6))

baselineSettings


## Set Connection  --------------------
executionSettings <- getExecutionSettings(configBlock)
con <- DatabaseConnector::connect(executionSettings$connectionDetails)
startSnowflakeSession(con, executionSettings)


### Step 1: Get Demographics  --------------------

demographics <- purrr::pmap_dfr(
      targetCohorts,
    ~demographicBaselineMap(executionSettings = executionSettings,
                            con = con,
                            targetCohortsName = ..2,
                            targetCohortsId = ..1,
                            outputFolder = outputFolder)
)

View(demographics)

### Save results - Parquet
save_path <- fs::path(outputFolder, "demographics_baseline")
arrow::write_parquet(demographics, sink = save_path)


### Step 2: Get Continuous  --------------------

continuous <- purrr::pmap_dfr(
   targetCohorts,
  ~continuousBaselineMap(executionSettings = executionSettings,
                         con = con,
                         targetCohortsName = ..2,
                         targetCohortsId = ..1,
                         outputFolder = outputFolder)
)

View(continuous)

### Save results - Parquet
save_path <- fs::path(outputFolder, "continuous_baseline")
arrow::write_parquet(continuous, sink = save_path)


### Step 3: Get Drugs  --------------------

drugs <- purrr::pmap_dfr(
  baselineSettings,
  ~drugsCovariatesMap(executionSettings = executionSettings,
                   con = con,
                   targetCohortsName = ..2,
                   targetCohortsId = ..1,
                   timeA = ..3,
                   timeB = ..4,
                   outputFolder = outputFolder)
)

View(drugs)

### Save results - Parquet
save_path <- fs::path(outputFolder, "drugs")
arrow::write_parquet(drugs, sink = save_path)


### Step 4: Get Conditions  --------------------

conditionsGroup <- purrr::pmap_dfr(
  baselineSettings,
  ~conditionCovariatesGroupMap(executionSettings = executionSettings,
                              con = con,
                              targetCohortsName = ..2,
                              targetCohortsId = ..1,
                              timeA = ..3,
                              timeB = ..4,
                              outputFolder = outputFolder)
)

View(conditionsGroup)

### Save results - Parquet
save_path <- fs::path(outputFolder, "condition_groups")
arrow::write_parquet(conditionsGroup, sink = save_path)


### Step 5: Get Cohort Covariates  --------------------

covariateKey <- picard::cohortManifest() %>%
  dplyr::filter(type == "covariates")

cohortCov <- purrr::pmap_dfr(
   baselineSettings,
  ~cohortCovariatesMap(executionSettings = executionSettings,
                       con = con,
                       targetCohortsName = ..2,
                       targetCohortsId = ..1,
                       covariateKey = covariateKey,
                       timeA = ..3,
                       timeB = ..4,
                       outputFolder = outputFolder)
)

View(cohortCov)


### Save results - Parquet
save_path <- fs::path(outputFolder, "cohort_covariates")
arrow::write_parquet(cohortCov, sink = save_path)

