## Setup ---------------
source("analysis/R/_executionSettings.R")
source("analysis/R/_clinicalChar.R")
options(connectionObserver = NULL)
options(dplyr.summarise.inform = FALSE)
library(tidyverse, quietly = TRUE)


## Set variables ---------------
configBlock <- "optum"

outputFolder <- here::here("output", "04_postIndexDrugs", configBlock)
outputPath <- fs::path(outputFolder)
fs::dir_create(outputPath)

cohortsToCreate <- readr::read_csv(here::here("output", "01_buildCohorts" ,configBlock, "cohortManifest.csv"),
                                   show_col_types = FALSE)

targetCohorts <- cohortsToCreate %>%
  dplyr::filter(type == "studyPop") %>%
  dplyr::select(id, name)

timeA <- c(0, 184, 366, 731)
timeB <- c(183, 365, 730, 1825)

scriptSettings <- tidyr::expand_grid(targetCohorts, timeA) %>%
  dplyr::mutate(timeB = rep(timeB, 6))


## Set Connection ---------------
executionSettings <- getExecutionSettings(configBlock)
con <- DatabaseConnector::connect(executionSettings$connectionDetails)
startSnowflakeSession(con, executionSettings)


### 1. Get Post-Index Drugs ---------------
covariateKey <- cohortsToCreate %>%
  dplyr::filter(type %in% c("class3","class1"))

#debug(postIndexCovariatesMap)
postIndexDrugs <- purrr::pmap_dfr(
  scriptSettings,
  ~postIndexCovariatesMap(executionSettings = executionSettings,
                          con = con,
                          targetCohortsName = ..2,
                          targetCohortsId = ..1,
                          covariateKey = covariateKey,
                          timeA = ..3,
                          timeB = ..4,
                          outputFolder = outputFolder,
                          printSql = FALSE))

View(postIndexDrugs)


## Save results (Parquet)
save_path <- fs::path(outputFolder, "postIndex")
arrow::write_parquet(postIndexDrugs, sink = save_path)

