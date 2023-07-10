## Setup ---------------------------
source("analysis/R/_buildCohorts.R")
source("analysis/R/_executionSettings.R")
options(connectionObserver = NULL)
options(dplyr.summarise.inform = FALSE)
library(tidyverse, quietly = TRUE)


## Set variables ----------------
configBlock <- "cprd_aurum"
outputFolder <- here::here("output", "01_buildCohorts", configBlock)

cohortsToCreate <- picard::cohortManifest(inputPath = here::here("input/cohortsToCreate"))  %>%
  dplyr::filter(type != "diagnostics")


## Set Connection  ---------------------------
executionSettings <- getExecutionSettings(configBlock)


## Initialize Cohort Tables
dropCohortTables(executionSettings = executionSettings)

cohortTableNames <- initializeCohortTables(executionSettings = executionSettings)




### 1. Generate cohorts  ---------------------------

generatedCohorts <- generateCohorts(
  cohortTableNames = cohortTableNames,
  executionSettings = executionSettings,
  cohortManifest = cohortsToCreate,
  outputFolder = outputFolder
)

