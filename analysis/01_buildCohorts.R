## Setup ---------------------------
source("analysis/R/_buildCohorts.R")
source("analysis/R/_executionSettings.R")
options(connectionObserver = NULL)
options(dplyr.summarise.inform = FALSE)
library(tidyverse, quietly = TRUE)


## Set variables ----------------
configBlock <- "mrktscan"
outputFolder <- here::here("output","01_buildCohorts",configBlock)

cohortsToCreate <- picard::cohortManifest()  %>%
  dplyr::filter(type != "diagnostics")


## Set Connection  ---------------------------
executionSettings <- getExecutionSettings(configBlock)


## Initialize Cohort Tables
cohortTableNames <- initializeCohortTables(executionSettings = executionSettings)


## Generate cohorts  ---------------------------

debug(generateCohorts)
generatedCohorts <- generateCohorts(
  cohortTableNames = cohortTableNames,
  #con = con,
  executionSettings = executionSettings,
  cohortManifest = cohortsToCreate,
  outputFolder = outputFolder
)

