## Setup ----------------------
source("analysis/R/_executionSettings.R")
source("analysis/R/_treatmentPatterns.R")
library(txPath)
library(tidyverse)
options(connectionObserver = NULL)


## Set variables ----------------------
configBlock <- "optum"
outputFolder <- here::here("output", "07_treatmentPatterns", configBlock)

cohortsToCreate <- readr::read_csv(here::here("output", "01_buildCohorts", configBlock, "cohortManifest.csv"),
                                   show_col_types = FALSE)


## Set Connection -------------------------
executionSettings <- getExecutionSettings(configBlock)
con <- DatabaseConnector::connect(executionSettings$connectionDetails)
startSnowflakeSession(con = con, executionSettings = executionSettings)


## Treatment Patterns -------------------------
eraCollapseSize <- c(30, 60)

eventType <- c("ingredient", "class3", "class1")

targetCohorts <- cohortsToCreate %>%
  dplyr::filter(type == "studyPop")


for (i in 1:length(eraCollapseSize)) {

  for(j in 1:nrow(targetCohorts))  {

    for(k in 1:length(eventType)) {

      treatmentPatterns(executionSettings = executionSettings,
                        eraCollapseSize = eraCollapseSize[i],
                        eventType = eventType[k],
                        targetCohorts = targetCohorts[j,],
                        minNumPatterns = 30L,
                        outputFolder = outputFolder)

    }
  }
}

