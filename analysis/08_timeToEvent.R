## Setup -------------------------
library(txPath)
library(tidyverse)
source("analysis/R/_tte.R")
source("analysis/R/_executionSettings.R")
options(connectionObserver = NULL)


## Set variables ----------------------
configBlock <- "mrktscan"
outputFolder <- here::here("output", "08_timeToInitialTreatment", configBlock)

cohortsToCreate <- readr::read_csv(here::here("output", "01_buildCohorts", configBlock, "cohortManifest.csv"),
                                   show_col_types = FALSE)


## Set Connection -------------------------
executionSettings <- getExecutionSettings(configBlock)
con <- DatabaseConnector::connect(executionSettings$connectionDetails)
startSnowflakeSession(con = con, executionSettings = executionSettings)


### 1. Time To Event Analysis -------------------------

eraCollapseSize <- c(30,60)

eventType <- c("ingredient", "class3", "class1")

targetCohorts <- cohortsToCreate %>%
  dplyr::filter(type == "studyPop")


for (i in 1:length(eraCollapseSize)) {

  for(j in 1:nrow(targetCohorts))  {

    for(k in 1:length(eventType)) {

  ttiDat <- timeToTreatmentDiscontinuationData(executionSettings = executionSettings,
                                       eraCollapseSize = eraCollapseSize[i],
                                       eventType = eventType[k],
                                       targetCohorts = targetCohorts[j,],
                                       outputFolder = outputFolder)

    }
  }
}


View(ttiDat$total)

