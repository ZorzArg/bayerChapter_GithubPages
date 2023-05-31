## Setup ----------------------
source("analysis/R/_executionSettings.R")
source("analysis/R/_treatmentPatterns.R")
library(txPath)
library(tidyverse)
options(connectionObserver = NULL)


## Set variables ----------------------
configBlock <- "mrktscan"
outputFolder <- here::here("output", "07_treatmentPatterns", configBlock)

cohortsToCreate <- readr::read_csv(here::here("output","01_buildCohorts",configBlock,"cohortManifest.csv"),
                                   show_col_types = FALSE)


## Set Connection -------------------------
executionSettings <- getExecutionSettings(configBlock)
con <- DatabaseConnector::connect(executionSettings$connectionDetails)
startSnowflakeSession(con = con, executionSettings = executionSettings)


## Treatment Patterns -------------------------

eraCollapseSize <- c(30,60)
#eraCollapseSize <- 30

targetCohorts <- cohortsToCreate %>%
  dplyr::filter(type == "studyPop")
#targetCohorts <- targetCohorts[4,]

eventType <- c("ingredient", "class3")
#eventType <- c("ingredient")

for (i in 1:length(eraCollapseSize)) {

  for(j in 1:nrow(targetCohorts))  {

    for(k in 1:length(eventType)) {

      #debug(treatmentPatterns)
      patterns <- treatmentPatterns(executionSettings = executionSettings,
                                    eraCollapseSize = eraCollapseSize[i],
                                    eventType = eventType[k],
                                    targetCohorts = targetCohorts[j,],
                                    minNumPatterns = 30L,
                                    outputFolder = outputFolder)

    }
  }
}

patterns$total_total$treatmentPatterns
