## Setup ----------------------
source("analysis/R/_executionSettings.R")
source("analysis/R/_treatmentHistory.R")
options(connectionObserver = NULL)
options(dplyr.summarise.inform = FALSE)
library(tidyverse, quietly = TRUE)
library(data.table)


## Set variables ----------------------
configBlock <- "optum"
outputFolder <- here::here("output", "06_treatmentHistory", configBlock)


cohortsToCreate <- readr::read_csv(here::here("output", "01_buildCohorts", configBlock, "cohortManifest.csv"),
                                   show_col_types = FALSE)


## Set Connection ----------------------
executionSettings <- getExecutionSettings(configBlock)
con <- DatabaseConnector::connect(executionSettings$connectionDetails)
startSnowflakeSession(con = con, executionSettings = executionSettings)


### 1. Get Treatment History ----------------------
eraCollapseSize <- c(30, 60)

eventType <- c("ingredient", "class3", "class1")

targetId <- cohortsToCreate %>%
  dplyr::filter(type == "studyPop") %>%
  dplyr::pull(id)


for (i in 1:length(eraCollapseSize)) {

  for(j in 1:length(targetId))  {

    for(k in 1:length(eventType)) {

      eventId <- cohortsToCreate %>%
        dplyr::filter(type %in% c(eventType[k])) %>%
        dplyr::pull(id)

      thSettings <- cohortsToCreate %>%
        dplyr::filter(id %in% c(targetId[j], eventId)) %>%
        dplyr::mutate(
          cohort_id = as.integer(id),
          cohort_name = name,
          event_type = eventType[k],
          type = dplyr::case_when(
            type %in% c(eventType[k]) ~ "event",
            type %in% c("studyPop") ~ "studyPop"
          )
        ) %>%
        dplyr::select(cohort_id, cohort_name, type, event_type) %>%
        txPath::defineTreatmentHistory(
          minCellCount = 30L,
          eraCollapseSize = eraCollapseSize[i]
        )

  thDat <- treatmentHistory(executionSettings = executionSettings,
                            treatmentHistorySettings = thSettings,
                            outputFolder = outputFolder)

  }
 }
}

View(thDat)
