# Setup -------------------
library(tidyverse)
library(readr)
library(fs)
library(gtsummary)
library(ggsurvfit)
library(arrow)
library(patchwork)
source("analysis/R/_tte.R")


## Set variables -------------
database <- c("mrktscan", "optum", "cprd_aurum", "cprd_gold")


### 1. Time-to-event ----------------
eraCollapseSize <- c(30,60)

eventType <- c("ingredient", "class3", "class1")

cohortManifest <- readr::read_csv(here::here("output", "01_buildCohorts", database, "cohortManifest.csv"),
                                  show_col_types = FALSE) %>%
  dplyr::filter(type == "studyPop")


for (i in 1:length(database)) {

  for (t in 1:nrow(cohortManifest)) {

    for (j in 1:length(eraCollapseSize)) {

      for (g in 1:length(eventType)) {

          tte <- readr::read_rds(here::here("output", "08_timeToInitialTreatment", database[i], cohortManifest$name[t],
                                            paste0("tte_", eventType[g], "_", eraCollapseSize[j], ".rds")))
          tteStratas <- names(tte) # Number of list elements (stratas)

        for (k in 1:length(tteStratas)) {

          if (tteStratas[k] == "total") {

            dataTTE <- tte[[k]]

            ## Create survFit object
            survFit <- ggsurvfit::survfit2(
              ggsurvfit::Surv(time_years, event) ~ type,
              data = dataTTE)

            outputFolderCSV <- fs::path(here::here("output", "09_TimePropTables", database[i], cohortManifest$name[t]))
            fs::dir_create(outputFolderCSV)

            ## Time table
            timeTable <- survFit %>%
              makeTimeTable() %>%
              dplyr::mutate(database = database[i],
                            cohort = cohortManifest$name[t],
                            era = eraCollapseSize[j],
                            eventType = eventType[g],
                            strata = tteStratas[k])

            readr::write_csv(timeTable, here::here(outputFolderCSV,
                                                   paste0("timeTb_", database[i], "_", cohortManifest$name[t], "_", eventType[g], "_", eraCollapseSize[j], "_", tteStratas[k], ".csv")))

            ## Probability table
            probTable <- survFit %>%
              makeSurvTable() %>%
              dplyr::mutate(database = database[i],
                            cohort = cohortManifest$name[t],
                            era = eraCollapseSize[j],
                            eventType = eventType[g],
                            strata = tteStratas[k])

            readr::write_csv(probTable, here::here(outputFolderCSV,
                                                   paste0("probTb_", database[i], "_", cohortManifest$name[t], "_", eventType[g], "_", eraCollapseSize[j], "_", tteStratas[k], ".csv")))

            ## KM plot
            reportFolderKM <- here::here("report", "www",  database[i], cohortManifest$name[t])
            fs::dir_create(reportFolderKM)

            kmPlotPhoto(fit = survFit,
                        database = database[i],
                        cohort = cohortManifest$name[t],
                        eventType = eventType[g],
                        eraCollapseSize = eraCollapseSize[j],
                        tteStratas = tteStratas[k])

          } else {

            dataTTE <- tte[[k]]

            ## Create survFit object
            survFit <- ggsurvfit::survfit2(
              ggsurvfit::Surv(time_years, event) ~ typeAll,
              data = dataTTE)

            outputFolderCSV <- fs::path(here::here("output", "09_TimePropTables", database[i], cohortManifest$name[t]))
            fs::dir_create(outputFolderCSV)

            ## Time table
            timeTable <- survFit %>%
              makeTimeTable() %>%
              dplyr::mutate(database = database[i],
                            cohort = cohortManifest$name[t],
                            era = eraCollapseSize[j],
                            eventType = eventType[g],
                            strata = tteStratas[k])

            readr::write_csv(timeTable, here::here(outputFolderCSV,
                                                 paste0("timeTb_", database[i], "_", cohortManifest$name[t], "_", eventType[g], "_", eraCollapseSize[j], "_", tteStratas[k], ".csv")))

            ## Probability table
            probTable <- survFit %>%
              makeSurvTable() %>%
              dplyr::mutate(database = database[i],
                            cohort = cohortManifest$name[t],
                            era = eraCollapseSize[j],
                            eventType = eventType[g],
                            strata = tteStratas[k])

            readr::write_csv(probTable, here::here(outputFolderCSV,
                                                 paste0("probTb_", database[i], "_", cohortManifest$name[t], "_", eventType[g], "_", eraCollapseSize[j], "_", tteStratas[k], ".csv")))


            ## KM plot
            reportFolderKM <- here::here("report", "www",  database[i], cohortManifest$name[t])
            fs::dir_create(reportFolderKM)

            kmPlotPhoto(fit = survFit,
                        database = database[i],
                        cohort = cohortManifest$name[t],
                        eventType = eventType[g],
                        eraCollapseSize = eraCollapseSize[j],
                        tteStratas = tteStratas[k])

          }
        }
      }
    }
  }
}


