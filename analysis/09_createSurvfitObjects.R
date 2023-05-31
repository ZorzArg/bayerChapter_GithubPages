# Dependencies -------------------

library(tidyverse)
library(readr)
library(fs)
library(gtsummary)
library(ggsurvfit)
library(arrow)
library(patchwork)
source("analysis/R/_helper.R")

# Should be the name of the database as is in output folder
database <- "mrktscan"


## Time-to-event ----------------
#eraCollapseSize <- c(30,60)
eraCollapseSize <- c(30)

#eventType <- c("ingredient", "class3")
eventType <- c("ingredient")

cohortManifest <- readr::read_csv(here::here("output", "01_buildCohorts", database, "cohortManifest.csv"),
                                  show_col_types = FALSE) %>%
  dplyr::filter(type == "studyPop")

cohortManifest <- cohortManifest[4,]

for (i in 1:length(database)) {

  for (t in 1:nrow(cohortManifest)) {

    for (j in 1:length(eraCollapseSize)) {

      for (g in 1:length(eventType)) {

        tte <- readr::read_rds(here::here("output", "08_timeToInitialTreatment", database[i], cohortManifest$name[t], paste0("tte_", eventType[g], "_", eraCollapseSize[j], ".rds")))

        tteStratas <- names(tte) # Number of list elements (stratas)

      for (k in 1:length(tteStratas)) {

        if (tteStratas[k] == "total") {

          dataTTE <- tte[[k]]

          survFit <- ggsurvfit::survfit2(
            ggsurvfit::Surv(time_years, event) ~ type,
            data = dataTTE)

          outputFolder <- fs::path(here::here("output", "09_survfitObjects", database[i], cohortManifest$name[t]))
          fs::dir_create(outputFolder)

          readr::write_rds(survFit, here::here("output", "09_survfitObjects", database[i], cohortManifest$name[t],
                                               paste0("tte_", eventType[g], "_", eraCollapseSize[j], "_", tteStratas[k], ".rds")))

          outputFolderKM <- here::here("www",  database[i], cohortManifest$name[t])
          fs::dir_create(outputFolderKM)

          #debug(kmPlotPhoto)
          kmPlotPhoto(fit = survFit,
                      database = database[i],
                      cohort = cohortManifest$name[t],
                      eventType = eventType[g],
                      eraCollapseSize = eraCollapseSize[j],
                      tteStratas = tteStratas[k])

        } else {

          dataTTE <- tte[[k]]

          survFit <- ggsurvfit::survfit2(
            ggsurvfit::Surv(time_years, event) ~ typeAll,
            data = dataTTE)

          outputFolder <- fs::path(here::here("output", "09_survfitObjects", database[i], cohortManifest$name[t]))
          fs::dir_create(outputFolder)

          readr::write_rds(survFit, here::here("output", "09_survfitObjects", database[i], cohortManifest$name[t],
                                               paste0("tte_", eventType[g], "_", eraCollapseSize[j], "_", tteStratas[k], ".rds")))


          outputFolderKM <- here::here("output", "www",  database[i], cohortManifest$name[t])
          fs::dir_create(outputFolderKM)

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


