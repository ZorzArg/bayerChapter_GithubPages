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
eraCollapseSize <- c(30,60)
#eraCollapseSize <- c(30)

eventType <- c("ingredient", "class3")
#eventType <- c("ingredient")

cohortManifest <- readr::read_csv(here::here("output", "01_buildCohorts", database, "cohortManifest.csv"),
                                  show_col_types = FALSE) %>%
  dplyr::filter(type == "studyPop")

#cohortManifest <- cohortManifest[3,]

for (i in 1:length(database)) {

  for (t in 1:nrow(cohortManifest)) {

    for (j in 1:length(eraCollapseSize)) {

      for (g in 1:length(eventType)) {

        tte <- readr::read_rds(here::here("output", "08_timeToInitialTreatment", database[i], cohortManifest$name[t], paste0("tte_", eventType[g], "_", eraCollapseSize[j], ".rds")))

        tteStratas <- names(tte) # Number of list elements (stratas)

        for (k in 1:length(tteStratas)) {

          if (tteStratas[k] == "total") {

            dataTTE <- tte[[k]]

            ## survFit rds for tables
            survFit <- ggsurvfit::survfit2(
              ggsurvfit::Surv(time_years, event) ~ type,
              data = dataTTE)

            # outputFolderRDS <- fs::path(here::here("output", "09_survfitObjects", database[i], cohortManifest$name[t]))
            # fs::dir_create(outputFolderRDS)
            #
            # readr::write_rds(survFit, here::here(outputFolderRDS, paste0("tte_", eventType[g], "_", eraCollapseSize[j], "_", tteStratas[k], ".rds")))


            outputFolderCSV <- fs::path(here::here("output", "09_TimePropTables", database[i], cohortManifest$name[t]))
            fs::dir_create(outputFolderCSV)

            ## Time table
            timeTable <- survFit %>% makeTimeTable()

            readr::write_csv(timeTable, here::here(outputFolderCSV,
                                                   paste0("timeTb_", database[i], "_", cohortManifest$name[t], "_", eventType[g], "_", eraCollapseSize[j], "_", tteStratas[k], ".csv")))

            ## Probability table
            probTable <- survFit %>% makeSurvTable()

            readr::write_csv(probTable, here::here(outputFolderCSV,
                                                   paste0("probTb_", database[i], "_", cohortManifest$name[t], "_", eventType[g], "_", eraCollapseSize[j], "_", tteStratas[k], ".csv")))


            ## Plot photo
            # outputFolderKM <- here::here("www",  database[i], cohortManifest$name[t])
            # fs::dir_create(outputFolderKM)
            #
            # kmPlotPhoto(fit = survFit,
            #             database = database[i],
            #             cohort = cohortManifest$name[t],
            #             eventType = eventType[g],
            #             eraCollapseSize = eraCollapseSize[j],
            #             tteStratas = tteStratas[k])

          } else {

            dataTTE <- tte[[k]]

            ## survFit rds for tables
            survFit <- ggsurvfit::survfit2(
              ggsurvfit::Surv(time_years, event) ~ typeAll,
              data = dataTTE)

            # outputFolderRDS <- fs::path(here::here("output", "09_survfitObjects", database[i], cohortManifest$name[t]))
            # fs::dir_create(outputFolderRDS)
            #
            # readr::write_rds(survFit, here::here(outputFolderRDS, paste0("tte_", eventType[g], "_", eraCollapseSize[j], "_", tteStratas[k], ".rds")))


            outputFolderCSV <- fs::path(here::here("output", "09_TimePropTables", database[i], cohortManifest$name[t]))
            fs::dir_create(outputFolderCSV)

            ## Time table
            timeTable <- survFit %>% makeTimeTable()

            readr::write_csv(timeTable, here::here(outputFolderCSV,
                                                 paste0("timeTb_", database[i], "_", cohortManifest$name[t], "_", eventType[g], "_", eraCollapseSize[j], "_", tteStratas[k], ".csv")))

            ## Probability table
            probTable <- survFit %>% makeSurvTable()

            readr::write_csv(probTable, here::here(outputFolderCSV,
                                                 paste0("probTb_", database[i], "_", cohortManifest$name[t], "_", eventType[g], "_", eraCollapseSize[j], "_", tteStratas[k], ".csv")))


            # ### Plot photo
            # outputFolderKM <- here::here("output", "www",  database[i], cohortManifest$name[t])
            # fs::dir_create(outputFolderKM)
            #
            # stratas <- unique(dataTTE$strata)
            #
            #
            # if (unique(dataTTE$strata_name == "age")) {
            #
            #   survFit_1 <- ggsurvfit::survfit2(
            #     ggsurvfit::Surv(time_years, event) ~ typeAll,
            #     data = dataTTE %>% dplyr::filter(strata == 1L))
            #
            #   survFit_2 <- ggsurvfit::survfit2(
            #     ggsurvfit::Surv(time_years, event) ~ typeAll,
            #     data = dataTTE %>% dplyr::filter(strata == 2L))
            #
            #   survFit_3 <- ggsurvfit::survfit2(
            #     ggsurvfit::Surv(time_years, event) ~ typeAll,
            #     data = dataTTE %>% dplyr::filter(strata == 3L))
            #
            #   survFit_4 <- ggsurvfit::survfit2(
            #     ggsurvfit::Surv(time_years, event) ~ typeAll,
            #     data = dataTTE %>% dplyr::filter(strata == 4L))
            #
            #
            #   survFit <- list(survFit_1, survFit_2, survFit_3, survFit_4)
            #   names(survFit) <- c("survFit_1", "survFit_2", "survFit_3", "survFit_4")
            #
            #   debug(kmPlotPhotoAgeStrata)
            #   kmPlotPhotoAgeStrata(fit = survFit,
            #                       database = database[i],
            #                       cohort = cohortManifest$name[t],
            #                       eventType = eventType[g],
            #                       eraCollapseSize = eraCollapseSize[j],
            #                       tteStratas = tteStratas[k])
            #
            # } else {
            #
            #   survFit_1 <- ggsurvfit::survfit2(
            #     ggsurvfit::Surv(time_years, event) ~ typeAll,
            #     data = dataTTE %>% dplyr::filter(strata == 1L))
            #
            #   survFit_0 <- ggsurvfit::survfit2(
            #     ggsurvfit::Surv(time_years, event) ~ typeAll,
            #     data = dataTTE %>% dplyr::filter(strata == 0L))
            #
            #
            #   survFit <- list(survFit_1, survFit_0)
            #   names(survFit) <- c("survFit_1", "survFit_0")
            #
            #   debug(kmPlotPhotoStrata)
            #   kmPlotPhotoStrata(fit = survFit,
            #                      database = database[i],
            #                      cohort = cohortManifest$name[t],
            #                      eventType = eventType[g],
            #                      eraCollapseSize = eraCollapseSize[j],
            #                      tteStratas = tteStratas[k])
            #
            #
            # }
          }
        }
      }
    }
  }
}


