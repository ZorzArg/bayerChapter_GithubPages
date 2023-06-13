## Setup ---------------
source("analysis/R/_executionSettings.R")
source("analysis/R/_incidence.R")
library(CohortIncidence)
library(tidyverse, quietly = TRUE)
options(connectionObserver = NULL)


## Set variables ---------------
configBlock <- "cprd_gold"

outputFolder <- here::here("output", "05_incidence", configBlock)
fs::dir_create(outputFolder)

cohortsToCreate <- readr::read_csv(here::here("output", "01_buildCohorts", configBlock, "cohortManifest.csv"),
                                   show_col_types = FALSE)

targetInputs <- cohortsToCreate %>%
  dplyr::filter(type == "studyPop" & name != "c5") %>% # Excluding cohort c5 per protocol
  dplyr::select(id, name) %>%
  dplyr::rename(targetId = id, targetName = name)

outcomeInputs <- cohortsToCreate %>%
  dplyr::filter(type == "outcome") %>%
  dplyr::select(id, name) %>%
  dplyr::rename(outcomeId = id, outcomeName = name)

subgroupInputs <- cohortsToCreate %>%
  dplyr::filter(type == "strata") %>%
  dplyr::select(id, name) %>%
  dplyr::rename(subgroupId = id, subgroupName = name)


## Set Connection ---------------
executionSettings <- getExecutionSettings(configBlock)


## IR manifest ---------------
windowInputs <- c("0-183", "184-365", "366-730", "731-1825")

irManifest <- tidyr::expand_grid(targetInputs, outcomeInputs ,subgroupInputs, windowInputs) %>%
  dplyr::mutate(
    id = dplyr::row_number(), .before = 1
  ) %>%
  tidyr::separate_wider_delim(
    cols = windowInputs,
    delim = "-",
    names = c("startOffset", "endOffset")
  ) %>%
  dplyr::mutate(
    startOffset = as.integer(startOffset),
    endOffset = as.integer(endOffset),
    startWith = "start",
    endWith = "start",
    cleanWindow = 0
  )


## IR Analysis ---------------
ageBreaks <- c(18, 40, 50, 60)

irAnalysis <- irManifest %>%
  purrr::pmap(function(...) {
    row <- tibble(...)
    irDesign(row = row, ageBreaks = ageBreaks)
  })


## Generate Incidence Analysis ---------------
ir_results <- purrr::map2_dfr(irAnalysis, 1:length(irAnalysis),
                              ~generateIncidence(
                                executionSettings = executionSettings,
                                incidenceDesign = .x,
                                refId = .y))

View(ir_results)


## Save results - Parquet ---------------
save_path <- fs::path(outputFolder, "ir")
arrow::write_parquet(ir_results, sink = save_path)
