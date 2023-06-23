# Load data ------------------

## Bind rows of data frames located in the same folder (deleting files after binding)
bindFiles <- function(inputPath,
                      outputPath,
                      filename)  {

    ## List all csv files in folder
    filepath <- list.files(inputPath, pattern = filename, full.names = TRUE)

    ## Read all files and save in list
    listed_files <- lapply(filepath, readr::read_csv, show_col_types = FALSE)

    ## Created binded data frame with all data frames of list
    binded_df <- dplyr::bind_rows(listed_files)

    ## Save output
    fs::dir_create(outputPath)

    readr::write_csv(
      x = binded_df,
      file = file.path(outputPath, paste0(filename, ".csv")),
      append = FALSE
    )

  ## Delete files from directory
  invisible(lapply(filepath, unlink))

}


## Bind rows of data frames located in the same folder
bindFiles2 <- function(path,
                       databaseName,
                       cohortName,
                       filename)  {

    inputPath <- here::here(path, databaseName, cohortName)

    ## List all csv files in folder
    filepath <- list.files(inputPath, pattern = filename, full.names = TRUE)

    ## Read all files and save in list
    listed_files <- lapply(filepath, readr::read_csv, show_col_types = FALSE)

    ## Created binded data frame with all data frames of list
    binded_df <- dplyr::bind_rows(listed_files)

    ## Save output
    outputPath <- here::here(path)
    fs::dir_create(outputPath)

    readr::write_csv(
      x = binded_df,
      file = file.path(outputPath, paste0(filename, "_", databaseName, ".csv")),
      append = FALSE
    )

}



loadCohortManifest <- function(database) {

  inputPath <- here::here("output", "01_buildCohorts")

  for (i in 1:length(database)) {

    cohortManifestDb <- readr::read_csv(here::here(inputPath, database[i], "cohortManifest.csv"), show_col_types = FALSE)
    readr::write_csv(cohortManifestDb, here::here(inputPath, paste0("cohortManifest_", database[i], ".csv")))

  }

  bindFiles(inputPath = inputPath,
            outputPath = here::here("report", "data", "01_buildCohorts"),
            filename = "cohortManifest")

}


loadStrataManifest <- function(database) {

  inputPath <- here::here("output", "02_buildStrata")

  for (i in 1:length(database)) {

    strataManifestDb <- readr::read_csv(here::here(inputPath, database[i], "strataManifest.csv"), show_col_types = FALSE)
    readr::write_csv(strataManifestDb, here::here(inputPath, paste0("strataManifest_", database[i], ".csv")))

  }

  bindFiles(inputPath = inputPath,
             outputPath = here::here("report", "data", "02_buildStrata"),
             filename = "strataManifest")

}


loadBaseline <- function(database) {

  inputPath <- here::here("output", "03_baselineCharacteristics")

  for (i in 1:length(database)) {

    #debug(formatTable1)
    baseline <- formatTable1(database = database[i], outputPath = inputPath)
    readr::write_csv(baseline, here::here(inputPath, paste0("baseline_", database[i], ".csv")))

  }

  bindFiles(inputPath = inputPath,
             outputPath = here::here("report", "data", "03_baselineCharacteristics"),
             filename = "baseline")

}


loadPostIndex <- function(database) {

  inputPath <- here::here("output", "04_postIndexDrugs")

  for (i in 1:length(database)) {

    postIndexDb <- arrow::read_parquet(here::here(inputPath, database[i], "postIndex"))
    readr::write_csv(postIndexDb, here::here(inputPath,  paste0("postIndex_", database[i], ".csv")))

  }

  bindFiles(inputPath = inputPath,
             outputPath = here::here("report", "data", "04_postIndex"),
             filename = "postIndex")

}


loadIncidence <- function(database) {

  inputPath <- here::here("output", "05_incidence")

  for (i in 1:length(database)) {

    incidenceDb <- arrow::read_parquet(here::here(inputPath, database[i], "ir"))
    readr::write_csv(incidenceDb, here::here(inputPath,  paste0("incidence_", database[i], ".csv")))

  }

  bindFiles(inputPath = inputPath,
             outputPath = here::here("report", "data", "05_incidence"),
             filename = "incidence")

}


loadTreatmentPathways <- function(database,
                                  cohort) {

  inputPath <- here::here("output", "07_treatmentPatterns")

  for(i in 1:length(database)) {

      bindFiles2(path = inputPath,
                  database = database[i],
                  cohortName = cohort,
                  filename = "tp_")

    }

  bindFiles(inputPath = here::here(inputPath),
            outputPath = here::here("report", "data", "06_treatmentPatterns"),
            filename = "tp")

}


loadTTE <- function(database,
                    cohort) {

  inputPath <- here::here("output", "09_TimePropTables")

  for (i in 1:length(database)) {

      bindFiles2(filename = "probTb",
                  databaseName = database[i],
                  cohortName = cohort,
                  path = inputPath)

      bindFiles2(filename = "timeTb",
                  databaseName = database[i],
                  cohortName = cohort,
                  path = inputPath)

    }

    bindFiles(outputPath = here::here("report", "data", "07_TTE"),
              inputPath = inputPath,
              filename = "probTb")

    bindFiles(outputPath = here::here("report", "data", "07_TTE"),
              inputPath = inputPath,
              filename = "timeTb")

}



# Baseline  -------------------------------

formatTable1 <- function(outputPath,
                         database)  {

  outputPath <- here::here(outputPath, database)

  # Format demographics
  demoTbl <- arrow::read_parquet(
    file = here::here(outputPath, "demographics_baseline")
  ) %>%
    dplyr::filter(nn >= 5) %>% ## Exclude counts lower than 5
    dplyr::mutate(
      category = "Demographics",
      nn = format(nn, big.mark = ",", scientific = FALSE),
      pct = scales::label_percent(0.1, suffix = "")(pct),
      value = paste0(nn, " (", pct, ")"),
      name = paste(categoryName, conceptName, sep = ": ")
    ) %>%
    tibble::add_column(p25 = NA,
                       p75 = NA,
                       mean = NA,
                       sd = NA,
                       median = NA) %>%
    dplyr::select(database, cohort, category, name, window, strata_id, strata, value, nn, total, pct, mean, median, p25, p75, sd)


  # Format continuous
  ctsTbl <- arrow::read_parquet(
    file = here::here(outputPath, "continuous_baseline")
  ) %>%
    dplyr::mutate(
      name = categoryName,
      category = "Continuous",
      value = paste0(median, " (", p25, ",", p75, ")"),
    ) %>%
    tibble::add_column(nn = NA,
                       pct = NA,
                       total = NA) %>%
    dplyr::select(database, cohort, category, name, window, strata_id, strata, value, nn, pct, mean, median, p25, p75, sd)


  # Format cohort covariates
  cohortTbl <- arrow::read_parquet(
    file = here::here(outputPath, "cohort_covariates")
  ) %>%
    dplyr::filter(nn >= 5) %>% ## Exclude counts lower than 5
    dplyr::mutate(
      strata_id = strataId,
      category = "Cohort",
      nn = format(nn, big.mark = ",", scientific = FALSE),
      pct = scales::label_percent(0.1, suffix = "")(pct),
      value = paste0(nn, " (", pct, ")"),
      name = cohortCovariateName
    ) %>%
    tibble::add_column(p25 = NA,
                       p75 = NA,
                       mean = NA,
                       sd = NA,
                       median = NA) %>%
    dplyr::select(database, cohort, category, name, window, strata_id, strata, value, nn, total, pct, mean, median, p25, p75, sd)


  # Format condition groups
  condGroupTbl <- arrow::read_parquet(
    file = here::here(outputPath, "condition_groups")
  ) %>%
    dplyr::filter(nn >= 5) %>% ## Exclude counts lower than 5
    dplyr::mutate(
      category = "Condition Groups",
      nn = format(nn, big.mark = ",", scientific = FALSE),
      pct = scales::label_percent(0.1, suffix = "")(pct),
      value = paste0(nn, " (", pct, ")"),
      name = categoryName
    ) %>%
    tibble::add_column(p25 = NA,
                       p75 = NA,
                       mean = NA,
                       sd = NA,
                       median = NA) %>%
    dplyr::select(database, cohort, category, name, window, strata_id, strata, value, nn, total, pct, mean, median, p25, p75, sd)


  # ## Prep conditions
  # condTbl <- arrow::read_parquet(
  #   file = here::here(outputPath, "conditions")
  # ) %>%
  #   dplyr::filter(nn >= 5) %>% ## Exclude counts lower than 5
  #   dplyr::mutate(
  #     category = "Conditions",
  #     nn = format(nn, big.mark = ",", scientific = FALSE),
  #     pct = scales::label_percent(0.1, suffix = "")(pct),
  #     value = paste0(nn, " (", pct, ")"),
  #     name = conceptName
  #   ) %>%
  #   tibble::add_column(p25 = NA,
  #                      p75 = NA,
  #                      mean = NA,
  #                      sd = NA,
  #                      median = NA) %>%
  #   dplyr::ungroup() %>%
  #   dplyr::select(database, cohort, category, name, window, strata_id, strata, value, nn, total, pct, mean, median, p25, p75, sd)


  # Format drugs
  drugTbl <- arrow::read_parquet(
    file = here::here(outputPath, "drugs")
  ) %>%
    dplyr::filter(categoryId == conceptId,
                  nn >= 5,                  ## Exclude counts lower than 5
                  pct >= 0.02) %>%
    dplyr::mutate(
      category = "Drugs",
      nn = format(nn, big.mark = ",", scientific = FALSE),
      pct = scales::label_percent(0.1, suffix = "")(pct),
      value = paste0(nn, " (", pct, ")"),
      name = categoryName
    ) %>%
    tibble::add_column(p25 = NA,
                       p75 = NA,
                       mean = NA,
                       sd = NA,
                       median = NA) %>%
    dplyr::select(database, cohort, category, name, window, strata_id, strata, value, nn, total, pct, mean, median, p25, p75, sd)


  # Bind results together
  all_baseline <- dplyr::bind_rows(demoTbl, ctsTbl, cohortTbl, condGroupTbl, drugTbl) %>%
    dplyr::mutate(mean = signif(mean, 2),
                  median = signif(median, 2),
                  sd = signif(sd, 2),
                  p25 = signif(p25, 2),
                  p75 = signif(p75, 2))

  return(all_baseline)

}



# Other ----------
## notin ---------
`%notin%` <- Negate("%in%")

