# Capr Templates ----------------------
conditionCohort <- function(path) {

  #set up name for cohorts
  name <- tools::file_path_sans_ext(basename(path))
  conceptSet <- Capr::readConceptSet(path = path, name = name)

  cd <- Capr::cohort(
    entry = Capr::entry(
      Capr::condition(conceptSet),
      observationWindow = Capr::continuousObservation(0, 0)
    ),
    exit = Capr::exit(
      endStrategy = Capr::observationExit()
    )
  )

  res <- Capr::toCirce(cd) %>%
    jsonlite::toJSON(auto_unbox = TRUE, pretty = TRUE) %>%
    as.character()

  return(res)

}

drugCohort <- function(path) {

  #set up name for cohorts
  name <- tools::file_path_sans_ext(basename(path))
  conceptSet <- Capr::readConceptSet(path = path, name = name)

  #create visit
  anyVisit <- Capr::cs(Capr::descendants(9201, 9202, 9203, 262))
  ageStrata <- Capr::age(gte(65L))

  cd <- Capr::cohort(
    entry = Capr::entry(
      Capr::drug(conceptSet),
      observationWindow = Capr::continuousObservation(365L, 0)
    ),
    exit = Capr::exit(
      endStrategy = Capr::observationExit(),
      censor = Capr::censoringEvents(
        Capr::visit(anyVisit, ageStrata) # censor of visit
      )
    )
  )

  res <- Capr::toCirce(cd) %>%
    jsonlite::toJSON(auto_unbox = TRUE, pretty = TRUE) %>%
    as.character()

  return(res)


}


# Build Json --------------------------

## Condition Covariates ----------------------

savePath <- here::here("input/cohortsToCreate/03_covariates")
conceptPath <- here::here("input/conceptSets/covariates") %>%
  fs::dir_ls() %>%
  unname()
covName <- tools::file_path_sans_ext(basename(conceptPath))
saveName <- fs::path(savePath, covName, ext = "json")
conditionCov <- purrr::map(conceptPath, ~conditionCohort(.x))

purrr::walk2(conditionCov, saveName, ~readr::write_file(x = .x, file = .y))

## strata cohorts ---------------------

savePath <- here::here("input/cohortsToCreate/02_strata")
conceptPath <- here::here("input/conceptSets/strata") %>%
  fs::dir_ls() %>%
  unname()
covName <- tools::file_path_sans_ext(basename(conceptPath))
saveName <- fs::path(savePath, covName, ext = "json")
conditionCov <- purrr::map(conceptPath, ~conditionCohort(.x))

purrr::walk2(conditionCov, saveName, ~readr::write_file(x = .x, file = .y))

## outcome cohorts ---------------------

savePath <- here::here("input/cohortsToCreate/06_outcome")
conceptPath <- here::here("input/conceptSets/outcome") %>%
  fs::dir_ls() %>%
  unname()
covName <- tools::file_path_sans_ext(basename(conceptPath))
saveName <- fs::path(savePath, covName, ext = "json")
conditionCov <- purrr::map(conceptPath, ~conditionCohort(.x))

purrr::walk2(conditionCov, saveName, ~readr::write_file(x = .x, file = .y))
