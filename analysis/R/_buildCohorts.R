# Functions for Build Cohorts Picard Script --------------------------

# Desc: Function to convert manifest to format for Cohort Generator
# Params:
# cohortManifest -> the cohort manifest listing all cohorts to build
# Return:
# returns a tibble structured cohortId, cohortName, json, sql where the
# json is the json file read as a character string and the sql is
# the json converted to ohdsisql using CirceR

prepManifestForCohortGenerator <- function(cohortManifest) {

  cohortsToCreate <- cohortManifest %>%
    dplyr::mutate(
      json = purrr::map_chr(file, ~ readr::read_file(.x))
    ) %>%
    dplyr::select(id, name, json) %>%
    dplyr::rename(cohortId = id, cohortName = name)

  cohortsToCreate$sql <- purrr::map_chr(
    cohortsToCreate$json,
    ~CirceR::buildCohortQuery(CirceR::cohortExpressionFromJson(.x),
                              CirceR::createGenerateOptions(generateStats = TRUE)))

  return(cohortsToCreate)

}

# Desc: Wrapper for Cohort Generator
# HADES Dependencies: CohortGenerator, CirceR
# Params:
# executionSettings -> an executionSettings object specifying the db connection
# cohortManifest -> a tibble listing all cohorts in the project
# outputFolder -> a folder specifying where to output the incremental file
# type -> either analysis of diagnostics cohort table
# Return:
# a tibble with the cohort counts after generation. function will also print progress
# of cohort generation

generateCohorts <- function(executionSettings, cohortManifest, outputFolder, con, cohortTableNames,
                            type = "analysis") {

  ## Create output directory
  fs::dir_create(outputFolder)

  ## Set variables
  if (executionSettings$connectionDetails$dbms == "snowflake") {

    writeSchema <- paste(executionSettings$writeDatabase, executionSettings$writeSchema, sep = ".")
    cdmSchema <- paste(executionSettings$cdmDatabase, executionSettings$cdmSchema, sep = ".")
    vocabularySchema <- paste(executionSettings$cdmDatabase, executionSettings$vocabSchema, sep = ".")

  } else {

    writeSchema <- executionSettings$writeSchema
    cdmSchema <- executionSettings$cdmSchema
    vocabularySchema <- executionSettings$vocabSchema
  }

  cohortTable <- paste0(executionSettings$cohortTable,"_",executionSettings$databaseId)
  cohortsToCreate <- prepManifestForCohortGenerator(cohortManifest)
  incrementalFolder <- fs::path(outputFolder)

  con <- DatabaseConnector::connect(executionSettings$connectionDetails)
  startSnowflakeSession(con = con, executionSettings = executionSettings)

  ## Generate cohorts
  CohortGenerator::generateCohortSet(
    connection = con,
    cdmDatabaseSchema = cdmSchema,
    cohortDatabaseSchema = writeSchema,
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortsToCreate,
    incremental = TRUE,
    incrementalFolder = incrementalFolder
  )

  ## Get cohort counts
  cohortCounts <- CohortGenerator::getCohortCounts(
    connection = con,
    cohortDatabaseSchema = writeSchema,
    cohortTable = cohortTable,
    cohortDefinitionSet = cohortsToCreate
  )%>%
  dplyr::select(cohortId, cohortName, cohortEntries, cohortSubjects)

  ## Save cohort counts
  tb <- cohortManifest %>%
    dplyr::left_join(cohortCounts %>%
                       dplyr::select(cohortId, cohortEntries, cohortSubjects),
                     by = c("id" = "cohortId")) %>%
    dplyr::rename(
      entries = cohortEntries,
      subjects = cohortSubjects) %>%
    dplyr::mutate(database = executionSettings$databaseId,
                  databaseFullName = dplyr::case_when(
                    database == "mrktscan" ~ "MarketScan",
                    database == "optum" ~ "Optum Claims",
                    database == "cprd_gold" ~ "CPRD Gold",
                    database == "cprd_aurum" ~ "CPRD Aurum"
                  ),
                  fullName = dplyr::case_when(
                    name == "c1" ~ "Natural menopausal women",
                    name == "c2" ~ "Natural menopausal women contraindicated for HT",
                    name == "c3" ~ "Women with natural menopause who have a family history of breast cancer",
                    name == "c4" ~ "Women with or at high risk of breast cancer exposed to endocrine-adjuvant therapy",
                    name == "c5" ~ "Women with menopause symptoms",
                    name == "c6" ~ "Women with initial prescription to hormone therapy",
                    name == "anxiety" ~ "Anxiety",
                    name == "bipolar" ~ "Bipolar",
                    name == "combinedMood" ~ "Mood disorders",
                    name == "depression" ~ "Depression",
                    name == "ivms" ~ "iVMS",
                    name == "vms" ~ "VMS",
                    name == "organicSleep" ~ "Organic sleep",
                    name == "sleepDisturbances" ~ "Sleep disturbances",
                    name == "alopecia" ~ "Alopecia",
                    name == "breastCancer" ~ "Breast cancer",
                    name == "cancer" ~ "Cancer",
                    name == "gynCancer" ~ "Gynecological cancers",
                    name == "headache" ~ "Headache",
                    name == "hypertension" ~ "Hypertension",
                    name == "hyperthyroidism" ~ "Hyperthyroidism",
                    name == "hypothyroidism" ~ "Hypothyroidism",
                    name == "malaiseOrFatigue" ~ "Malaise or fatigue",
                    name == "migraine" ~ "Migraine",
                    name == "myocardial" ~ "Myocardial infarction",
                    name == "osteoarthritis" ~ "Osteoarthritis",
                    name == "osteoporosis" ~ "Osteoporosis",
                    name == "stroke" ~ "Stroke",
                    name == "t2dm" ~ "Type 2 Diabetes",
                    name == "vte" ~ "VTE",
                    name == "anticonvulsants_lvl3" ~ "Anticonvulsants (Level 3)",
                    name == "antidepressants_lvl3" ~ "Antidepressants (Level 3)",
                    name == "antihypertensives_lvl3" ~ "Antihypertensives (Level 3)",
                    name == "benzodiazepines_lvl3" ~ "Benzodiazepines (Level 3)",
                    name == "hormoneTherapy_lvl3" ~ "Hormone therapy (Level 3)",
                    name == "anticonvulsants_lvl1" ~ "Anticonvulsants (Level 1)",
                    name == "antidepressants_lvl1" ~ "Antidepressants (Level 1)",
                    name == "antihypertensives_lvl1" ~ "Antihypertensives (Level 1)",
                    name == "benzodiazepines_lvl1" ~ "Benzodiazepines (Level 1)",
                    name == "hormoneTherapy_lvl1" ~ "Hormone therapy (Level 1)",
                    name == "benzodiazepines" ~ "Benzodiazepines",
                    name == "amitriptyline" ~ "Amitriptyline",
                    name == "citalopram" ~ "Citalopram",
                    name == "clonidine" ~ "Clonidine",
                    name == "desvenlafaxine" ~ "Desvenlafaxine",
                    name == "escitalopram" ~ "Escitalopram",
                    name == "estrogens" ~ "Estrogens",
                    name == "estrogensCombo" ~ "Estrogens: combinations",
                    name == "fluoxetine" ~ "Fluoxetine",
                    name == "gabapentin" ~ "Gabapentin",
                    name == "paroxetine" ~ "Paroxetine",
                    name == "pregabalin" ~ "Pregabalin",
                    name == "progestogens" ~ "Progestogens",
                    name == "progestogensEstrogens" ~ "Progestogens & estrogens",
                    name == "raloxifene" ~ "Raloxifene",
                    name == "sertraline" ~ "Sertraline",
                    name == "venlafaxine" ~ "Venlafaxine",
                    name == "vaginalDryness" ~ "Vaginal dryness"
                  )) %>%
    dplyr::select(id, name, fullName, type, entries, subjects, file, database, databaseFullName)

  savePath <- fs::path(outputFolder, "cohortManifest.csv")
  readr::write_csv(x = tb, file = savePath)

  savePath <- fs::path(outputFolder, paste0("cohortManifest_", str_replace_all(Sys.Date(), "-", "_"), ".csv"))
  readr::write_csv(x = tb, file = savePath)

  cli::cat_bullet("Saving Generated Cohorts to ", crayon::cyan(savePath), bullet = "tick", bullet_col = "green")

  DatabaseConnector::disconnect(con)
  cli::cat_line("Database connection closed")

  return(tb)

}
