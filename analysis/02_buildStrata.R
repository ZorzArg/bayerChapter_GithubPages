## Setup  --------------------
options(connectionObserver = NULL)
options(dplyr.summarise.inform = FALSE)
source("analysis/R/_executionSettings.R")
library(tidyverse, quietly = TRUE)


configBlock <- "mrktscan"
outputFolder <- here::here("output", "02_buildStrata", configBlock)

## Set Connection  --------------------
executionSettings <- getExecutionSettings(configBlock)
con <- DatabaseConnector::connect(executionSettings$connectionDetails)
startSnowflakeSession(con, executionSettings)


## Set variables  --------------------
if (executionSettings$connectionDetails$dbms == "snowflake") {

  writeSchema <- paste(executionSettings$writeDatabase, executionSettings$writeSchema, sep = ".")
  cdmSchema <- paste(executionSettings$cdmDatabase, executionSettings$cdmSchema, sep = ".")

} else {

  writeSchema <- executionSettings$writeSchema
  cdmSchema <- executionSettings$cdmSchema

}

cohortTable <- paste(executionSettings$cohortTable, configBlock, sep = "_")
strataTable <- paste(executionSettings$cohortTable, "strata", configBlock, sep = "_")

cohortsToCreate <- readr::read_csv(here::here("output","01_buildCohorts",configBlock,"cohortManifest.csv"),
                                   show_col_types = FALSE)

targetCohortIds <- cohortsToCreate %>%
  dplyr::filter(type == "studyPop") %>%
  dplyr::pull(id)

strataCohorts <- cohortsToCreate %>%
  dplyr::filter(type == "strata")

anxietyId    <- strataCohorts %>% dplyr::filter(name == "anxiety") %>% dplyr::pull(id)
bipolarId    <- strataCohorts %>% dplyr::filter(name == "bipolar") %>% dplyr::pull(id)
moodDisId    <- strataCohorts %>% dplyr::filter(name == "combinedMood") %>% dplyr::pull(id)
depressId    <- strataCohorts %>% dplyr::filter(name == "depression") %>% dplyr::pull(id)
ivmsId       <- strataCohorts %>% dplyr::filter(name == "ivms") %>% dplyr::pull(id)
orgSleepId   <- strataCohorts %>% dplyr::filter(name == "organicSleep") %>% dplyr::pull(id)
sleepDistId  <- strataCohorts %>% dplyr::filter(name == "sleepDisturbances") %>% dplyr::pull(id)
vmsId        <- strataCohorts %>% dplyr::filter(name == "vms") %>% dplyr::pull(id)


strataManifest <- tibble::tribble(
  ~strata_id, ~strata, ~strata_name, ~strata_value, ~ strataFullName, ~stratumFullName,
  1, 1, "total", "total", "Total",  "Total",
  2, 0, "age", "other", "Age", "Other",
  2, 1, "age", "18-39", "Age", "18-39",
  2, 2, "age", "40-49", "Age", "40-49",
  2, 3, "age", "50-59", "Age", "50-59",
  2, 4, "age", "60-65", "Age", "60-65",
  3, 0, "sleepDisturbances", "no", "Sleep Disturbances", "No",
  3, 1, "sleepDisturbances", "yes", "Sleep Disturbances", "Yes",
  4, 0, "vms", "no", "VMS", "No",
  4, 1, "vms", "yes", "VMS", "Yes",
  5, 0, "moodDisorder", "no", "Mood Disorder", "No",
  5, 1, "moodDisorder", "yes", "Mood Disorder", "Yes",
  6, 0, "depression", "no", "Depression", "No",
  6, 1, "depression", "yes", "Depression", "Yes",
  7, 0, "anxiety", "no", "Anxiety", "No",
  7, 1, "anxiety", "yes", "Anxiety", "Yes",
  8, 0, "bipolar", "no", "Bipolar", "No",
  8, 1, "bipolar", "yes", "Bipolar", "Yes",
  9, 0, "ivms", "no", "iVMS", "No",
  9, 1, "ivms", "yes", "iVMS", "Yes",
  10, 0, "organicSleep", "no", "Organic Sleep", "No",
  10, 1, "organicSleep", "yes", "Organic Sleep", "Yes"
)

#View(strataManifest)


## Create strata table ----------------------
sql <-
  "IF OBJECT_ID('@write_db_schema.@strata_table', 'U') IS NULL
  CREATE TABLE @write_db_schema.@strata_table (
    strata_id INT,
    cohort_definition_id BIGINT,
    subject_id BIGINT,
    strata INT);"  %>%
  SqlRender::render(
    write_db_schema = writeSchema,
    strata_table = strataTable
  ) %>%
  SqlRender::translate(executionSettings$connectionDetails$dbms)

DatabaseConnector::executeSql(connection = con, sql = sql, progressBar = FALSE)


## Delete from strata table
sql <-
  "DELETE FROM @write_db_schema.@strata_table
   WHERE cohort_definition_id  IN (@cohortId);"  %>%
  SqlRender::render(
    write_db_schema = writeSchema,
    strata_table = strataTable,
    cohortId = targetCohortIds
  ) %>%
  SqlRender::translate(executionSettings$connectionDetails$dbms)

DatabaseConnector::executeSql(connection = con, sql = sql, progressBar = FALSE)


## Build stratas  --------------------
for (i in 1:length(targetCohortIds)) {

  targetCohortId <- targetCohortIds[i]

### 1.Total Strata --------------------

sql <-
  "INSERT INTO @writeSchema.@strataTable (strata_id, cohort_definition_id, subject_id, strata)
  SELECT
    1 AS strata_id,
    cohort_definition_id,
    subject_id,
    1 AS strata
  FROM @writeSchema.@cohortTable
  WHERE cohort_definition_id  = @cohortId
;"

totalStrataSql <- SqlRender::render(
  sql,
  writeSchema = writeSchema,
  cohortTable = cohortTable,
  strataTable = strataTable,
  cohortId = targetCohortId) %>%
  SqlRender::translate(targetDialect = executionSettings$connectionDetails$dbms)

DatabaseConnector::executeSql(connection = con, totalStrataSql, progressBar = FALSE)


### 2.Age Strata --------------------

sql <-
  "WITH t1 AS (
  SELECT lhs.*, rhs.year_of_birth
  FROM (
    SELECT *
    FROM @writeSchema.@cohortTable
    WHERE cohort_definition_id  = @cohortId
  ) AS lhs
  LEFT JOIN @cdmSchema.person AS rhs
    ON lhs.subject_id = rhs.person_id
),
t2 AS (
  SELECT
    *,
    CAST(YEAR(cohort_start_date) - year_of_birth AS INT) AS age
  FROM t1
)
INSERT INTO @writeSchema.@strataTable (strata_id, cohort_definition_id, subject_id, strata)
  SELECT
    2 AS strata_id,
    cohort_definition_id,
    subject_id,
    (CASE
      WHEN (age BETWEEN 18 AND 39) THEN 1
      WHEN (age BETWEEN 40 AND 49) THEN 2
      WHEN (age BETWEEN 50 AND 59) THEN 3
      WHEN (age BETWEEN 60 AND 65) THEN 4
      ELSE 0
    END) AS strata
  FROM t2
;"

ageStrataSql <- SqlRender::render(
  sql,
  writeSchema = writeSchema,
  cohortTable = cohortTable,
  cdmSchema = cdmSchema,
  strataTable = strataTable,
  cohortId = targetCohortId) %>%
  SqlRender::translate(targetDialect = executionSettings$connectionDetails$dbms)

DatabaseConnector::executeSql(connection = con, ageStrataSql, progressBar = FALSE)


### 3.Sleep disturbances Strata --------------------

sql <-
  "WITH t1 AS (
  SELECT *
  FROM @writeSchema.@cohortTable
  WHERE cohort_definition_id  = @cohortId
),
t2 AS (
  SELECT
    t1.cohort_definition_id AS cohort_definition_id_target,
    t1.subject_id AS subject_id,
    t1.cohort_start_date AS cohort_start_date_target,
    t1.cohort_end_date AS cohort_end_date_target,
    rhs.cohort_definition_id AS cohort_definition_id_strata,
    rhs.cohort_start_date AS cohort_start_date_strata,
    rhs.cohort_end_date AS cohort_end_date_strata
  FROM t1
  LEFT JOIN(
    SELECT *
    FROM @writeSchema.@cohortTable
    WHERE cohort_definition_id = @cohortId2
  ) rhs
    ON t1.subject_id = rhs.subject_id
),
t3 AS(
  SELECT
  *,
  DATEADD(day, 0, cohort_start_date_target) AS a,
  DATEADD(day, 0, cohort_start_date_target) AS b
  FROM t2
)
INSERT INTO @writeSchema.@strataTable (strata_id, cohort_definition_id, subject_id, strata)
  SELECT
    3 AS strata_id,
    cohort_definition_id_target AS cohort_definition_id,
    subject_id,
    (CASE WHEN cohort_start_date_strata BETWEEN a AND b THEN 1 ELSE 0 END) AS strata
  FROM t3
;"

sleepDistStrataSql <- SqlRender::render(
  sql,
  writeSchema = writeSchema,
  cohortTable = cohortTable,
  strataTable = strataTable,
  cohortId = targetCohortId,
  cohortId2 = sleepDistId
) %>%
  SqlRender::translate(targetDialect = executionSettings$connectionDetails$dbms)

DatabaseConnector::executeSql(connection = con, sleepDistStrataSql, progressBar = FALSE)


### 4.VMS Strata --------------------

sql <-
  "WITH t1 AS (
  SELECT *
  FROM @writeSchema.@cohortTable
  WHERE cohort_definition_id  = @cohortId
),
t2 AS (
  SELECT
    t1.cohort_definition_id AS cohort_definition_id_target,
    t1.subject_id AS subject_id,
    t1.cohort_start_date AS cohort_start_date_target,
    t1.cohort_end_date AS cohort_end_date_target,
    rhs.cohort_definition_id AS cohort_definition_id_strata,
    rhs.cohort_start_date AS cohort_start_date_strata,
    rhs.cohort_end_date AS cohort_end_date_strata
  FROM t1
  LEFT JOIN(
    SELECT *
    FROM @writeSchema.@cohortTable
    WHERE cohort_definition_id = @cohortId2
  ) rhs
    ON t1.subject_id = rhs.subject_id
),
t3 AS(
  SELECT
  *,
  DATEADD(day, 0, cohort_start_date_target) AS a,
  DATEADD(day, 0, cohort_start_date_target) AS b
  FROM t2
)
INSERT INTO @writeSchema.@strataTable (strata_id, cohort_definition_id, subject_id, strata)
  SELECT
    4 AS strata_id,
    cohort_definition_id_target AS cohort_definition_id,
    subject_id,
    (CASE WHEN cohort_start_date_strata BETWEEN a AND b THEN 1 ELSE 0 END) AS strata
  FROM t3
;"

vmsStrataSql <- SqlRender::render(
  sql,
  writeSchema = writeSchema,
  cohortTable = cohortTable,
  strataTable = strataTable,
  cohortId = targetCohortId,
  cohortId2 = vmsId
) %>%
  SqlRender::translate(targetDialect = executionSettings$connectionDetails$dbms)

DatabaseConnector::executeSql(connection = con, vmsStrataSql, progressBar = FALSE)


### 5.Mood Disorder Strata --------------------

sql <-
  "WITH t1 AS (
  SELECT *
  FROM @writeSchema.@cohortTable
  WHERE cohort_definition_id  = @cohortId
),
t2 AS (
  SELECT
    t1.cohort_definition_id AS cohort_definition_id_target,
    t1.subject_id AS subject_id,
    t1.cohort_start_date AS cohort_start_date_target,
    t1.cohort_end_date AS cohort_end_date_target,
    rhs.cohort_definition_id AS cohort_definition_id_strata,
    rhs.cohort_start_date AS cohort_start_date_strata,
    rhs.cohort_end_date AS cohort_end_date_strata
  FROM t1
  LEFT JOIN(
    SELECT *
    FROM @writeSchema.@cohortTable
    WHERE cohort_definition_id = @cohortId2
  ) rhs
    ON t1.subject_id = rhs.subject_id
),
t3 AS(
  SELECT
  *,
  DATEADD(day, -183, cohort_start_date_target) AS a,
  DATEADD(day, 0, cohort_start_date_target) AS b
  FROM t2
)
INSERT INTO @writeSchema.@strataTable (strata_id, cohort_definition_id, subject_id, strata)
  SELECT
    5 AS strata_id,
    cohort_definition_id_target AS cohort_definition_id,
    subject_id,
    (CASE WHEN cohort_start_date_strata BETWEEN a AND b THEN 1 ELSE 0 END) AS strata
  FROM t3
;"

moodStrataSql <- SqlRender::render(
  sql,
  writeSchema = writeSchema,
  cohortTable = cohortTable,
  strataTable = strataTable,
  cohortId = targetCohortId,
  cohortId2 = moodDisId
) %>%
  SqlRender::translate(targetDialect = executionSettings$connectionDetails$dbms)

DatabaseConnector::executeSql(connection = con, moodStrataSql, progressBar = FALSE)


### 6.Depression Strata --------------------

sql <-
  "WITH t1 AS (
  SELECT *
  FROM @writeSchema.@cohortTable
  WHERE cohort_definition_id  = @cohortId
),
t2 AS (
  SELECT
    t1.cohort_definition_id AS cohort_definition_id_target,
    t1.subject_id AS subject_id,
    t1.cohort_start_date AS cohort_start_date_target,
    t1.cohort_end_date AS cohort_end_date_target,
    rhs.cohort_definition_id AS cohort_definition_id_strata,
    rhs.cohort_start_date AS cohort_start_date_strata,
    rhs.cohort_end_date AS cohort_end_date_strata
  FROM t1
  LEFT JOIN(
    SELECT *
    FROM @writeSchema.@cohortTable
    WHERE cohort_definition_id = @cohortId2
  ) rhs
    ON t1.subject_id = rhs.subject_id
),
t3 AS(
  SELECT
  *,
  DATEADD(day, -183, cohort_start_date_target) AS a,
  DATEADD(day, 0, cohort_start_date_target) AS b
  FROM t2
)
INSERT INTO @writeSchema.@strataTable (strata_id, cohort_definition_id, subject_id, strata)
  SELECT
    6 AS strata_id,
    cohort_definition_id_target AS cohort_definition_id,
    subject_id,
    (CASE WHEN cohort_start_date_strata BETWEEN a AND b THEN 1 ELSE 0 END) AS strata
  FROM t3
;"

depressionStrataSql <- SqlRender::render(
  sql,
  writeSchema = writeSchema,
  cohortTable = cohortTable,
  strataTable = strataTable,
  cohortId = targetCohortId,
  cohortId2 = depressId
) %>%
  SqlRender::translate(targetDialect = executionSettings$connectionDetails$dbms)

DatabaseConnector::executeSql(connection = con, depressionStrataSql, progressBar = FALSE)


### 7.Anxiety Strata --------------------

sql <-
  "WITH t1 AS (
  SELECT *
  FROM @writeSchema.@cohortTable
  WHERE cohort_definition_id  = @cohortId
),
t2 AS (
  SELECT
    t1.cohort_definition_id AS cohort_definition_id_target,
    t1.subject_id AS subject_id,
    t1.cohort_start_date AS cohort_start_date_target,
    t1.cohort_end_date AS cohort_end_date_target,
    rhs.cohort_definition_id AS cohort_definition_id_strata,
    rhs.cohort_start_date AS cohort_start_date_strata,
    rhs.cohort_end_date AS cohort_end_date_strata
  FROM t1
  LEFT JOIN(
    SELECT *
    FROM @writeSchema.@cohortTable
    WHERE cohort_definition_id = @cohortId2
  ) rhs
    ON t1.subject_id = rhs.subject_id
),
t3 AS(
  SELECT
  *,
  DATEADD(day, -183, cohort_start_date_target) AS a,
  DATEADD(day, 0, cohort_start_date_target) AS b
  FROM t2
)
INSERT INTO @writeSchema.@strataTable (strata_id, cohort_definition_id, subject_id, strata)
  SELECT
    7 AS strata_id,
    cohort_definition_id_target AS cohort_definition_id,
    subject_id,
    (CASE WHEN cohort_start_date_strata BETWEEN a AND b THEN 1 ELSE 0 END) AS strata
  FROM t3
;"

anxietyStrataSql <- SqlRender::render(
  sql,
  writeSchema = writeSchema,
  cohortTable = cohortTable,
  strataTable = strataTable,
  cohortId = targetCohortId,
  cohortId2 = anxietyId
) %>%
  SqlRender::translate(targetDialect = executionSettings$connectionDetails$dbms)

DatabaseConnector::executeSql(connection = con, anxietyStrataSql, progressBar = FALSE)



### 8.Bipolar Strata --------------------

sql <-
  "WITH t1 AS (
  SELECT *
  FROM @writeSchema.@cohortTable
  WHERE cohort_definition_id  = @cohortId
),
t2 AS (
  SELECT
    t1.cohort_definition_id AS cohort_definition_id_target,
    t1.subject_id AS subject_id,
    t1.cohort_start_date AS cohort_start_date_target,
    t1.cohort_end_date AS cohort_end_date_target,
    rhs.cohort_definition_id AS cohort_definition_id_strata,
    rhs.cohort_start_date AS cohort_start_date_strata,
    rhs.cohort_end_date AS cohort_end_date_strata
  FROM t1
  LEFT JOIN(
    SELECT *
    FROM @writeSchema.@cohortTable
    WHERE cohort_definition_id = @cohortId2
  ) rhs
    ON t1.subject_id = rhs.subject_id
),
t3 AS(
  SELECT
  *,
  DATEADD(day, -183, cohort_start_date_target) AS a,
  DATEADD(day, 0, cohort_start_date_target) AS b
  FROM t2
)
INSERT INTO @writeSchema.@strataTable (strata_id, cohort_definition_id, subject_id, strata)
  SELECT
    8 AS strata_id,
    cohort_definition_id_target AS cohort_definition_id,
    subject_id,
    (CASE WHEN cohort_start_date_strata BETWEEN a AND b THEN 1 ELSE 0 END) AS strata
  FROM t3
;"

bipolarStrataSql <- SqlRender::render(
  sql,
  writeSchema = writeSchema,
  cohortTable = cohortTable,
  strataTable = strataTable,
  cohortId = targetCohortId,
  cohortId2 = bipolarId
) %>%
  SqlRender::translate(targetDialect = executionSettings$connectionDetails$dbms)

DatabaseConnector::executeSql(connection = con, bipolarStrataSql, progressBar = FALSE)


### 9.iVMS Strata --------------------

sql <-
  "WITH t1 AS (
  SELECT *
  FROM @writeSchema.@cohortTable
  WHERE cohort_definition_id  = @cohortId
),
t2 AS (
  SELECT
    t1.cohort_definition_id AS cohort_definition_id_target,
    t1.subject_id AS subject_id,
    t1.cohort_start_date AS cohort_start_date_target,
    t1.cohort_end_date AS cohort_end_date_target,
    rhs.cohort_definition_id AS cohort_definition_id_strata,
    rhs.cohort_start_date AS cohort_start_date_strata,
    rhs.cohort_end_date AS cohort_end_date_strata
  FROM t1
  LEFT JOIN(
    SELECT *
    FROM @writeSchema.@cohortTable
    WHERE cohort_definition_id = @cohortId2
  ) rhs
    ON t1.subject_id = rhs.subject_id
),
t3 AS(
  SELECT
  *,
  DATEADD(day, 0, cohort_start_date_target) AS a,
  DATEADD(day, 0, cohort_start_date_target) AS b
  FROM t2
)
INSERT INTO @writeSchema.@strataTable (strata_id, cohort_definition_id, subject_id, strata)
  SELECT
    9 AS strata_id,
    cohort_definition_id_target AS cohort_definition_id,
    subject_id,
    (CASE WHEN cohort_start_date_strata BETWEEN a AND b THEN 1 ELSE 0 END) AS strata
  FROM t3
;"

iVmsStrataSql <- SqlRender::render(
  sql,
  writeSchema = writeSchema,
  cohortTable = cohortTable,
  strataTable = strataTable,
  cohortId = targetCohortId,
  cohortId2 = ivmsId
) %>%
  SqlRender::translate(targetDialect = executionSettings$connectionDetails$dbms)

DatabaseConnector::executeSql(connection = con, iVmsStrataSql, progressBar = FALSE)


### 10.Organic Sleep Strata --------------------

sql <-
  "WITH t1 AS (
  SELECT *
  FROM @writeSchema.@cohortTable
  WHERE cohort_definition_id  = @cohortId
),
t2 AS (
  SELECT
    t1.cohort_definition_id AS cohort_definition_id_target,
    t1.subject_id AS subject_id,
    t1.cohort_start_date AS cohort_start_date_target,
    t1.cohort_end_date AS cohort_end_date_target,
    rhs.cohort_definition_id AS cohort_definition_id_strata,
    rhs.cohort_start_date AS cohort_start_date_strata,
    rhs.cohort_end_date AS cohort_end_date_strata
  FROM t1
  LEFT JOIN(
    SELECT *
    FROM @writeSchema.@cohortTable
    WHERE cohort_definition_id = @cohortId2
  ) rhs
    ON t1.subject_id = rhs.subject_id
),
t3 AS(
  SELECT
  *,
  DATEADD(day, -183, cohort_start_date_target) AS a,
  DATEADD(day, 0, cohort_start_date_target) AS b
  FROM t2
)
INSERT INTO @writeSchema.@strataTable (strata_id, cohort_definition_id, subject_id, strata)
  SELECT
    10 AS strata_id,
    cohort_definition_id_target AS cohort_definition_id,
    subject_id,
    (CASE WHEN cohort_start_date_strata BETWEEN a AND b THEN 1 ELSE 0 END) AS strata
  FROM t3
;"

orgSleepStrataSql <- SqlRender::render(
  sql,
  writeSchema = writeSchema,
  cohortTable = cohortTable,
  strataTable = strataTable,
  cohortId = targetCohortId,
  cohortId2 = orgSleepId
) %>%
  SqlRender::translate(targetDialect = executionSettings$connectionDetails$dbms)

DatabaseConnector::executeSql(connection = con, orgSleepStrataSql, progressBar = FALSE)

}


## Summarize Strata and save --------------------
strataSum <- dplyr::tbl(con, dbplyr::in_schema(writeSchema, strataTable)) %>%
  dplyr::group_by(strata_id, strata, cohort_definition_id) %>%
  dplyr::count() %>%
  dplyr::arrange(strata_id, strata, cohort_definition_id) %>%
  dplyr::collect() %>%
  dplyr::inner_join(strataManifest, by = c("strata_id", "strata")) %>%
  dplyr::inner_join(cohortsToCreate, by = c("cohort_definition_id" = "id")) %>%
  dplyr::select(cohort_definition_id, name, strata_id, strata, strata_name, strata_value, strataFullName, stratumFullName, stratumFullName, n, subjects) %>%
  dplyr::rename(cohort_name = name, strata_count = n, cohort_count = subjects) %>%
  dplyr::mutate(pctStrata = strata_count/cohort_count,
                database = executionSettings$databaseId)


fs::dir_create(outputFolder)
save_path <- fs::path(outputFolder, "strataManifest", ext = "csv")
readr::write_csv(strataSum, file = save_path)

