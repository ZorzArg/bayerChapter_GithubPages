
is_credential_set <- function(configBlock, credential) {

  cred <- paste(configBlock, credential, sep = "_")

  tmp <- purrr::safely(keyring::key_get)(cred)

  if (is.null(tmp$error)) {
    TRUE
  } else{
    FALSE
  }
}



getExecutionSettings <- function(configBlock, file = here::here("config.yml")) {

  executionSettings <- structure(list(
    'connectionDetails' = config::get("connectionDetails", config = configBlock, file = file),
    'cdmDatabase' = config::get("cdmDatabase", config = configBlock, file = file),
    'cdmSchema' = config::get("cdmSchema", config = configBlock, file = file),
    'vocabSchema' = config::get("vocabSchema", config = configBlock, file = file),
    'writeDatabase' = config::get("writeDatabase", config = configBlock, file = file),
    'writeSchema' = config::get("writeSchema", config = configBlock, file = file),
    'cohortTable' = config::get("analysisCohorts", file = file),
    'databaseId' = config::get("databaseName", config = configBlock, file = file),
    'userRole'= config::get("role", config = configBlock, file = file)
  ), class = "executionSettings")


  return(executionSettings)
}


startSnowflakeSession <- function(con, executionSettings) {
  sql <- "
  ALTER SESSION SET JDBC_QUERY_RESULT_FORMAT='JSON';
    USE ROLE @user_role;
    USE SECONDARY ROLES ALL;
    USE DATABASE @write_database;
    USE SCHEMA @write_schema;
  "
  sessionSql <- SqlRender::render(
    sql = sql,
    user_role = executionSettings$userRole,
    write_database = executionSettings$writeDatabase,
    write_schema = executionSettings$writeSchema
  ) %>%
    SqlRender::translate(targetDialect = executionSettings$connectionDetails$dbms)
  DatabaseConnector::executeSql(connection = con, sql = sessionSql)
  cli::cat_line("Setting up Snowflake session")
  invisible(sessionSql)
}


initializeCohortTables <- function(executionSettings) {

  if (executionSettings$connectionDetails$dbms == "snowflake") {
    writeSchema <- paste(executionSettings$writeDatabase, executionSettings$writeSchema, sep = ".")
  } else {
    writeSchema <- executionSettings$writeSchema
  }

  name <- executionSettings$cohortTable
  cohortTableNames <- list(cohortTable = paste0(name,"_",executionSettings$databaseId),
                           cohortInclusionTable = paste0(name, "_inclusion_",executionSettings$databaseId),
                           cohortInclusionResultTable = paste0(name, "_inclusion_result_",executionSettings$databaseId),
                           cohortInclusionStatsTable = paste0(name, "_inclusion_stats_",executionSettings$databaseId),
                           cohortSummaryStatsTable = paste0(name, "_summary_stats_",executionSettings$databaseId),
                           cohortCensorStatsTable = paste0(name, "_censor_stats_",executionSettings$databaseId))

  con <- DatabaseConnector::connect(executionSettings$connectionDetails)
  startSnowflakeSession(con = con, executionSettings = executionSettings)

  CohortGenerator::createCohortTables(connection = con,
                                      cohortDatabaseSchema = writeSchema,
                                      cohortTableNames = cohortTableNames,
                                      incremental = TRUE)
  return(cohortTableNames)

}
