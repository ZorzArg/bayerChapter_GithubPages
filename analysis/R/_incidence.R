# Incidence Analysis
irDesign <- function(row, ageBreaks) {

  ### Define target
  t1 <- CohortIncidence::createCohortRef(
    id = row$targetId,
    name = row$targetName
  )

  ### Define outcome
  o1 <- CohortIncidence::createOutcomeDef(
    id = row$outcomeId,
    name = row$outcomeName,
    cohortId = row$outcomeId,
    cleanWindow = row$cleanWindow
  )

  ### Define tar
  tar1 <- CohortIncidence::createTimeAtRiskDef(
    id = 1,
    startWith = row$startWith,
    startOffset = row$startOffset,
    endWith = row$endWith,
    endOffset = row$endOffset
  )

  ### Set analysis
  analysis1 <- CohortIncidence::createIncidenceAnalysis(
    targets = c(t1$id),
    outcomes = c(o1$id),
    tars = c(tar1$id)
  )

  ### Define subgroups
  subgroup1 <- CohortIncidence::createCohortSubgroup(
    id = 1,
    name = row$subgroupName,
    cohortRef = CohortIncidence::createCohortRef(
      id = row$subgroupId, name = row$subgroupName
    )
  )

  strataSettings <- CohortIncidence::createStrataSettings(
          byAge = TRUE,
          ageBreaks = ageBreaks,
          byGender = FALSE,
          byYear = TRUE
        )

  ### Initialize design
  ir_design <- CohortIncidence::createIncidenceDesign(
    targetDefs = list(t1),
    outcomeDefs = list(o1),
    tars = list(tar1),
    subgroups = list(subgroup1),
    analysisList = list(analysis1),
    strataSettings = strataSettings
  )

  return(ir_design)

}


generateIncidence <- function(executionSettings,
                              outputFolder,
                              incidenceDesign,
                              refId) {

  ## Set variables
  if (executionSettings$connectionDetails$dbms == "snowflake") {

    writeSchema <- paste(executionSettings$writeDatabase, executionSettings$writeSchema, sep = ".")
    cdmSchema <- paste(executionSettings$cdmDatabase, executionSettings$cdmSchema, sep = ".")
    vocabularySchema <- paste(executionSettings$writeDatabase, executionSettings$vocabSchema, sep = ".")

  } else {

    writeSchema <- executionSettings$writeSchema
    cdmSchema <- executionSettings$cdmSchema
    vocabularySchema <- executionSettings$vocabSchema
  }

  cohortTable <- paste(executionSettings$cohortTable, executionSettings$databaseId, sep = "_")


  buildOptions <- CohortIncidence::buildOptions(
    cohortTable = cohortTable,
    cdmDatabaseSchema = cdmSchema,
    sourceName = executionSettings$databaseId,
    resultsDatabaseSchema = writeSchema,
    vocabularySchema = vocabularySchema,
    useTempTables = FALSE,
    refId = refId)

  #get elements from environment in R6
  targetId <- incidenceDesign$.__enclos_env__$private$.targetDefs[[1]]$id
  targetName <- incidenceDesign$.__enclos_env__$private$.targetDefs[[1]]$name
  outcomeId <- incidenceDesign$.__enclos_env__$private$.outcomeDefs[[1]]$id
  outcomeName <- incidenceDesign$.__enclos_env__$private$.outcomeDefs[[1]]$name
  startOffset <- incidenceDesign$.__enclos_env__$private$.timeAtRiskDefs[[1]]$startOffset
  endOffset <- incidenceDesign$.__enclos_env__$private$.timeAtRiskDefs[[1]]$endOffset

  cli::cat_rule()
  cli::cat_line("Executing Incidence Analysis Id: ", refId)
  cli::cat_line("\tTarget ID ", targetId, " : ", targetName)
  cli::cat_line("\tOutcome ID ", outcomeId, " : ", outcomeName)
  cli::cat_line("\tTAR between: ", startOffset, " and ", endOffset, " days")


  # executeResults <- CohortIncidence::executeAnalysis(
  #   connectionDetails = executionSettings$connectionDetails,
  #   incidenceDesign = incidenceDesign,
  #   buildOptions = buildOptions)

  ### Executing results for incidence analysis
  executeResults <- executeAnalysis(
    executionSettings = executionSettings,
    connectionDetails = executionSettings$connectionDetails,
    incidenceDesign = incidenceDesign,
    buildOptions = buildOptions)

  return(executeResults)

}


## executeAnalysis (from CohortGenerator package) ----------------------------
## NOTE: Added startSnowflakeSession function

executeAnalysis <-function (connectionDetails = NULL, connection = NULL, incidenceDesign, executionSettings,
                            buildOptions, sourceName = "default")

{

  irDesign <- incidenceDesign
  if (checkmate::testClass(incidenceDesign, "IncidenceDesign")) {
    irDesign <- as.character(irDesign$asJSON())
  }
  else if (checkmate::testCharacter(irDesign)) {
    invisible(IncidenceDesign$new(irDesign))
  }
  else {
    stop("Error in executAnalysis(): incidenceDesign must be either R6 IncidenceDesign or JSON character string.")
  }
  if (is.null(connectionDetails) && is.null(connection)) {
    stop("Need to provide either connectionDetails or connection")
  }
  if (!is.null(connectionDetails) && !is.null(connection)) {
    stop("Need to provide either connectionDetails or connection, not both")
  }
  if (!is.null(connectionDetails)) {
    conn <- DatabaseConnector::connect(connectionDetails)
    startSnowflakeSession(con = conn, executionSettings = executionSettings)
    on.exit(DatabaseConnector::disconnect(conn))
  }
  else {
    conn <- connection
  }
  if (rJava::is.jnull(buildOptions$targetCohortTable) || rJava::is.jnull(buildOptions$cdmSchema)) {
    stop("buildOptions$targetCohortTable or buildOptions$cdmSchema is missing.")
  }
  buildOptions$useTempTables = T
  if (rJava::is.jnull(buildOptions$sourceName)) {
    buildOptions$sourceName = sourceName
  }
  targetDialect <- attr(conn, "dbms")
  tempDDL <- SqlRender::translate(CohortIncidence::getResultsDdl(useTempTables = T),
                                  targetDialect = targetDialect)
  DatabaseConnector::executeSql(conn, tempDDL)
  analysisSql <- CohortIncidence::buildQuery(incidenceDesign = irDesign,
                                             buildOptions = buildOptions)
  analysisSql <- SqlRender::translate(analysisSql, targetDialect = targetDialect)
  analysisSqlQuries <- SqlRender::splitSql(analysisSql)
  DatabaseConnector::executeSql(conn, analysisSql)
  results <- DatabaseConnector::querySql(conn, SqlRender::translate("select * from #incidence_summary",
                                                                    targetDialect = targetDialect))
  cleanupSql <- SqlRender::translate(CohortIncidence::getCleanupSql(useTempTables = T),
                                     targetDialect)

  DatabaseConnector::executeSql(conn, cleanupSql)

  return(invisible(results))
}
