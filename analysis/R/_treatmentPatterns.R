
# Treatment Patterns --------------------------


prepSankey <- function(th, minNumPatterns = 30L) {

  treatment_pathways <- th %>%
    tidyr::pivot_wider(id_cols = person_id,
                       names_from = event_seq,
                       names_prefix = "event_cohort_name",
                       values_from = event_cohort_name) %>%
    dplyr::count(dplyr::across(tidyselect::starts_with("event_cohort_name"))) %>%
    dplyr::mutate(End = "end", .before = "n") %>%
    dplyr::filter(n >= minNumPatterns)


  links <- treatment_pathways %>%
    #dplyr::filter(n >= 500) %>%   # Filter pathways to more than 500 to make sankey plot more readable
    dplyr::mutate(row = dplyr::row_number()) %>%
    tidyr::pivot_longer(cols = c(-row, -n),
                        names_to = 'column', values_to = 'source') %>%
    dplyr::mutate(column = match(column, names(treatment_pathways))) %>%
    tidyr::drop_na(source) %>%
    dplyr::mutate(source = paste0(source, '__', column)) %>%
    dplyr::group_by(row) %>%
    dplyr::mutate(target = dplyr::lead(source, order_by = column)) %>%
    tidyr::drop_na(target, source) %>%
    dplyr::group_by(source, target) %>%
    dplyr::summarise(value = sum(n), .groups = 'drop') %>%
    dplyr::arrange(desc(value))

  nodes <- data.frame(name = unique(c(links$source, links$target)))
  nodes <- data.table::data.table(nodes)
  links <- data.table::data.table(links)
  links$source <- match(links$source, nodes$name) - 1
  links$target <- match(links$target, nodes$name) - 1
  nodes$name <- sub('__[0-9]+$', '', nodes$name)
  links$type <- sub(' .*', '',
                    as.data.frame(nodes)[links$source + 1, 'name'])
  data.table::setkey(links, type)
  data.table::setorder(links, cols = - "value")

  res <- list(
    'treatmentPatterns' = treatment_pathways,
    'links' = links,
    'nodes' = nodes
  )

  return(res)

}


treatmentPatterns <- function(executionSettings,
                              treatmentHistory,
                              eventType,
                              eraCollapseSize,
                              targetCohorts,
                              strataNames,
                              minNumPatterns = 30L,
                              outputFolder) {

  ## Read treatment history data frame
  treatmentHistory <- arrow::read_parquet(file = here::here("output", "06_treatmentHistory",
                                                            executionSettings$databaseId, targetCohorts$name,
                                                            paste0("th_", eventType, "_", eraCollapseSize, ".parquet")))


  ## Create directory
  outputFolder <- fs::path(outputFolder, targetCohorts$name)
  fs::dir_create(outputFolder)


  ## Set variables
  if (executionSettings$connectionDetails$dbms == "snowflake") {
    writeSchema <- paste(executionSettings$writeDatabase, executionSettings$writeSchema, sep = ".")
    cdmSchema <- paste(executionSettings$cdmDatabase, executionSettings$cdmSchema, sep = ".")
  } else {
    writeSchema <- executionSettings$writeSchema
    cdmSchema <- executionSettings$cdmSchema
  }

  cohortTable <- paste(executionSettings$cohortTable, executionSettings$databaseId, sep = "_")
  strataTable <- paste(executionSettings$cohortTable, "strata", configBlock, sep = "_")
  targetCohortId <- as.double(targetCohorts$id)


  ## Get strata table
  sql <- "SELECT * FROM @write_schema.@strata_table
          WHERE cohort_definition_id = @target_cohort_id;"  %>%
    SqlRender::render(
      write_schema = writeSchema,
      strata_table = strataTable,
      target_cohort_id = targetCohortId
    ) %>%
    SqlRender::translate(executionSettings$connectionDetails$dbms)

  strataTbl <-  DatabaseConnector::querySql(connection = con, sql = sql)

  colnames(strataTbl) <- tolower(colnames(strataTbl))


  ## Create treatment patterns

  strataNames <- readr::read_csv(here::here("output", "02_buildStrata", executionSettings$databaseId, "strataManifest.csv"),
                                 show_col_types = FALSE) %>%
    dplyr::filter(cohort_definition_id == targetCohortId) %>%
    dplyr::mutate(nm = paste(strata_name, strata_value, sep = "_")) %>%
    dplyr::pull(nm) %>%
    unique()


  patterns <- treatmentHistory %>%
    dplyr::left_join(strataTbl, by = c("person_id" = "subject_id"), multiple = "all") %>%
    dplyr::group_by(strata_id, strata) %>%
    dplyr::group_split() %>%
    purrr::map(~prepSankey(.x, minNumPatterns = minNumPatterns)) %>%
    purrr::set_names(nm = strataNames)


  ## Save output - rds
  save_path <- fs::path(outputFolder, paste0("treatmentPatterns_", eventType, "_", eraCollapseSize, ".rds"))
  readr::write_rds(patterns, file = save_path)
  cli::cat_line("\nTreatment Patterns run at: ", Sys.time())
  cli::cat_line("\nSaved to: ", save_path)
  cli::cat_line("Connection Closed\n\n")

  return(patterns)

}

