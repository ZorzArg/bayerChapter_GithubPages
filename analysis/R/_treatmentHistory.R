# Treatment Patterns code (modification of TreatmentPatterns)


# Define treatment history---------------------

defineTreatmentHistory <- function(cohorts, ...) {


  ths <- structure(
    list(
      cohorts = cohorts,
      minEraDuration = 0L,
      eraCollapseSize = 30L,
      combinationWindow = 30L,
      minPostCombinationDuration =30L,
      filterTreatments = "Changes",
      periodPriorToIndex = 0L,
      includeTreatments = "startDate",
      maxPathLength = 5L,
      minCellCount = 5L,
      minCellMethod = "Remove",
      groupCombinations = 10L,
      addNoPaths = FALSE),
    class = "treatmentHistorySettings")


  ths <- purrr::list_modify(ths, ...)
  return(ths)

}
# Treatment History ---------------------------

collect_cohorts <- function(executionsettings,
                            targetId,
                            eventId) {


  ## Set variables
  if (executionSettings$connectionDetails$dbms == "snowflake") {
    writeSchema <- paste(executionSettings$writeDatabase, executionSettings$writeSchema, sep = ".")
    cdmSchema <- paste(executionSettings$cdmDatabase, executionSettings$cdmSchema, sep = ".")
  } else {
    writeSchema <- executionSettings$writeSchema
    cdmSchema <- executionSettings$cdmSchema
  }

  currentCohorts <- "current_cohorts"
  cohortTable <- paste0(executionSettings$cohortTable,"_",executionSettings$databaseId)
  targetEventIds <- c(targetId, eventId)


  ##
  sql <- "
    CREATE TEMPORARY TABLE @write_schema.@current_cohorts (
      cohort_definition_id_event INT,
      cohort_start_date_event DATE,
      cohort_end_date_event DATE,
      cohort_definition_id_target INT,
      cohort_start_date_target DATE,
      cohort_end_date_target DATE,
      subject_id BIGINT
    );

     INSERT INTO @write_schema.@current_cohorts
    SELECT
     a.cohort_definition_id as cohort_definition_id_event,
     a.cohort_start_date as cohort_start_date_event,
     a.cohort_end_date as cohort_end_date_event,
     b.cohort_definition_id as cohort_definition_id_target,
     b.cohort_start_date as cohort_start_date_target,
     b.cohort_end_date as cohort_end_date_target,
     b.subject_id as subject_id
    FROM @write_schema.@cohort_table a
    INNER JOIN @write_schema.@cohort_table b
     ON a.subject_id = b.subject_id
    WHERE a.COHORT_DEFINITION_ID IN (@target_event_ids)
     AND b.COHORT_DEFINITION_ID = @target_id;" %>%
    SqlRender::render(
      write_schema = writeSchema,
      cohort_table = cohortTable,
      current_cohorts = currentCohorts,
      target_event_ids = targetEventIds,
      target_id  = targetId
    ) %>%
    SqlRender::translate(executionSettings$connectionDetails$dbms)

    DatabaseConnector::executeSql(connection = con, sql = sql)


  ##
  sql <- "SELECT * FROM @write_schema.@current_cohorts;"  %>%
    SqlRender::render(
      write_schema = writeSchema,
      current_cohorts = currentCohorts
    ) %>%
    SqlRender::translate(executionSettings$connectionDetails$dbms)

  eventTarget <-  DatabaseConnector::querySql(connection = con, sql = sql)

  colnames(eventTarget) <- tolower(colnames(eventTarget))

  ##
  eventTarget <- eventTarget %>%
      dplyr::rename(cohort_id = cohort_definition_id_event,
                    person_id = subject_id,
                    start_date = cohort_start_date_event,
                    end_date = cohort_end_date_event) %>%
      dplyr::select(cohort_id, person_id, start_date, end_date) %>%
      dplyr::mutate(cohort_id = as.double(cohort_id),
                    person_id = as.double(person_id)) %>%
      data.table::as.data.table()


 ##
 sql <- "DROP TABLE @write_schema.@current_cohorts;" %>%
    SqlRender::render(
      write_schema = writeSchema,
      current_cohorts = currentCohorts
    ) %>%
    SqlRender::translate(executionSettings$connectionDetails$dbms)

  DatabaseConnector::executeSql(connection = con, sql = sql)

  return(eventTarget)

}



treatmentHistory <- function(executionSettings,
                             treatmentHistorySettings,
                             outputFolder) {


  ## Set Variables
  targetCohortId <- treatmentHistorySettings$cohorts %>%
    dplyr::filter(type == "studyPop") %>%
    dplyr::pull(cohort_id) %>%
    as.integer()

  eventCohortIds <- treatmentHistorySettings$cohorts %>%
    dplyr::filter(type == "event") %>%
    dplyr::pull(cohort_id) %>%
    as.integer()

  targetCohortName <- treatmentHistorySettings$cohorts %>%
    dplyr::filter(type == "studyPop") %>%
    dplyr::pull(cohort_name)

  # eventCohortNames <- treatmentHistorySettings$cohorts %>%
  #   dplyr::filter(type == "event") %>%
  #   dplyr::pull(cohort_name)

  eventCohortNames <- treatmentHistorySettings$cohorts %>%
    dplyr::filter(type == "event") %>%
    dplyr::mutate(eventFullName = dplyr::case_when(
      cohort_name == "amitriptyline" ~ "amit",
      cohort_name == "benzodiazepines" ~ "benzo",
      cohort_name == "citalopram" ~ "cita",
      cohort_name == "clonidine" ~ "clon",
      cohort_name == "desvenlafaxine" ~ "desv",
      cohort_name == "escitalopram" ~ "esci",
      cohort_name == "estrogens" ~ "estr",
      cohort_name == "estrogensCombo" ~ "estrCombo",
      cohort_name == "fluoxetine" ~ "fluo",
      cohort_name == "gabapentin" ~ "gaba",
      cohort_name == "paroxetine" ~ "paro",
      cohort_name == "pregabalin" ~ "preg",
      cohort_name == "progestogens" ~ "prog",
      cohort_name == "progestogensEstrogens" ~ "progEstr",
      cohort_name == "raloxifene" ~ "ralo",
      cohort_name == "sertraline" ~ "sertr",
      cohort_name == "venlafaxine" ~ "venl",
      cohort_name == "anticonvulsants_lvl1" ~ "AC",
      cohort_name == "antidepressants_lvl1" ~ "AD",
      cohort_name == "antihypertensives_lvl1" ~ "AH",
      cohort_name == "benzodiazepines_lvl1" ~ "BD",
      cohort_name == "hormoneTherapy_lvl1" ~ "HT",
      cohort_name == "anticonvulsants_lvl3" ~ "AC",
      cohort_name == "antidepressants_lvl3" ~ "AD",
      cohort_name == "antihypertensives_lvl3" ~ "AH",
      cohort_name == "benzodiazepines_lvl3" ~ "BD",
      cohort_name == "hormoneTherapy_lvl3" ~ "HT"
     )
    ) %>%
    dplyr::pull(eventFullName)


  ## Get target and event cohorts
  current_cohorts <- collect_cohorts(executionSettings,
                                     targetId = targetCohortId,
                                     eventId = eventCohortIds)


  ## Run treatment history
  tik <- Sys.time()

  res <- doCreateTreatmentHistory(current_cohorts,
                                  targetCohortId,
                                  eventCohortIds,
                                  treatmentHistorySettings$periodPriorToIndex,
                                  treatmentHistorySettings$includeTreatments) %>%
    doEraDuration(treatmentHistorySettings$minEraDuration) %>%
    doEraCollapse(treatmentHistorySettings$eraCollapseSize) %>%
    doCombinationWindow(treatmentHistorySettings$combinationWindow,
                        treatmentHistorySettings$minPostCombinationDuration) %>%
    doFilterTreatments(treatmentHistorySettings$filterTreatments) %>%
    postProcess(eventCohortIds,
                eventCohortNames,
                treatmentHistorySettings$maxPathLength) %>%
    dplyr::mutate(database = executionSettings$databaseId,
                  cohort = targetCohortId,
                  era_size = treatmentHistorySettings$eraCollapseSize,
                  event_type = unique(treatmentHistorySettings$cohorts$event_type)
                  )

  tok <- Sys.time()
  tdif <- tok - tik
  tok_format <- paste(scales::label_number(0.01)(as.double(tdif)), attr(tdif, "units"))
  cli::cat_line("\nTreatment History built at: ", tok)
  cli::cat_line("\nTreatment History build took: ", tok_format)


  ## Create directory
  outputFolder <- fs::path(outputFolder, targetCohortName)
  fs::dir_create(outputFolder)


  ## Save output - Parquet
  save_path <- fs::path(outputFolder, paste0("th_", unique(treatmentHistorySettings$cohorts$event_type), "_", treatmentHistorySettings$eraCollapseSize), ext = "parquet")
  arrow::write_parquet(x = res, sink = save_path)
  cli::cat_line("\nSaved to: ", save_path)
  cli::cat_line("Connection Closed\n\n")

  return(res)

}



# Internals --------------------

# Treatment History Functions
# Functions with modifications of TreatmentPatterns
#Functions from TreatmentPatterns ConstructPathways.R

doCreateTreatmentHistory <- function(current_cohorts, targetCohortId, eventCohortIds, periodPriorToIndex, includeTreatments) {

  # Add index year column based on start date target cohort
  targetCohort <- current_cohorts[current_cohorts$cohort_id %in% targetCohortId,,]
  targetCohort$index_year <- as.double(format(targetCohort$start_date, "%Y"))

  # Select event cohorts for target cohort and merge with start/end date and index year
  eventCohorts <- current_cohorts[current_cohorts$cohort_id %in% eventCohortIds,,]
  current_cohorts <- data.table::merge.data.table(x = eventCohorts,
                                                  y = targetCohort,
                                                  by = c("person_id"),
                                                  all.x = TRUE,
                                                  all.y = TRUE,
                                                  allow.cartesian = TRUE)

  # Only keep event cohorts starting (startDate) or ending (endDate) after target cohort start date
  if (includeTreatments == "startDate") {
    current_cohorts <- current_cohorts[current_cohorts$start_date.y - as.difftime(periodPriorToIndex, unit="days") <= current_cohorts$start_date.x & current_cohorts$start_date.x < current_cohorts$end_date.y,]
  } else if (includeTreatments == "endDate") {
    current_cohorts <- current_cohorts[current_cohorts$start_date.y - as.difftime(periodPriorToIndex, unit="days") <= current_cohorts$end_date.x & current_cohorts$start_date.x < current_cohorts$end_date.y,]
    current_cohorts$start_date.x <- pmax(current_cohorts$start_date.y - as.difftime(periodPriorToIndex, unit="days"), current_cohorts$start_date.x)
  } else {
    warning("includeTreatments input incorrect, return all event cohorts ('includeTreatments')")
    current_cohorts <- current_cohorts[current_cohorts$start_date.y - as.difftime(periodPriorToIndex, unit="days") <= current_cohorts$start_date.x & current_cohorts$start_date.x < current_cohorts$end_date.y,]
  }

  # Remove unnecessary columns
  current_cohorts <- current_cohorts[,c("person_id", "index_year", "cohort_id.x", "start_date.x", "end_date.x")]
  colnames(current_cohorts) <- c("person_id", "index_year", "event_cohort_id", "event_start_date", "event_end_date")

  # Calculate duration and gap same
  current_cohorts[,duration_era:=difftime(event_end_date, event_start_date, units = "days")]

  current_cohorts <- current_cohorts[order(event_start_date, event_end_date),]
  current_cohorts[,lag_variable:=data.table::shift(event_end_date, type = "lag"), by=c("person_id", "event_cohort_id")]
  current_cohorts[,gap_same:=difftime(event_start_date, lag_variable, units = "days"),]
  current_cohorts$lag_variable <- NULL

  return(current_cohorts)
}


doEraDuration <- function(treatment_history, minEraDuration) {
  treatment_history <- treatment_history[duration_era >= minEraDuration,]
  cli::cat_line(paste0("After minEraDuration: ", nrow(treatment_history)))

  return(treatment_history)
}


doCombinationWindow <- function(treatment_history, combinationWindow, minPostCombinationDuration) {

  time1 <- Sys.time()

  treatment_history$event_cohort_id <- as.character(treatment_history$event_cohort_id)

  # Find which rows contain some overlap
  treatment_history <- selectRowsCombinationWindow(treatment_history)

  # While rows that need modification exist:
  iterations <- 1
  while(sum(treatment_history$SELECTED_ROWS)!=0) {

    # Which have gap previous shorter than combination window OR min(current duration era, previous duration era) -> add column switch
    treatment_history[SELECTED_ROWS == 1 & (-GAP_PREVIOUS < combinationWindow  & !(-GAP_PREVIOUS == duration_era | -GAP_PREVIOUS == data.table::shift(duration_era, type = "lag"))), switch:=1]

    # For rows selected not in column switch -> if treatment_history[r - 1, event_end_date] <= treatment_history[r, event_end_date] -> add column combination first received, first stopped
    treatment_history[SELECTED_ROWS == 1 & is.na(switch) & data.table::shift(event_end_date, type = "lag") <= event_end_date, combination_FRFS:=1]

    # For rows selected not in column switch -> if treatment_history[r - 1, event_end_date] > treatment_history[r, event_end_date] -> add column combination last received, first stopped
    treatment_history[SELECTED_ROWS == 1 & is.na(switch) & data.table::shift(event_end_date, type = "lag") > event_end_date, combination_LRFS:=1]

    cli::cat_line(paste0("Iteration ", iterations, " modifying  ", sum(treatment_history$SELECTED_ROWS), " selected rows out of ", nrow(treatment_history), ": ", sum(!is.na(treatment_history$switch)) , " switches, ", sum(!is.na(treatment_history$combination_FRFS)), " combinations FRFS and ", sum(!is.na(treatment_history$combination_LRFS)), " combinations LRFS"))
    if (sum(!is.na(treatment_history$switch)) + sum(!is.na(treatment_history$combination_FRFS)) +  sum(!is.na(treatment_history$combination_LRFS)) != sum(treatment_history$SELECTED_ROWS)) {
      warning(paste0(sum(treatment_history$SELECTED_ROWS), ' does not equal total sum ', sum(!is.na(treatment_history$switch)) +  sum(!is.na(treatment_history$combination_FRFS)) +  sum(!is.na(treatment_history$combination_LRFS))))
    }

    # Do transformations for each of the three newly added columns
    # Construct helpers
    treatment_history[,event_start_date_next:=data.table::shift(event_start_date, type = "lead"),by=person_id]
    treatment_history[,event_end_date_previous:=data.table::shift(event_end_date, type = "lag"),by=person_id]
    treatment_history[,event_end_date_next:=data.table::shift(event_end_date, type = "lead"),by=person_id]
    treatment_history[,event_cohort_id_previous:=data.table::shift(event_cohort_id, type = "lag"),by=person_id]

    # Case: switch
    # Change end treatment_history of previous row -> no minPostCombinationDuration
    treatment_history[data.table::shift(switch, type = "lead")==1,event_end_date:=event_start_date_next]

    # Case: combination_FRFS
    # Add a new row with start date (r) and end date (r-1) as combination (copy current row + change end date + update concept id) -> no minPostCombinationDuration
    add_rows_FRFS <- treatment_history[combination_FRFS==1,]
    add_rows_FRFS[,event_end_date:=event_end_date_previous]
    add_rows_FRFS[,event_cohort_id:=paste0(event_cohort_id, "+", event_cohort_id_previous)]

    # Change end date of previous row -> check minPostCombinationDuration
    treatment_history[data.table::shift(combination_FRFS, type = "lead")==1,c("event_end_date","check_duration"):=list(event_start_date_next, 1)]

    # Change start date of current row -> check minPostCombinationDuration
    treatment_history[combination_FRFS==1,c("event_start_date", "check_duration"):=list(event_end_date_previous,1)]

    # Case: combination_LRFS
    # Change current row to combination -> no minPostCombinationDuration
    treatment_history[combination_LRFS==1,event_cohort_id:=paste0(event_cohort_id, "+", event_cohort_id_previous)]

    # Add a new row with end date (r) and end date (r-1) to split drug era (copy previous row + change end date) -> check minPostCombinationDuration
    add_rows_LRFS <- treatment_history[data.table::shift(combination_LRFS, type = "lead")==1,]
    add_rows_LRFS[,c("event_start_date", "check_duration"):=list(event_end_date_next,1)]

    # Change end date of previous row -> check minPostCombinationDuration
    treatment_history[data.table::shift(combination_LRFS, type = "lead")==1,c("event_end_date", "check_duration"):=list(event_start_date_next,1)]

    # Combine all rows and remove helper columns
    treatment_history <- rbind(treatment_history, add_rows_FRFS, fill=TRUE)
    treatment_history <- rbind(treatment_history, add_rows_LRFS)

    # Re-calculate duration_era
    treatment_history[,duration_era:=difftime(event_end_date, event_start_date, units = "days")]

    # Check duration drug eras before/after generated combination treatments
    treatment_history <- doStepDuration(treatment_history, minPostCombinationDuration)

    # Preparations for next iteration
    treatment_history <- treatment_history[,c("person_id", "index_year", "event_cohort_id", "event_start_date", "event_end_date", "duration_era")]
    treatment_history <- selectRowsCombinationWindow(treatment_history)
    iterations <- iterations + 1

    gc()
  }

  cli::cat_line(paste0("After combinationWindow: ", nrow(treatment_history)))

  treatment_history[,GAP_PREVIOUS:=NULL]
  treatment_history[,SELECTED_ROWS:=NULL]

  time2 <- Sys.time()
  cli::cat_line(paste0("Time needed to execute combination window ", difftime(time2, time1, units = "mins")))

  return(treatment_history)
}


selectRowsCombinationWindow <- function(treatment_history) {
  # Order treatment_history by person_id, event_start_date, event_end_date
  treatment_history <- treatment_history[order(person_id, event_start_date, event_end_date),]

  # Calculate gap with previous treatment
  treatment_history[,GAP_PREVIOUS:=difftime(event_start_date, data.table::shift(event_end_date, type = "lag"), units = "days"), by = person_id]
  treatment_history$GAP_PREVIOUS <- as.integer(treatment_history$GAP_PREVIOUS)

  # Find all rows with gap_previous < 0
  treatment_history[treatment_history$GAP_PREVIOUS < 0, ALL_ROWS:=which(treatment_history$GAP_PREVIOUS < 0)]

  # Select one row per iteration for each person
  rows <- treatment_history[!is.na(ALL_ROWS),head(.SD,1), by=person_id]$ALL_ROWS

  treatment_history[rows,SELECTED_ROWS:=1]
  treatment_history[!rows,SELECTED_ROWS:=0]
  treatment_history[,ALL_ROWS:=NULL]

  return(treatment_history)
}
doStepDuration <- function(treatment_history, minPostCombinationDuration) {
  treatment_history <- treatment_history[(is.na(check_duration) | duration_era >= minPostCombinationDuration),]
  cli::cat_line(paste0("After minPostCombinationDuration: ", nrow(treatment_history)))

  return(treatment_history)
}

doEraCollapse <- function(treatment_history, eraCollapseSize) {
  # Order treatment_history by person_id, event_cohort_id, start_date, end_date
  treatment_history <- treatment_history[order(person_id, event_cohort_id,event_start_date, event_end_date),]

  # Find all rows with gap_same < eraCollapseSize
  rows <- which(treatment_history$gap_same < eraCollapseSize)

  # For all rows, modify the row preceding, loop backwards in case more than one collapse
  for (r in rev(rows)) {
    treatment_history[r - 1,"event_end_date"] <- treatment_history[r,event_end_date]
  }

  # Remove all rows with gap_same < eraCollapseSize
  treatment_history <- treatment_history[!rows,]
  treatment_history[,gap_same:=NULL]

  # Re-calculate duration_era
  treatment_history[,duration_era:=difftime(event_end_date , event_start_date, units = "days")]

  cli::cat_line(paste0("After eraCollapseSize: ", nrow(treatment_history)))
  return(treatment_history)
}


doFilterTreatments <- function(treatment_history, filterTreatments) {

  # Order treatment_history by person_id, event_start_date, event_end_date
  treatment_history <- treatment_history[order(person_id, event_start_date, event_end_date),]

  if (filterTreatments == "All") {} # Do nothing
  else {
    # Order the combinations
    cli::cat_line("Order the combinations.")
    combi <- grep("+", treatment_history$event_cohort_id, fixed=TRUE)
    if (length(combi) != 0) {
      concept_ids <- strsplit(treatment_history$event_cohort_id[combi], split="+", fixed=TRUE)
      treatment_history$event_cohort_id[combi] <- sapply(concept_ids, function(x) paste(sort(x), collapse = "+"))
    }

    if (filterTreatments == "First") {
      treatment_history <- treatment_history[, head(.SD,1), by=.(person_id, event_cohort_id)]

    } else if (filterTreatments == "Changes") {
      # Group all rows per person for which previous treatment is same
      tryCatch(treatment_history <- treatment_history[, group:=data.table::rleid(person_id,event_cohort_id)],
               error = function(e){print(paste0("Check if treatment_history contains sufficient records: ", e))})

      # Remove all rows with same sequential treatments
      treatment_history <- treatment_history[,.(event_start_date=min(event_start_date), event_end_date=max(event_end_date), duration_era=sum(duration_era)), by = .(person_id,index_year,event_cohort_id,group)]
      treatment_history[,group:=NULL]
    } else {
      warning("filterTreatments input incorrect, return all event cohorts ('All')")
    }
  }

  cli::cat_line(paste0("After filterTreatments: ", nrow(treatment_history)))

  return(treatment_history)
}

addDrugSequence <- function(treatment_history) {
  cli::cat_line("Adding drug sequence number.")
  treatment_history <- treatment_history[order(person_id, event_start_date, event_end_date),]
  treatment_history[, event_seq:=seq_len(.N), by= .(person_id)]
}

doMaxPathLength <- function(treatment_history, maxPathLength) {

  # Apply maxPathLength
  treatment_history <- treatment_history[event_seq <= maxPathLength,]

  cli::cat_line(paste0("After maxPathLength: ", nrow(treatment_history)))

  return(treatment_history)
}

addLabels <- function(treatment_history, eventCohortIds, eventCohortNames) {

  labels <- tibble(event_cohort_id = eventCohortIds,
                   event_cohort_name = eventCohortNames) %>%
    mutate(event_cohort_id = as.character(event_cohort_id))

  th <- treatment_history %>%
    dplyr::left_join(labels, by = c("event_cohort_id"))

  th$event_cohort_name[is.na(th$event_cohort_name)] <- sapply(th$event_cohort_id[is.na(th$event_cohort_name)], function(x) {

    # Revert search to look for longest concept_ids first
    for (l in nrow(labels):1)
    {
      # If treatment occurs twice in a combination (as monotherapy and as part of fixed-combination) -> remove monotherapy occurrence
      if (any(grep(labels$event_cohort_name[l], x))) {
        x <- gsub(labels$event_cohort_id[l], "", x)
      } else {
        x <- gsub(labels$event_cohort_id[l], labels$event_cohort_name[l], x)
      }
    }

    return(x)
  })


  # Filter out + at beginning/end or repetitions
  th$event_cohort_name <- gsub("\\++", "+", th$event_cohort_name)
  th$event_cohort_name <- gsub("^\\+", "", th$event_cohort_name)
  th$event_cohort_name <- gsub("\\+$", "", th$event_cohort_name)

  return(th)
}

orderCombinations <- function(th) {
  cli::cat_line("Ordering the combinations.")
  #some clean up for the combination names
  combi <- grep("+", th$event_cohort_name, fixed=TRUE)
  cohort_names <- strsplit(th$event_cohort_name[combi], split="+", fixed=TRUE)
  th$event_cohort_name[combi] <- sapply(cohort_names, function(x) paste(sort(x), collapse = "+"))
  th$event_cohort_name <- unlist(th$event_cohort_name)
  return(th)
}

postProcess <- function(treatment_history,
                        eventCohortIds,
                        eventCohortNames,
                        maxPathLength) {
  if (nrow(treatment_history) != 0) {
    res <- addDrugSequence(treatment_history) %>%
      doMaxPathLength(maxPathLength) %>%
      addLabels(eventCohortIds, eventCohortNames) %>%
      orderCombinations()
  } else{
    res <- treatment_history
    message("Treatment History has no rows")
  }

  return(res)
}
