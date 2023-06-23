# Dependencies ----------------

library(dplyr)
library(readr)
source("analysis/R/reportHelpers.R")

# Set variables ----------------

database <- c("mrktscan", "optum", "cprd_aurum", "cprd_gold") # Should be the name of the database as is in output folder

cohort <- c("c1", "c2", "c3", "c4", "c5", "c6")


# Load data ----------------
tik <- Sys.time()

## cohortManifest & strataManifest ----------------

#debug(loadCohortManifest)
loadCohortManifest(database = database)

#debug(loadStrataManifest)
loadStrataManifest(database = database)


## Baseline ----------------
#database <- c("optum", "cprd_aurum", "cprd_gold")
#debug(loadBaseline)
loadBaseline(database = database)


## Post-index ----------------

#debug(loadPostIndex)
loadPostIndex(database = database)


## Incidence ----------------

#debug(loadIncidence)
loadIncidence(database = database)


## Treatment Pathways (Tables) ----------------

#debug(loadTreatmentPathways)
loadTreatmentPathways(database = database,
                      cohort = cohort)


## Treatment Pathways (Sankey plots) ----------------
# RDS files for creating Sankey plots are saved in the "report/data" folder during execution of 07_treatmentPatterns.R


## TTE (Tables) ----------------

#debug(loadTTE)
loadTTE(database = database,
        cohort = cohort)


## TTE (KM plots) ----------------
# Pictures of the KM plots are created and saved in the "report/www" folder during execution



## Time
tok <- Sys.time()
tdif <- tok - tik
tok_format <- paste(scales::label_number(0.01)(as.numeric(tdif)), attr(tdif, "units"))
cli::cat_line("\nLoading data took: ", tok_format)
