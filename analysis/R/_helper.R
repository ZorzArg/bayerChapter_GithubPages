# ## Bind rows of data frames located in the same folder
# bindTTE <- function(outputPath,
#                     filename,
#                     database,
#                     cohortName)  {
#
#
#   if (filename %in% c("postIndex", "incidence", "baseline", "cohortManifest", "strataManifest", "probTb", "timeTb")) {
#
#     path <- here::here(outputPath, database, cohortName)
#
#     ## List all csv files in folder
#     filepath <- list.files(path, pattern = glue::glue("{filename}"), full.names = TRUE)
#
#     ## Read all files and save in list
#     listed_files <- lapply(filepath, readr::read_csv, show_col_types = FALSE)
#
#     ## Created binded data frame with all data frames of list
#     binded_df <- dplyr::bind_rows(listed_files)
#
#     outputPath <-  here::here(outputPath, database)
#
#     ## Save output
#     df <- readr::write_csv(
#       x = binded_df,
#       file = file.path(outputPath, paste0(filename, "_", cohortName, ".csv")),
#       append = FALSE
#     )
#
#   } else {
#
#     print("Invalid option")
#
#   }
#
#   ## Delete files from directory
#   #invisible(lapply(filepath, unlink))
#   #return(df)
#
# }
#
#
# ## Bind rows of data frames located in the same folder
# bindFiles <- function(outputPath,
#                       filename,
#                       database)  {
#
#   masterFile <- here::here(outputPath, paste0(filename, "_", database, ".csv"))
#   if (file.exists(masterFile)) {
#     unlink(masterFile)
#   }
#
#   if (filename %in% c("postIndex", "incidence", "baseline", "cohortManifest", "strataManifest", "probTb", "timeTb")) {
#
#     path <- here::here(outputPath, database)
#
#     ## List all csv files in folder
#     filepath <- list.files(path, pattern = glue::glue("{filename}"), full.names = TRUE)
#
#     ## Read all files and save in list
#     listed_files <- lapply(filepath, readr::read_csv, show_col_types = FALSE)
#
#     ## Created binded data frame with all data frames of list
#     binded_df <- dplyr::bind_rows(listed_files)
#
#     #outputPath <- here::here("output", "09_TimePropTables")
#
#     ## Save output
#     df <- readr::write_csv(
#       x = binded_df,
#       file = file.path(outputPath, paste0(filename, "_", database, ".csv")),
#       append = FALSE
#     )
#
#   } else {
#
#     print("Invalid option")
#
#   }
#
#   ## Delete files from directory
#   invisible(lapply(filepath, unlink))
#
# }
#
#
