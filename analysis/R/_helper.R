
makeSurvTable <- function(fit) {

  survProb <- gtsummary::tbl_survfit(
    fit,
    times = c(0.5, 1, 2)) %>%
    gtsummary::as_tibble()

  names(survProb) <- c("Characteristic", "6m survival", "1yr survival", "2yr survival")

  survProb <- survProb %>%
    dplyr::filter(!grepl("stratum_name", Characteristic) & !grepl("type", Characteristic))

  return(survProb)
}


makeTimeTable <- function(fit) {

  timeTb <- gtsummary::tbl_survfit(
    fit,
    probs = c(0.25, 0.5, 0.75)) %>%
    gtsummary::as_tibble()

  names(timeTb) <- c("Characteristic", "Time (days) at p25",
                     "Time (days) at p50",
                     "Time (days) at p75")

  timeTb <- timeTb %>%
    dplyr::filter(!grepl("stratum_name", Characteristic) & !grepl("type", Characteristic))

  return(timeTb)
}


kmPlot <- function(fit) {

  #drug_cols <- c("#88CCEE", "#CC6677", "#DDCC77", "#117733", "#332288", "#AA4499","#44AA99", "#999933", "#882255", "#661100", "#6699CC")

  drug_cols <- colorspace::rainbow_hcl(length(fit$strata))

  fit %>%
    ggsurvfit::ggsurvfit(linewidth = 1.2,
                         theme = list(ggplot2::theme_classic(), ggplot2::theme(legend.position = "bottom"))) +
    ggsurvfit::add_quantile(y_value = 0.5, linetype = 0, color = "grey30", linewidth = 0.8) +
    ggsurvfit::scale_ggsurvfit(x_scales = list(breaks = 0:15, limits = c(0,12))) +
    ggplot2::scale_color_manual(values = drug_cols) +
    ggplot2::labs(x = "Time (in years)") +
    ggplot2::ggtitle("Kaplan Meier plot")

}


kmPlotPhoto <- function(fit, database, cohort, eventType, eraCollapseSize, tteStratas) {

  drug_cols <- colorspace::rainbow_hcl(length(fit$strata))

  fit %>%
    ggsurvfit::ggsurvfit(linewidth = 1.2,
                         theme = list(ggplot2::theme_classic(), ggplot2::theme(legend.position = "bottom"))) +
    ggsurvfit::add_quantile(y_value = 0.5, linetype = 0, color = "grey30", linewidth = 0.8) +
    ggsurvfit::scale_ggsurvfit(x_scales = list(breaks = 0:12, limits = c(0,12))) +
    ggplot2::scale_color_manual(values = drug_cols) +
    ggplot2::labs(x = "Time (in years)") +
    ggplot2::ggtitle("Kaplan Meier plot")

  ggplot2::ggsave(filename = here::here("output", "www", database,  cohort,
                                        paste0("km_", database, "_", cohort, "_", eventType, "_", eraCollapseSize, "_", tteStratas, ".png")),
                  width = 8,
                  height = 6)
}


kmPlotPhotoStrata <- function(fit, database, cohort, eventType, eraCollapseSize, tteStratas) {

  drug_cols <- colorspace::rainbow_hcl(length(fit$survFit_0$strata) + 5L)


  p1 <- fit$survFit_1 %>%
      ggsurvfit::ggsurvfit(linewidth = 0.7,
                           theme = list(ggplot2::theme_classic(), ggplot2::theme(legend.position = "bottom"))) +
      ggsurvfit::add_quantile(y_value = 0.5, linetype = 0, color = "grey30", linewidth = 0.8) +
      ggsurvfit::scale_ggsurvfit(x_scales = list(breaks = 0:12, limits = c(0,12))) +
      ggplot2::scale_color_manual(values = drug_cols) +
      ggplot2::labs(x = "Time (in years)") +
      ggplot2::ggtitle("Kaplan Meier plot")

  p0 <- fit$survFit_0 %>%
    ggsurvfit::ggsurvfit(linewidth = 0.7,
                         theme = list(ggplot2::theme_classic(), ggplot2::theme(legend.position = "bottom"))) +
    ggsurvfit::add_quantile(y_value = 0.5, linetype = 0, color = "grey30", linewidth = 0.8) +
    ggsurvfit::scale_ggsurvfit(x_scales = list(breaks = 0:12, limits = c(0,12))) +
    ggplot2::scale_color_manual(values = drug_cols) +
    ggplot2::labs(x = "Time (in years)") +
    ggplot2::ggtitle("Kaplan Meier plot")

  p1 + p0 + plot_annotation(tag_levels = list(c("Yes", "No")))

  ggplot2::ggsave(filename = here::here("output", "www", database,  cohort,
                                        paste0("km_", database, "_", cohort, "_", eventType, "_", eraCollapseSize, "_", tteStratas, ".png")),
                  width = 14,
                  height = 10)

}


kmPlotPhotoAgeStrata <- function(fit, database, cohort, eventType, eraCollapseSize, tteStratas) {

  drug_cols <- colorspace::rainbow_hcl(length(fit$survFit_1$strata) + 5L)

  p1 <- fit$survFit_1 %>%
    ggsurvfit::ggsurvfit(linewidth = 0.7,
                         theme = list(ggplot2::theme_classic(), ggplot2::theme(legend.position = "bottom"))) +
    ggsurvfit::add_quantile(y_value = 0.5, linetype = 0, color = "grey30", linewidth = 0.8) +
    ggsurvfit::scale_ggsurvfit(x_scales = list(breaks = 0:12, limits = c(0,12))) +
    ggplot2::scale_color_manual(values = drug_cols) +
    ggplot2::labs(x = "Time (in years)") +
    ggplot2::ggtitle("Kaplan Meier plot")

  p2 <- fit$survFit_2 %>%
    ggsurvfit::ggsurvfit(linewidth = 0.7,
                         theme = list(ggplot2::theme_classic(), ggplot2::theme(legend.position = "bottom"))) +
    ggsurvfit::add_quantile(y_value = 0.5, linetype = 0, color = "grey30", linewidth = 0.8) +
    ggsurvfit::scale_ggsurvfit(x_scales = list(breaks = 0:12, limits = c(0,12))) +
    ggplot2::scale_color_manual(values = drug_cols) +
    ggplot2::labs(x = "Time (in years)") +
    ggplot2::ggtitle("Kaplan Meier plot")

  p3 <- fit$survFit_3 %>%
    ggsurvfit::ggsurvfit(linewidth = 0.7,
                         theme = list(ggplot2::theme_classic(), ggplot2::theme(legend.position = "bottom"))) +
    ggsurvfit::add_quantile(y_value = 0.5, linetype = 0, color = "grey30", linewidth = 0.8) +
    ggsurvfit::scale_ggsurvfit(x_scales = list(breaks = 0:12, limits = c(0,12))) +
    ggplot2::scale_color_manual(values = drug_cols) +
    ggplot2::labs(x = "Time (in years)") +
    ggplot2::ggtitle("Kaplan Meier plot")

  p4 <- fit$survFit_4 %>%
    ggsurvfit::ggsurvfit(linewidth = 0.7,
                         theme = list(ggplot2::theme_classic(), ggplot2::theme(legend.position = "bottom"))) +
    ggsurvfit::add_quantile(y_value = 0.5, linetype = 0, color = "grey30", linewidth = 0.8) +
    ggsurvfit::scale_ggsurvfit(x_scales = list(breaks = 0:12, limits = c(0,12))) +
    ggplot2::scale_color_manual(values = drug_cols) +
    ggplot2::labs(x = "Time (in years)") +
    ggplot2::ggtitle("Kaplan Meier plot")


  p1 + p2 + p3 + p4  + plot_annotation(tag_levels = 'A')

  ggplot2::ggsave(filename = here::here("output", "www", database,  cohort,
                                        paste0("km_", database, "_", cohort, "_", eventType, "_", eraCollapseSize, "_", tteStratas, ".png")),
                  width = 14,
                  height = 10)

}
