all_edits <- read_tsv("data/ios_edits.tsv", col_types = "Diliiilccl")
start_date <- min(all_edits$date)
end_date <- max(all_edits$date)

edits_per_editor_daily <- all_edits %>%
  group_by(wiki, language, date, local_user_id, new_ios_editor) %>%
  summarize(edits = length(rev_id),
            content_edits = sum(namespace == 0)) %>%
  ungroup()


# Total iOS app edit counts

active_wiki_edit_counts <- edits_per_editor_daily %>%
  group_by(date, wiki, language) %>%
  summarize(edits = sum(edits), content_edits = sum(content_edits)) %>%
  ungroup() %>%
  complete(date, nesting(wiki, language), fill = list(edits = 0, content_edits = 0))


# iOS monthly edits per editor

edits_per_editor_monthly <- edits_per_editor_daily %>%
  mutate(month = lubridate::month(date, label = TRUE)) %>%
  filter(month != lubridate::month(Sys.Date(), label = TRUE)) %>% # remove the incomplete month
  group_by(wiki, language, month, local_user_id) %>%
  summarize(edits = sum(edits), content_edits = sum(content_edits)) %>%
  ungroup()


# Revert rate of iOS edits

revert_rates <- all_edits %>%
  rename(Language = language) %>%
  filter(!is.na(is_reverted)) %>%
  group_by(wiki, Language) %>%
  summarize(`Number of edits` = length(rev_id),
            `Reverted edits` = sum(is_reverted),
            `Revert rate` = `Reverted edits`/`Number of edits`) %>%
  ungroup()


# Editor retention

check_retention <- function(birth_date, edit_dates, t1 = 1, t2 = 30, t3 = 30) {
  # t1 represents activation period,
  # t2 represents trial period,
  # t3 represents survival period.
  # See https://meta.wikimedia.org/wiki/Research:Surviving_new_editor
  edit_dates <- sort(edit_dates)
  # Check if user made an edit in the activation period.
  # If a user didn't have any edits in activation period, they are not considered as new editor
  first_milestone <- birth_date + t1
  if (!any(edit_dates >= birth_date & edit_dates < first_milestone)) return(NA)
  # Check if user made another edit in the survival period:
  second_milestone <- birth_date + t2
  third_milestone <- birth_date + t2 + t3
  is_survived <- any(edit_dates >= second_milestone & edit_dates < third_milestone)
  # if we haven't reach survival period
  # or survival period hasn't end and user hasn't make an edit
  # this users should not be counted
  if( (Sys.Date() < second_milestone) |
      (Sys.Date() >= second_milestone & Sys.Date() < third_milestone & !is_survived) ) {
    return(NA)
  } else {
    return(is_survived)
  }
}

# New editors (any account that has made at least one edit with the iOS app since – but not any edits with either app prior to – 2018-07-01)
new_ios_editor_retention <- edits_per_editor_daily %>%
  filter(new_ios_editor == TRUE, local_user_id != 0) %>%
  group_by(language, local_user_id) %>%
  summarize(
    total_edits = sum(edits),
    first_edit_date = min(date),
    retained_first7 = check_retention(first_edit_date, date, 1, 1, 7), # 1st-7th day following first_edit_date
    retained_7 = check_retention(first_edit_date, date, 1, 8, 7), # 8th-14th day following first_edit_date
    retained_first15 = check_retention(first_edit_date, date, 1, 1, 15), # 1st-15th day following first_edit_date
    retained_15 = check_retention(first_edit_date, date, 1, 16, 15), # 16th-30th day following first_edit_date
    retained_first30 = check_retention(first_edit_date, date, 1, 1, 30), # 1st-30th day following first_edit_date
    retained_30 = check_retention(first_edit_date, date, 1, 31, 30), # 31st-60th day following first_edit_date
    retained_first60 = check_retention(first_edit_date, date, 1, 1, 60) # 1st-60th day following first_edit_date
  ) %>%
  summarize(
    n_users = n(),
    total_edits = sum(total_edits),
    retention_first7 = mean(retained_first7, na.rm = TRUE),
    retention_7 = mean(retained_7, na.rm = TRUE),
    retention_first15 = mean(retained_first15, na.rm = TRUE),
    retention_15 = mean(retained_15, na.rm = TRUE),
    retention_first30 = mean(retained_first30, na.rm = TRUE),
    retention_30 = mean(retained_30, na.rm = TRUE),
    retention_first60 = mean(retained_first60, na.rm = TRUE)
  )

# Active editor (everyone who edit at least once in the selected period)
active_ios_editor_retention <- edits_per_editor_daily %>%
  filter(local_user_id != 0) %>%
  group_by(language, local_user_id) %>%
  summarize(
    total_edits = sum(edits),
    first_edit_date = min(date),
    retained_first7 = check_retention(first_edit_date, date, 1, 1, 7), # 1st-7th day following first_edit_date
    retained_7 = check_retention(first_edit_date, date, 1, 8, 7), # 8th-14th day following first_edit_date
    retained_first15 = check_retention(first_edit_date, date, 1, 1, 15), # 1st-15th day following first_edit_date
    retained_15 = check_retention(first_edit_date, date, 1, 16, 15), # 16th-30th day following first_edit_date
    retained_first30 = check_retention(first_edit_date, date, 1, 1, 30), # 1st-30th day following first_edit_date
    retained_30 = check_retention(first_edit_date, date, 1, 31, 30), # 31st-60th day following first_edit_date
    retained_first60 = check_retention(first_edit_date, date, 1, 1, 60) # 1st-60th day following first_edit_date
  ) %>%
  summarize(
    n_users = n(),
    total_edits = sum(total_edits),
    retention_first7 = mean(retained_first7, na.rm = TRUE),
    retention_7 = mean(retained_7, na.rm = TRUE),
    retention_first15 = mean(retained_first15, na.rm = TRUE),
    retention_15 = mean(retained_15, na.rm = TRUE),
    retention_first30 = mean(retained_first30, na.rm = TRUE),
    retention_30 = mean(retained_30, na.rm = TRUE),
    retention_first60 = mean(retained_first60, na.rm = TRUE)
  )

# Weekly retention of July editors for 6 weeks

new_editor_6week_retain <- edits_per_editor_daily %>%
  filter(new_ios_editor == TRUE, local_user_id != 0) %>%
  group_by(language, local_user_id) %>%
  summarize(
    first_edit_date = min(date),
    retained_week1 = check_retention(first_edit_date, date, 1, 1, 7),
    retained_week2 = check_retention(first_edit_date, date, 1, 8, 7),
    retained_week3 = check_retention(first_edit_date, date, 1, 15, 7),
    retained_week4 = check_retention(first_edit_date, date, 1, 22, 7),
    retained_week5 = check_retention(first_edit_date, date, 1, 29, 7),
    retained_week6 = check_retention(first_edit_date, date, 1, 36, 7)
  ) %>%
  gather(key = "Week", value = "retained", -language, -local_user_id, -first_edit_date) %>%
  mutate(Week = as.numeric(gsub("retained_week", "", Week)))
new_editor_weekly_retention <- new_editor_6week_retain %>%
  group_by(language) %>%
  filter(length(unique(local_user_id)) > 30 | language %in% target_wiki_languages) %>%
  group_by(language, Week) %>%
  summarize(
    total_editors = sum(!is.na(retained)),
    `Retained Editors` = sum(retained, na.rm = TRUE)
  ) %>%
  ungroup %>%
  cbind(as.data.frame(binom:::binom.confint(x=.$`Retained Editors`, n = .$total_editors, methods = "prop.test")[, c("mean", "lower", "upper")]))
new_editor_weekly_retention <- new_editor_weekly_retention %>%
  rbind(
    new_editor_weekly_retention %>%
      group_by(Week) %>%
      summarize_all(mean) %>%
      mutate(language = "Average*"),
    new_editor_6week_retain %>%
      group_by(Week) %>%
      summarize(
        total_editors = sum(!is.na(retained)),
        `Retained Editors` = sum(retained, na.rm = TRUE)
      ) %>%
      ungroup %>%
      cbind(as.data.frame(binom:::binom.confint(x=.$`Retained Editors`, n = .$total_editors, methods = "prop.test")[, c("mean", "lower", "upper")])) %>%
      mutate(language = "Global*")
  ) %>%
  rename("Retention" = mean, "Language" = language)


active_editor_6week_retain <- edits_per_editor_daily %>%
  filter(local_user_id != 0) %>%
  group_by(language, local_user_id) %>%
  summarize(
    first_edit_date = min(date),
    retained_week1 = check_retention(first_edit_date, date, 1, 1, 7),
    retained_week2 = check_retention(first_edit_date, date, 1, 8, 7),
    retained_week3 = check_retention(first_edit_date, date, 1, 15, 7),
    retained_week4 = check_retention(first_edit_date, date, 1, 22, 7),
    retained_week5 = check_retention(first_edit_date, date, 1, 29, 7),
    retained_week6 = check_retention(first_edit_date, date, 1, 36, 7)
  ) %>%
  gather(key = "Week", value = "retained", -language, -local_user_id, -first_edit_date) %>%
  mutate(Week = as.numeric(gsub("retained_week", "", Week)))
active_editor_weekly_retention <- active_editor_6week_retain %>%
  group_by(language) %>%
  filter(length(unique(local_user_id)) > 50 | language %in% target_wiki_languages) %>%
  group_by(language, Week) %>%
  summarize(
    total_editors = sum(!is.na(retained)),
    `Retained Editors` = sum(retained, na.rm = TRUE)
  ) %>%
  ungroup %>%
  cbind(as.data.frame(binom:::binom.confint(x=.$`Retained Editors`, n = .$total_editors, methods = "prop.test")[, c("mean", "lower", "upper")]))
active_editor_weekly_retention <- active_editor_weekly_retention %>%
  rbind(
    active_editor_weekly_retention %>%
      group_by(Week) %>%
      summarize_all(mean) %>%
      mutate(language = "Average*"),
    active_editor_6week_retain %>%
      group_by(Week) %>%
      summarize(
        total_editors = sum(!is.na(retained)),
        `Retained Editors` = sum(retained, na.rm = TRUE)
      ) %>%
      ungroup %>%
      cbind(as.data.frame(binom:::binom.confint(x=.$`Retained Editors`, n = .$total_editors, methods = "prop.test")[, c("mean", "lower", "upper")])) %>%
      mutate(language = "Global*")
  ) %>%
  rename("Retention" = mean, "Language" = language)
