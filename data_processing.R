load("data/edits_per_editor_daily.RData")
all_edits <- read_tsv("data/all_edits.tsv", col_types = "Diiiicl")

# Total iOS app edit counts

active_wiki_edit_counts <- edits_per_editor_daily %>%
  group_by(edit_date, wiki, language) %>%
  summarize(edits = sum(edits), content_edits = sum(content_edits)) %>%
  ungroup() %>%
  complete(edit_date, nesting(wiki, language), fill = list(edits = 0, content_edits = 0))


# iOS monthly edits per editor

edits_per_editor_monthly <- edits_per_editor_daily %>%
  mutate(month = lubridate::month(edit_date, label = TRUE)) %>%
  filter(month != 'Sep') %>%
  group_by(wiki, language, month, local_user_id) %>%
  summarize(edits = sum(edits), content_edits = sum(content_edits))


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
    first_edit_date = min(edit_date),
    retained_first6 = check_retention(first_edit_date, edit_date, 1, 1, 6), # next 6 days
    retained_7 = check_retention(first_edit_date, edit_date, 1, 7, 7), # 7 days after a week
    retained_15 = check_retention(first_edit_date, edit_date, 1, 15, 15), # 15 days after 15 days
    retained_30 = check_retention(first_edit_date, edit_date, 1, 30, 30) # 30 days after 30 days
  ) %>%
  filter(first_edit_date >= as.Date("2018-07-01"), first_edit_date < as.Date("2018-09-01")) %>% # new editors in Jul&Aug
  summarize(
    n_users = n(),
    median_edits_per_user = median(total_edits),
    total_edits = sum(total_edits),
    retention_first6 = mean(retained_first6, na.rm = TRUE),
    retention_7 = mean(retained_7, na.rm = TRUE),
    retention_15 = mean(retained_15, na.rm = TRUE),
    retention_30 = mean(retained_30, na.rm = TRUE)
  )

# Active editor (everyone who edit at least once in the selected period)
active_ios_editor_retention <- edits_per_editor_daily %>%
  filter(local_user_id != 0) %>%
  group_by(wiki, language, local_user_id) %>%
  summarize(
    total_edits = sum(edits),
    first_edit_date = min(edit_date),
    retained_first6 = check_retention(first_edit_date, edit_date, 1, 1, 6), # next 6 days
    retained_7 = check_retention(first_edit_date, edit_date, 1, 7, 7), # 7 days after a week
    retained_15 = check_retention(first_edit_date, edit_date, 1, 15, 15), # 15 days after 15 days
    retained_30 = check_retention(first_edit_date, edit_date, 1, 30, 30) # 30 days after 30 days
  ) %>%
  filter(first_edit_date >= as.Date("2018-07-01"), first_edit_date < as.Date("2018-09-01")) %>% # active editors in Jul&Aug
  summarize(
    n_users = n(),
    median_edits_per_user = median(total_edits),
    total_edits = sum(total_edits),
    retention_first6 = mean(retained_first6, na.rm = TRUE),
    retention_7 = mean(retained_7, na.rm = TRUE),
    retention_15 = mean(retained_15, na.rm = TRUE),
    retention_30 = mean(retained_30, na.rm = TRUE)
  )
