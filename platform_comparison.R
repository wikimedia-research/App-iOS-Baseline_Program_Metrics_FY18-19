library(tidyverse)
library(magrittr)
library(crosstalk)
library(glue)

target_wikis <- c("frwiki", "enwiki", "kowiki", "hiwiki", "cswiki", "arwiki", "bnwiki", "fawiki", "fiwiki", "hewiki", "itwiki",
    "mrwiki", "ptwiki", "ruwiki", "svwiki", "tawiki", "zhwiki")
start_date <- as.Date("2018-07-01")
end_date <- as.Date("2018-09-30")
# target_wiki_languages <- c("English", "French", "Czech", "Korean", "Hindi")

ios_edits <- read_tsv("data/ios_edits.tsv", col_types = "Diliiilccl") %>%
  mutate(platform = "iOS") %>%
  rename(new_editor = new_ios_editor)
android_edits <- read_tsv("data/android_edits.tsv", col_types = "Diliiilccl") %>%
  mutate(platform = "Android") %>%
  rename(new_editor = new_android_editor)
all_edits <- rbind(ios_edits, android_edits) %>%
  filter(date >= start_date, date <= end_date)


edits_per_editor_daily <- all_edits %>%
  group_by(platform, wiki, language, date, local_user_id, new_editor) %>%
  summarize(edits = length(rev_id),
            content_edits = sum(namespace == 0)) %>%
  ungroup()


# Total app edit counts

total_edit <- edits_per_editor_daily %>%
  group_by(platform, wiki, language) %>%
  summarize(edits = sum(edits), content_edits = sum(content_edits)) %>%
  ungroup() %>%
  filter(wiki %in% target_wikis)
langugage_level <- total_edit %>%
  filter(platform == "Android") %>%
  arrange(edits) %>%
  select(language) %>%
  unlist()
p <- total_edit %>%
  mutate(language = factor(language, levels = langugage_level)) %>%
  ggplot2::ggplot(aes(x = language, y = edits, fill = platform)) +
  ggplot2::geom_bar(stat = "identity", position = "dodge") +
  ggplot2::scale_fill_brewer("Platform", palette = "Set1") +
  ggplot2::geom_text(aes(label = edits, hjust = -0.05), position = position_dodge(width = 1), size = 3) +
  ggplot2::labs(y = 'Number of edits', x = 'Language',
                title = 'Number of edits from July to September 2018, by platform and wiki') +
  coord_flip() +
  wmf::theme_min()
ggsave("total_edits.png", p, path = 'figures', units = "in", dpi = 300, height = 6, width = 10)


# Monthly edits per editor

edits_per_editor_monthly <- edits_per_editor_daily %>%
  mutate(month = lubridate::month(date, label = TRUE)) %>%
  group_by(platform, wiki, language, month, local_user_id) %>%
  summarize(edits = sum(edits), content_edits = sum(content_edits)) %>%
  ungroup()
edit_by_editor_benchmarks <- edits_per_editor_monthly %>%
  group_by(platform) %>%
  summarize(`1 edit/month editor proportion` = length(local_user_id[local_user_id != 0 & edits == 1]) / length(local_user_id[local_user_id != 0 & edits != 0]),
            `5+ edit/month editor proportion` = length(local_user_id[local_user_id != 0 & edits >= 5]) / length(local_user_id[local_user_id != 0 & edits != 0])
            )

target_editor_prop_1edit <- edits_per_editor_monthly %>%
  filter(local_user_id != 0, wiki %in% target_wikis) %>%
  dplyr::group_by(platform, `Language`=language) %>%
  summarize(n_editor = length(local_user_id[edits != 0]),
            editor_1edit = length(local_user_id[edits == 1])) %>%
  ungroup %>%
  cbind(as.data.frame(binom:::binom.confint(x=.$editor_1edit, n = .$n_editor, methods = "prop.test")[, c("mean", "lower", "upper")]))
p <- ggplot(target_editor_prop_1edit, aes(x=Language, y=mean, color = platform)) +
  geom_pointrange(aes(ymin = lower, ymax = upper), position = position_dodge(width = 0.7)) +
  scale_color_brewer("Platform", palette = "Set1", guide = guide_legend(ncol = 2)) +
  scale_y_continuous("1 edit/month editor proportion", labels = scales::percent_format()) +
  geom_text(aes(label = scales::percent(mean), y = upper + 0.01, hjust = "bottom"),
            position = position_dodge(width = 0.7)) +
  theme_light() +
  coord_flip() +
  geom_hline(aes(yintercept = `1 edit/month editor proportion`), edit_by_editor_benchmarks, linetype = "dashed", color = c("#E41A1C", "#377EB8")) +
  wmf::theme_min(axis.title.y = element_blank()) +
  labs(ylab="1 edit/month editor proportion", title = "Proportion of registered editors who only have 1 edit per month",
       subtitle = "By target Wikipedias in Jul - Sep 2018, with 95% confidence intervals. Dash line represent the global proportion across all wikipedia by platform.")
ggsave("1edit_proportion.png", p, path = 'figures', units = "in", dpi = 300, height = 8, width = 12)

target_editor_prop_5edit <- edits_per_editor_monthly %>%
  filter(local_user_id != 0, wiki %in% target_wikis) %>%
  dplyr::group_by(platform, `Language`=language) %>%
  summarize(n_editor = length(local_user_id[edits != 0]),
            editor_1edit = length(local_user_id[edits >= 5])) %>%
  ungroup %>%
  cbind(as.data.frame(binom:::binom.confint(x=.$editor_1edit, n = .$n_editor, methods = "prop.test")[, c("mean", "lower", "upper")]))
p <- ggplot(target_editor_prop_5edit, aes(x=Language, y=mean, color = platform)) +
  geom_pointrange(aes(ymin = lower, ymax = upper), position = position_dodge(width = 0.7)) +
  scale_color_brewer("Platform", palette = "Set1", guide = guide_legend(ncol = 2)) +
  scale_y_continuous("5+ edit/month editor proportion", labels = scales::percent_format()) +
  geom_text(aes(label = scales::percent(mean), y = upper + 0.01, hjust = "bottom"),
            position = position_dodge(width = 0.7)) +
  theme_light() +
  coord_flip() +
  geom_hline(aes(yintercept = `5+ edit/month editor proportion`), edit_by_editor_benchmarks, linetype = "dashed", color = c("#E41A1C", "#377EB8")) +
  wmf::theme_min(axis.title.y = element_blank()) +
  labs(ylab="5+ edit/month editor proportion", title = "Proportion of registered editors who have at least 5 edits per month",
       subtitle = "By target Wikipedias in Jul - Sep 2018, with 95% confidence intervals. Dash line represent the global proportion across all wikipedia by platform.")
ggsave("5edit_proportion.png", p, path = 'figures', units = "in", dpi = 300, height = 8, width = 12)


# Revert rate

revert_rates <- all_edits %>%
  rename(Language = language) %>%
  filter(!is.na(is_reverted)) %>%
  group_by(platform, wiki, Language) %>%
  summarize(`Number of edits` = length(rev_id),
            `Reverted edits` = sum(is_reverted),
            `Revert rate` = `Reverted edits`/`Number of edits`) %>%
  ungroup()
revert_rates_pdata <- revert_rates %>%
  filter(wiki %in% target_wikis) %>%
  cbind(as.data.frame(binom:::binom.confint(x=.$`Reverted edits`, n = .$`Number of edits`, methods = "prop.test")[, c("mean", "lower", "upper")]))
revert_rates_benchmarks <- all_edits %>%
  group_by(platform) %>%
  summarize(revert_rate = mean(is_reverted, na.rm = TRUE))

p <- ggplot(revert_rates_pdata, aes(x=Language, y=mean, color=platform)) +
  geom_pointrange(aes(ymin = lower, ymax = upper), position = position_dodge(width = 0.7)) +
  scale_color_brewer("Platform", palette = "Set1", guide = guide_legend(ncol = 2)) +
  scale_y_continuous("Revert Rate", labels = scales::percent_format()) +
  geom_text(aes(label = scales::percent(mean), y = upper + 0.01, hjust = "bottom"),
            position = position_dodge(width = 0.7)) +
  theme_light() +
  coord_flip() +
  geom_hline(aes(yintercept = revert_rate), revert_rates_benchmarks, linetype = "dashed", color = c("#E41A1C", "#377EB8")) +
  wmf::theme_min(axis.title.y = element_blank()) +
  labs(ylab="Revert Rate", title = "Revert rate of app edits by Wikipedia",
       subtitle = glue("By target Wikipedias from {start_date} to {end_date}, with 95% confidence intervals. Dash line represent revert rate across all wikipedia by platform."))
ggsave("revert_rate.png", p, path = 'figures', units = "in", dpi = 300, height = 8, width = 12)


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

# New editors (any account that has made at least one edit with the Android app since – but not any edits with either app prior to – 2018-07-01)
new_editor_retention <- edits_per_editor_daily %>%
  filter(new_editor == TRUE, local_user_id != 0, wiki %in% target_wikis) %>%
  group_by(platform, language, local_user_id) %>%
  summarize(
    first_edit_date = min(date),
    retained_30 = check_retention(first_edit_date, date, 1, 31, 30) # 31st-60th day following first_edit_date
  ) %>%
  summarize(
    n_users = sum(!is.na(retained_30)),
    retained_30 = sum(retained_30, na.rm = TRUE)
  ) %>%
  ungroup() %>%
  filter(n_users != 0) %>%
  cbind(as.data.frame(binom:::binom.confint(x=.$retained_30, n = .$n_users, methods = "prop.test")[, c("mean", "lower", "upper")]))
new_editor_retention_benchmark <- edits_per_editor_daily %>%
  filter(new_editor == TRUE, local_user_id != 0) %>%
  group_by(platform, local_user_id) %>%
  summarize(
    first_edit_date = min(date),
    retained_30 = check_retention(first_edit_date, date, 1, 31, 30) # 31st-60th day following first_edit_date
  ) %>%
  summarize(
    n_users = sum(!is.na(retained_30)),
    retention_30 = mean(retained_30, na.rm = TRUE)
  )
p <- ggplot(new_editor_retention, aes(x=language, y=mean, color=platform)) +
  geom_pointrange(aes(ymin = lower, ymax = upper), position = position_dodge(width = 0.7)) +
  scale_color_brewer("Platform", palette = "Set1", guide = guide_legend(ncol = 2)) +
  scale_y_continuous("Retention", labels = scales::percent_format()) +
  geom_text(aes(label = scales::percent(mean), y = upper + 0.01, hjust = "bottom"),
            position = position_dodge(width = 0.7)) +
  theme_light() +
  coord_flip() +
  geom_hline(aes(yintercept = retention_30), new_editor_retention_benchmark, linetype = "dashed", color = c("#E41A1C", "#377EB8")) +
  wmf::theme_min(axis.title.y = element_blank()) +
  labs(ylab="Retention", title = "Proportion of new editors that come back to edit in the second month since their first edit",
       subtitle = glue("By target Wikipedias from {start_date} to {end_date}, with 95% confidence intervals. Dash line represent retention across all wikipedia by platform.
                       New editors are those who never edit with either apps before."))
ggsave("new_editor_retention.png", p, path = 'figures', units = "in", dpi = 300, height = 8, width = 12)


# Active editor (everyone who edit at least once in the selected period)
active_editor_retention <- edits_per_editor_daily %>%
  filter(local_user_id != 0, wiki %in% target_wikis) %>%
  group_by(platform, language, local_user_id) %>%
  summarize(
    first_edit_date = min(date),
    retained_30 = check_retention(first_edit_date, date, 1, 31, 30) # 31st-60th day following first_edit_date
  ) %>%
  summarize(
    n_users = sum(!is.na(retained_30)),
    retained_30 = sum(retained_30, na.rm = TRUE)
  ) %>%
  ungroup() %>%
  filter(n_users != 0) %>%
  cbind(as.data.frame(binom:::binom.confint(x=.$retained_30, n = .$n_users, methods = "prop.test")[, c("mean", "lower", "upper")]))
active_editor_retention_benchmark <- edits_per_editor_daily %>%
  filter(local_user_id != 0) %>%
  group_by(platform, local_user_id) %>%
  summarize(
    first_edit_date = min(date),
    retained_30 = check_retention(first_edit_date, date, 1, 31, 30) # 31st-60th day following first_edit_date
  ) %>%
  summarize(
    n_users = sum(!is.na(retained_30)),
    retention_30 = mean(retained_30, na.rm = TRUE)
  )
p <- ggplot(active_editor_retention, aes(x=language, y=mean, color=platform)) +
  geom_pointrange(aes(ymin = lower, ymax = upper), position = position_dodge(width = 0.7)) +
  scale_color_brewer("Platform", palette = "Set1", guide = guide_legend(ncol = 2)) +
  scale_y_continuous("Retention", labels = scales::percent_format()) +
  geom_text(aes(label = scales::percent(mean), y = upper + 0.01, hjust = "bottom"),
            position = position_dodge(width = 0.7)) +
  theme_light() +
  coord_flip() +
  geom_hline(aes(yintercept = retention_30), active_editor_retention_benchmark, linetype = "dashed", color = c("#E41A1C", "#377EB8")) +
  wmf::theme_min(axis.title.y = element_blank()) +
  labs(ylab="Retention", title = glue("Proportion of all editors that come back to edit in the second month since their first edit from {start_date} to {end_date}"),
       subtitle = "By target Wikipedias, with 95% confidence intervals. Dash line represent retention across all wikipedia by platform.")
ggsave("active_editor_retention.png", p, path = 'figures', units = "in", dpi = 300, height = 8, width = 12)
