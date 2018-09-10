# On stat machine

library(magrittr)
library(glue)
library(tidyverse)

# Get a list of active wikis

site_matrix <- jsonlite::fromJSON("https://www.mediawiki.org/w/api.php?action=sitematrix&format=json&smtype=language&formatversion=2")$sitematrix
site_matrix$count <- NULL
extract_active_wiki <- function(listname) {
  if ("closed" %in% names(site_matrix[[listname]]$site)){
    output <- site_matrix[[listname]]$site %>%
      filter(is.na(closed), code == "wiki") %>%
      select(-closed)
  } else if (length(site_matrix[[listname]]$site) == 0){
    return(NULL)
  } else {
    output <- site_matrix[[listname]]$site %>%
      filter(code == "wiki")
  }

  if (nrow(output) == 0) {
    return(NULL)
  } else {
    output <- data.frame(
      language_code = site_matrix[[listname]]$code,
      language_name = site_matrix[[listname]]$localname,
      output
    )
    return(output)
  }
}

active_wikis <- purrr::map_df(
  .x = names(site_matrix),
  .f = extract_active_wiki
)

# Query

start_date <- "20180701"
end_date <- format(Sys.Date(), "%Y%m%d")

query <- "
SELECT
ios_app_edit_counts.local_user_id AS local_user_id,
IFNULL(previous_app_editors.new_ios_editor, 'TRUE') AS new_ios_editor,
edit_date,
edits,
content_edits
FROM (
SELECT
local_user_id,
edit_date,
COUNT(*) AS edits,
SUM(namespace = 0) AS content_edits
FROM (
-- Edits made with iOS app on visible pages:
SELECT
rev_user AS local_user_id,
DATE(LEFT(rev_timestamp, 8)) AS edit_date,
page.page_namespace AS namespace
FROM change_tag
INNER JOIN revision ON (
revision.rev_id = change_tag.ct_rev_id
AND revision.rev_timestamp >= '{start_date}'
AND revision.rev_timestamp < '{end_date}'
AND change_tag.ct_tag = 'ios app edit'
)
LEFT JOIN page ON revision.rev_page = page.page_id
UNION ALL
-- Edits made with iOS app on deleted pages:
SELECT
ar_user AS local_user_id,
DATE(LEFT(ar_timestamp, 8)) AS edit_date,
ar_namespace AS namespace
FROM change_tag
INNER JOIN archive ON (
archive.ar_id = change_tag.ct_rev_id
AND archive.ar_timestamp >= '{start_date}'
AND archive.ar_timestamp < '{end_date}'
AND change_tag.ct_tag = 'ios app edit'
)
) AS ios_app_edits
GROUP BY local_user_id, edit_date
) AS ios_app_edit_counts
LEFT JOIN (
-- Editors who have used a mobile app (Android/iOS)
-- to edit Wikipedia before the start_date:
SELECT DISTINCT local_user_id,
'FALSE' as new_ios_editor
FROM (
-- Editors who have previously used a mobile app to edit visible pages:
SELECT rev_user AS local_user_id
FROM change_tag
INNER JOIN revision ON (
revision.rev_id = change_tag.ct_rev_id
AND revision.rev_timestamp < '{start_date}'
AND change_tag.ct_tag = 'mobile app edit'
)
UNION ALL
-- Editors who have previously used a mobile app to edit deleted pages:
SELECT ar_user AS local_user_id
FROM change_tag
INNER JOIN archive ON (
archive.ar_id = change_tag.ct_rev_id
AND archive.ar_timestamp < '{start_date}'
AND change_tag.ct_tag = 'mobile app edit'
)
) AS combined_revisions
) AS previous_app_editors
ON previous_app_editors.local_user_id = ios_app_edit_counts.local_user_id
"

edits_per_editor_daily <- map_df(
  set_names(active_wikis$dbname, active_wikis$dbname),
  ~ tryCatch(
    suppressMessages(wmf::mysql_read(glue(query), .x)),
    error = function(e) {
      return(data.frame())
    }
  ),
  .id = "wiki"
) %>%
  mutate(
    edit_date = as.Date(edit_date),
    language = active_wikis$language_name[match(wiki, active_wikis$dbname)]
  )

save(edits_per_editor_daily, file = "data/baseline_ios/edits_per_editor_daily.RData")
system("scp chelsyx@stat5:~/data/baseline_ios/edits_per_editor_daily.RData data/")
