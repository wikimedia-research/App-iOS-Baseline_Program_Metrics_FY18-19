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
active_wikis <- active_wikis$language_name %>%
  set_names(active_wikis$dbname)

start_date <- "20180701"
end_date <- format(Sys.Date(), "%Y%m%d")

# Total iOS app edit counts

query <- "
SELECT DATE(date) AS date,
IFNULL(SUM(edits), 0) AS edits,
IFNULL(SUM(content_edits), 0) AS content_edits,
IFNULL(SUM(deleted_edits), 0) AS deleted_edits
FROM (
SELECT
LEFT(rev_timestamp, 8) AS `date`,
COUNT(*) AS `edits`,
SUM(page_namespace = 0) AS content_edits,
SUM(rev_deleted = 1) AS deleted_edits
FROM revision
INNER JOIN change_tag ON rev_id = ct_rev_id AND ct_tag = 'ios app edit'
LEFT JOIN page ON rev_page = page_id
WHERE rev_timestamp >= '{start_date}'
AND rev_timestamp < '{end_date}'
GROUP BY LEFT(rev_timestamp, 8)

UNION ALL

SELECT
LEFT(ar_timestamp, 8) AS `date`,
COUNT(*) AS `edits`,
SUM(ar_namespace = 0) AS content_edits,
COUNT(*) AS deleted_edits
FROM archive
INNER JOIN change_tag ON ar_rev_id = ct_rev_id AND ct_tag = 'ios app edit'
WHERE ar_timestamp >= '{start_date}'
AND ar_timestamp < '{end_date}'
GROUP BY LEFT(ar_timestamp, 8)
) AS edit_counts
GROUP BY date
"

active_wiki_edit_counts <- map_df(
  set_names(names(active_wikis), names(active_wikis)),
  ~ tryCatch(
    suppressMessages(wmf::mysql_read(glue(query), .x)),
    error = function(e) {
      return(data.frame())
    }
  ),
  .id = "wiki"
)

active_wiki_edit_counts <- active_wiki_edit_counts %>%
  mutate(date = as.Date(date)) %>%
  complete(date, nesting(wiki), fill = list(edits = 0, content_edits = 0, deleted_edits = 0)) %>%
  mutate(
    not_deleted_edits = edits - deleted_edits,
    language = active_wikis[wiki]
  )
save(active_wiki_edit_counts, file = "data/baseline_ios/active_wiki_edit_counts.RData")


# iOS edits by editor

query <- "
SELECT month,
local_user_id,
IFNULL(user_name, '') AS user_name,
IFNULL(SUM(edits), 0) AS edits,
IFNULL(SUM(content_edits), 0) AS content_edits,
IFNULL(SUM(deleted_edits), 0) AS deleted_edits
FROM (

SELECT
LEFT(rev_timestamp, 6) AS `month`,
rev_user AS `local_user_id`,
COUNT(*) AS `edits`,
SUM(page_namespace = 0) AS content_edits,
SUM(rev_deleted = 1) AS deleted_edits
FROM revision
INNER JOIN change_tag ON rev_id = ct_rev_id AND ct_tag = 'ios app edit'
LEFT JOIN page ON rev_page = page_id
WHERE LEFT(rev_timestamp, 6) = '201807' -- time restriction here
GROUP BY LEFT(rev_timestamp, 6), rev_user

UNION ALL

SELECT
LEFT(ar_timestamp, 6) AS `month`,
ar_user AS `local_user_id`,
COUNT(*) AS `edits`,
SUM(ar_namespace = 0) AS content_edits,
COUNT(*) AS deleted_edits
FROM archive
INNER JOIN change_tag ON ar_rev_id = ct_rev_id AND ct_tag = 'ios app edit'
WHERE LEFT(ar_timestamp, 6) = '201807' -- time restriction here
GROUP BY LEFT(ar_timestamp, 6), ar_user

) AS edit_per_editor
LEFT JOIN user ON local_user_id = user_id
GROUP BY month, local_user_id
"

active_wiki_edit_by_editor <- map_df(
  set_names(names(active_wikis), names(active_wikis)),
  ~ tryCatch(
    suppressMessages(wmf::mysql_read(glue(query), .x)),
    error = function(e) {
      return(data.frame())
    }
  ),
  .id = "wiki"
)

active_wiki_edit_by_editor <- active_wiki_edit_by_editor %>%
  mutate(
    not_deleted_edits = edits - deleted_edits,
    language = active_wikis[wiki]
  )
save(active_wiki_edit_by_editor, file = "data/baseline_ios/active_wiki_edit_by_editor.RData")
