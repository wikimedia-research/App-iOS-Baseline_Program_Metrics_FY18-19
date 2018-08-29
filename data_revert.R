# On stat machine

library(magrittr)
library(glue)
library(tidyverse)
library(reticulate)

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

# Query all the revisions

start_date <- "20180701"
end_date <- format(Sys.Date(), "%Y%m%d")

query <- "
SELECT
DATE(LEFT(rev_timestamp, 8)) AS `date`,
rev_id,
rev_user AS `local_user_id`,
page_namespace
FROM revision
INNER JOIN change_tag ON rev_id = ct_rev_id AND ct_tag = 'ios app edit'
LEFT JOIN page ON rev_page = page_id
WHERE rev_timestamp >= '{start_date}'
AND rev_timestamp < '{end_date}'
"

all_edits <- map_df(
  set_names(active_wikis$dbname, active_wikis$dbname),
  ~ tryCatch(
    suppressMessages(wmf::mysql_read(glue(query), .x)),
    error = function(e) {
      return(data.frame())
    }
  ),
  .id = "wiki"
)

print("Finish querying all_edits")

# Fetch revert information

# use_virtualenv("~/python_virtualenv/edit_revert") NOT WORK!!! https://github.com/rstudio/reticulate/issues/194
use_python("/Users/cxie/anaconda/bin/python3")
mwapi <- import("mwapi")
mwreverts <- import("mwreverts.api")

fetch_revert <- function(session, rev_id) {
  tryCatch({
    reverted <- mwreverts$check(session, rev_id, radius=5, window=48*60*60)[[2]]
    return(!is.null(reverted))
  }, error = function(e) {
    return(NA)
    }
  )
}

fetch_revert_for_wiki <- function(data, wiki) {
  session = mwapi$Session(host = active_wikis$url[active_wikis$dbname == wiki], user_agent="Revert detection <cxie@wikimedia.org>")
  output <- data %>%
    dplyr::filter(wiki == wiki) %>%
    dplyr::mutate(
      is_reverted = purrr::map_lgl(
      .x = rev_id,
      .f = ~fetch_revert(session, rev_id = .x)
    )
    )
  return(output)
}

all_edits <- purrr::map_df(
  .x = unique(all_edits$wiki),
  .f = ~fetch_revert_for_wiki(data = all_edits, wiki = .x)
  )
