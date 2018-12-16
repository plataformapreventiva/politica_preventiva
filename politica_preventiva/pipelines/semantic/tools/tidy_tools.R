#!/usr/bin/env Rscript
# Tidyverse like tools for Postgres

gather_db <- function(df, key, value, not_gathered) {
  key_vals <- setdiff(colnames(df),not_gathered)

  get_rows <- function(key_val) {
    df %>%
      select_(.dots = c(not_gathered, key_val)) %>%
      mutate(key_ = as.character(key_val)) %>%
      rename_(value_ = key_val) %>%
      select_(.dots = c(not_gathered, "key_", "value_"))
  }

  df_new <- lapply(key_vals, get_rows)
  Reduce(union_all, df_new) %>%
    rename_(.dots= setNames("key_", key)) %>%
    rename_(.dots= setNames("value_", value))
}

spread_db <- function(df, key, value) {
  key_vals <-
    df %>%
    select_(key) %>%
    distinct() %>%
    collect() %>%
    .[[1]] %>%
    sort()

  get_rows <- function(key_val) {
    df %>%
      rename_(.dots= setNames(key, "key")) %>%
      rename_(.dots= setNames(value, key_val)) %>%
      filter(key == key_val) %>%
      select(-key)
  }

  join_vars <- setdiff(colnames(df), c(key, value))
  print(join_vars)
  full_join_alt <- function(x, y) {
    full_join(x, y, by=join_vars)
  }

  df_new <- lapply(key_vals, get_rows)
  Reduce(full_join_alt, df_new)
}

replace_high_bound <- function(x){
    wrong_bound <- gsub('.*,(.*)\\)', '\\1', x) %>% as.numeric()
    paste0(gsub('(.*,)(.*)\\)', '\\1', x), wrong_bound-1, ')')
}

cut_edad <- function(x){
  cut(x, breaks = c(seq(from = 0, to = 76, by = 5), Inf), right = F) %>%
      replace_high_bound() %>%
      gsub('\\[([0-9]+),(.*)\\)', '\\1-\\2', .) %>%
      gsub('-Inf', '+', .)
}

check_queries <- function(plots_metadata, sql_queries){
  query_data <- tibble::tibble()
  for (i in 1:nrow(plots_metadata)){
    query <- sql_queries[i]
    print(i)
    data <- tryCatch(
                     {query_result <- tbl(con, dbplyr::sql(query)) %>%
                                        dplyr::collect()
                      plots_metadata[i,] %>%
                      mutate(status = 1,
                             error = list('e'=''))
                     },
                     error = function(e) {
                      result_data <- plots_metadata[i,] %>%
                                    mutate(status = 0,
                                           error = list(e))
                       print(e)
                       return(result_data)
                     })
    query_data <- bind_rows(query_data, data)
  }
return(query_data)
}
