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


