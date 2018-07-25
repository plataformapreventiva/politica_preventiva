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

filter_values <- function(x, subset){
    x_char <- as.character(x)
    subset_char <- as.character(subset)
    ifelse(x_char %in% subset_char, x_char, NA)
}

pull_filler <- function(data, varname){
  varname_string <- sym(varname)
  varname_quo <- quo(!! varname_string)

  filler <- data %>%
            pull( !! varname_quo) %>%
            unique() %>%
            sort(decreasing = TRUE)
  return(filler[1])
}

tidy_count <- function(data, count_var, plotname, uncounted = c('actualizacion_sedesol', 'data_date'), subset = NULL){

  count_var_name <- sym(count_var)
  count_var_quo <- quo(!! count_var_name)

  tidy_count_data <-  data %>%
                        dplyr::group_by_at(vars(one_of(uncounted))) %>%
                        dplyr::count(!! count_var_quo) %>%
                        dplyr::mutate(plot = plotname,
                                      variable = count_var) %>%
                        dplyr::rename(categoria = !! count_var_quo,
                                      valor  = n) %>%
                        dplyr::ungroup() %>%
                        dplyr::filter(!is.na(categoria)) %>%
                        dplyr::mutate(categoria = as.character(categoria))

  if(is.null(subset)) return(tidy_count_data)

  subset_values = tibble(categoria = as.character(subset),
                         actualizacion_sedesol = pull_filler(data, 'actualizacion_sedesol'),
                         data_date = pull_filler(data, 'data_date'))

  if ('orden_gob' %in% uncounted){
    subset_values_filtered <- map_df(1:3, function(x) mutate(subset_values, orden_gob = x))
  } else {
      subset_values_filtered <- subset_values
    }

  tidy_count_data %>%
      right_join(subset_values_filtered) %>%
      replace_na(list(plot = plotname,
                      variable = count_var,
                      valor = 0))
}

get_varnames <- function(data, prefix, plot_title, subset_list){

  subset_vector = subset_list[prefix]

  data %>%
      names() %>%
      grep(prefix, ., value = T) %>%
      tibble(varname = .,
             plotname = plot_title,
             subset = ifelse(is.null(subset_vector), '', subset_vector))
}

create_varnames_data <- function(data, varnames, plotnames, subset_list){
map_df(1:length(varnames), function(x) get_varnames(data, varnames[x], plotnames[x], subset_list))
}

