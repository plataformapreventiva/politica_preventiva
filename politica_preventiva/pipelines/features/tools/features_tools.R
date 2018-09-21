#!/usr/bin/env Rscript

library(dplyr)
library(purrr)
library(rlang)
library(yaml)

apply_condition <- function(df, ids, name, condition, groupby, dofilter){
    #' @title Create variable from condition.
    #' @description This function creates new variable by parsing
    #' condition from yaml and applying it to a dataframe
    #' @param df data.frame (df)
    #' @param ids table ids column name (string)
    #' @param name name of new variable to be created (string)
    #' @condition condition to be evaluated to create new variable (string)
    #' @groupby name of df variable to groupby (string)
    #' @dofilter text to parse in order to filter df rows and apply
    #' condition to those rows (string)

  condition <- eval(parse(text=condition))

  if (is.null(dofilter)) {
    if (!is.null(groupby)){
      df_tmp <- df %>%
                  dplyr::group_by(!!!rlang::syms(groupby)) %>%
                  dplyr::summarise(!!name := !!condition) %>%
                  dplyr::compute()
      df <- df %>%
              dplyr::left_join(df_tmp, by = groupby) %>%
              dplyr::mutate_at(vars(one_of(name)), funs(coalesce(., 0)))
    } else {
      df <- df %>%
              dplyr::mutate(!!name := !!condition)
    }
  } else {
    dofilter <- glue::glue("quote({dofilter})")
    dofilter <- eval(parse(text = dofilter))

    if (!is.null(groupby)){
      df_tmp <- df %>%
                  dplyr::filter(!!dofilter) %>%
                  dplyr::group_by(!!!rlang::syms(groupby)) %>%
                  dplyr::summarise(!!name := !!condition) %>%
                  dplyr::compute()
      df <- df %>%
              dplyr::left_join(df_tmp, by = groupby) %>%
              dplyr::mutate_at(name, funs(coalesce(., 0)))
    } else {
      df_tmp <- df %>%
                  dplyr::filter(!!dofilter) %>%
                  dplyr::mutate(!!name := !!condition) %>%
                  dplyr::select(!!!c(rlang::sym(name), ids)) %>%
                  dplyr::compute()
      df <- df %>% dplyr::left_join(df_tmp, by = ids)
    }
  }
  return(df)
}

getDummy <- function(df, ids, name, condition, groupby = NA, dofilter = NA){
  condition <- glue::glue("quote(ifelse(({condition}), 1, 0))")

  df %>%
    apply_condition(ids, name, condition, groupby, dofilter) %>%
    dplyr::compute()
}

getCases <- function(df, ids, name, condition, groupby = NA, dofilter = NA){
  condition <- glue::glue("quote(case_when({condition}))")

  df %>%
    apply_condition(ids, name, condition, groupby, dofilter) %>%
    dplyr::compute()
}

getFunc <- function(df, ids, name, condition, groupby = NA, dofilter = NA){
  condition <- glue::glue("quote({condition})")

  df %>%
    apply_condition(ids, name, condition, groupby, dofilter) %>%
    dplyr::compute()
}


aux <- function(df=NULL, ids=NULL, transmute=NULL, condition=NULL, name=NULL){
  fun <- get(transmute$fun)
  groupby <- try(transmute$groupby)
  dofilter <- try(transmute$filter)

  fun(df = df,
      ids = ids,
      name = name,
      condition = condition,
      groupby = groupby,
      dofilter = dofilter)
}

make_features <- function(yml_name){
  # run chunks
  yml <- yaml::read_yaml(yml_name)
  doChunk <- function(df=NULL, chunkname=NULL, vars=NULL, ids=NULL, return.all=FALSE){
    #' @title Apply chunk
    #' @description Apply functions to a complete chunk of the yaml
    #' @param df data.frame (df)
    #' @param chunkname key name in yaml (string)
    #' @param ids table ids column name (string)
    #' @param return.all  return all df or just new columns (bool)
    #' @return data.frame with new columns (df)
    params <- yml[[c(chunkname)]]

    names <- c()
    for (i in seq_along(params)){
      if (params[[i]]$name %in% vars){
        df <- aux(df=df,
                   ids=ids,
                   transmute=params[[i]]$transmute,
                   condition=params[[i]]$condition,
                   name=params[[i]]$name) %>% compute()
        print(params[[i]]$name)
        names <- c(names, params[[i]]$name)
        }
    }
    if(return.all) return(df)

    df %>% select(c(ids, names))
  }
}

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


