#!/usr/bin/env Rscript

library(rlang)
library(dplyr)
##############################
# Auxiliary functions
##############################

#source('setup.R')
#source('connect.R')
source('pipelines/etl/tools/clean_tools.R')


get_table_name <- function(table_name){
  raw_table_name <- sub('temp_', '', table_name)

  # Define which schema to look for table
  if (grepl('temp_', table_name)){
    schema <- 'public'
  } else{
    schema <- 'raw'
  }

  return(list(raw_table_name, schema))
}


get_table <- function(table_name, db_connection, schema = 'public'){
  # Lazy query for fetching table
  if (schema == 'public'){
    dplyr::tbl(db_connection, table_name)
  } else{
    dplyr::tbl(db_connection, dbplyr::in_schema(schema, table_name))
  }
}


get_variable_name <- function(table_name, old_name, recode_table){
  # Reads new variable name
  matches <- recode_table %>%
               dplyr::filter(tabla  == table_name, variable_inicial == old_name)

  check_rows <- matches %>%
                  dplyr::summarise(n = n()) %>%
                  dplyr::pull(n)

  if(check_rows == 0) return(NA)

  matches %>%
    dplyr::pull(variable_final) %>%
    unique()
}


get_varlist <- function(table_name, recode_table){
  # Fetch list of variables to recode
  matches <- recode_table %>%
               dplyr::filter(tabla == table_name)

  check_rows <- matches %>%
                  dplyr::summarise(n = n()) %>%
                  dplyr::pull(n)

  if(check_rows == 0) return(NA)

  matches %>%
    dplyr::pull(variable_inicial) %>%
    unique()
}


join_codes <- function(raw_table, table_name, old_name, new_name, recode_table){
  # Recoding function
  if(is.na(new_name)) return(raw_table)

  name <- eval(old_name)
  var_enquo <- rlang::enquo(name)
  by <- purrr::set_names('valor_inicial', rlang::quo_name(var_enquo))

  filter_recode <- recode_table %>%
                     dplyr::filter(tabla == table_name, variable_inicial == !! var_enquo)
  valores <- filter_recode %>% dplyr::pull(valor_inicial)

  if (is.na(valores[1])){
    old_name_sym <- rlang::sym(old_name)
    old_name_quo <- rlang::quo(!!old_name_sym)
    return_table <- raw_table %>%
                      dplyr::mutate(!!new_name := !!old_name_quo) %>%
                      dplyr::select(-old_name) %>%
                      dplyr::compute()
  } else {
    return_table <- raw_table %>%
                      dplyr::left_join(filter_recode, by = by) %>%
                      dplyr::select(-variable_inicial) %>%
                      dplyr::rename(temp = !!name, !!new_name := valor_final) %>%
                      dplyr::select(-temp, -tabla, -nombre_valor_final, -variable_final) %>%
                      dplyr::compute()
  }
  return(return_table)
}


cast_int <- function(table_name, raw_table_name, db_connection, schema){
    recode_table <- get_table(table_name = 'recode',
                              db_connection = db_connection,
                              schema = 'raw')
    clean_table <- get_table(table_name = table_name,
                             db_connection = db_connection,
                             schema = schema)
    varlist <- get_varlist(raw_table_name, recode_table)

    clean_table %>%
      dplyr::mutate_at(vars(varlist), as.integer)
}


recode_vars <- function(table_name,
                        recode_table_name = 'recode',
                        db_connection = con){

    # Get raw_table_name and schema
    c(raw_table_name, schema) := get_table_name(table_name)

    # Get tables
    recode_table <- get_table(recode_table_name, db_connection, 'raw')
    raw_table <- get_table(table_name, db_connection, schema)
    # Get selected variables
    varlist <- get_varlist(raw_table_name, recode_table)

    if (is.na(varlist[1])) return(raw_table)

    # Separate variables to be recoded in order to simplify computing process
    raw_table <- raw_table %>%
                 dplyr::mutate(temp_id = row_number())
    not_recoded <- raw_table %>%
                    dplyr::select(-one_of(varlist))
    new_vars <- raw_table %>%
                dplyr::select(temp_id)

    for(old_name in varlist){
      # Get new varname from recode_table
      new_name <- get_variable_name(raw_table_name, old_name, recode_table)
      to_recode <- raw_table %>%
                    dplyr::select(one_of(c('temp_id', old_name)))
      # Perform variable recode
      new_var <- join_codes(to_recode, raw_table_name, old_name, new_name, recode_table)
      new_vars <- new_vars %>%
                    dplyr::left_join(new_var) %>%
                    dplyr::compute()
      print(glue::glue('Renamed variable {old_name} to {new_name}'))
    }

    new_table <- not_recoded %>%
                  dplyr::left_join(new_vars)

    new_table
}
