#!/usr/bin/env Rscript
library(DBI)
library(stringr)
library(dbplyr)
library(jsonlite)
library(optparse)
library(tidyverse)
library(stringr)
source("pipelines/semantic/tools/tidy_tools.R")

option_list = list(
  make_option(c("--data_date"), type="character", default="",
              help="data date", metavar="character"),
  make_option(c("--database"), type="character", default="",
              help="database name", metavar="character"),
  make_option(c("--user"), type="character", default="",
              help="database user", metavar="character"),
  make_option(c("--password"), type="character", default="",
              help="password for datbase user", metavar="character"),
  make_option(c("--host"), type="character", default="",
              help="database host name", metavar="character"),
  make_option(c("--pipeline"), type="character", default="",
              help="semantic pipeline task", metavar="character")
);

opt_parser <- OptionParser(option_list=option_list);

opt <- tryCatch(
  {
    parse_args(opt_parser);
  },
  error=function(cond) {
    message("Error: Provide database connection arguments appropriately.")
    message(cond)
    print_help(opt_parser)
    return(NA)
  },
  warning=function(cond) {
    message("Warning:")
    message(cond)
    return(NULL)
  },
  finally={
    message("Finished attempting to parse arguments.")
  }
)

if(length(opt) > 1){

  if (opt$database=="" | opt$user=="" |
      opt$password=="" | opt$host=="" ){
    print_help(opt_parser)
    stop("Database connection arguments are not supplied.n", call.=FALSE)
  }else{
    PGDATABASE <- opt$database
    POSTGRES_PASSWORD <- opt$password
    POSTGRES_USER <- opt$user
    PGHOST <- opt$host
    PGPORT <- "5432"
  }

  con <- DBI::dbConnect(RPostgres::Postgres(),
                        host=PGHOST,
                        port=PGPORT,
                        dbname=PGDATABASE,
                        user=POSTGRES_USER,
                        password=POSTGRES_PASSWORD
  )

  pipeline_task <- opt$pipeline

  query <- glue::glue("DROP TABLE IF EXISTS tidy.{pipeline_task};")
  DBI::dbGetQuery(con, query)

  query_meta <- "SELECT string_agg(
                'SELECT ' ||
                   quote_literal(table_schema) || ' AS table_schema, ' ||
                   quote_literal(table_name) || ' AS table_name, id, nombre ,
                   fuente FROM ' || table_schema || '.' || table_name , ' UNION ')::text
                   FROM information_schema.tables WHERE (table_schema = 'features'
                                                         OR table_schema = 'raw' )
                   AND table_name ~ '_dic$';"
  metadata <- DBI::dbGetQuery(con, query_meta)
  metadata <- sub(" *string_agg *1",'',metadata)
  dict <-  tbl(con, dbplyr::sql(metadata)) %>% collect() %>%
    rename(vars=id) %>% distinct(vars, .keep_all = TRUE)

  dbDisconnect(con)
  con <- DBI::dbConnect(RPostgres::Postgres(),
                        host=PGHOST,
                        port=PGPORT,
                        dbname=PGDATABASE,
                        user=POSTGRES_USER,
                        password=POSTGRES_PASSWORD
  )
  plots_data <- tbl(con, dbplyr::sql(glue::glue("SELECT nivel, plot, plot_type, metadata \
                                               FROM plots.{pipeline_task}"))) %>%
    dplyr::collect() %>% # Los mapas, pub y las series de tiempo se hacen en otro pipeline.
    dplyr::distinct(plot, .keep_all = TRUE)

  plots_metadata <- textConnection(dplyr::pull(plots_data, metadata) %>%
                                     gsub("\\n", "", .)) %>%
    jsonlite::stream_in() %>%
    dplyr::bind_cols(plots_data, .) %>%
    unnest(vars=vars) %>%
    left_join(dict, by=c('vars')) %>%
    select(-table_name.y, table_name=table_name.x) %>%
    rowwise %>%
    mutate(vars_dict = toJSON(list(variable=vars,
                                   name=nombre,
                                   source=fuente),auto_unbox=T)) %>%
    group_by(nivel, plot, plot_type, metadata,
             title, schema, palette, subtext, table_name, section,
             table_schema) %>%
    summarise(vars = paste(vars, collapse=','),
              vars_dict = paste(vars_dict, collapse=','))


  plots_metadata_d <- plots_metadata %>%
    rowwise %>%
    mutate(metadata = toJSON(list(plot_type=plot_type,
                                  vars = vars,
                                  metadata=metadata,
                                  #dictionary=vars_dict,
                                  title=title,
                                  palette=palette,
                                  subtext=subtext),auto_unbox=T)) %>%
    dplyr::select(plot, metadata)

  tidy_data <- tibble::tibble()
  concat <- tibble::tibble()

  con <- DBI::dbConnect(RPostgres::Postgres(),
    host=PGHOST,
    port=PGPORT,
    dbname=PGDATABASE,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD
  )

  # Group by into clave (one row per mun/ent)
  data <-  tbl(con, dbplyr::sql(glue::glue("SELECT * FROM temp_perfil_ubicaciones_t2"))) %>%
      collect()

  data <- data %>% left_join(dict, by=c('variable'='vars')) %>%
      drop_na(variable) %>%
      rowwise %>%
      dplyr::mutate(values = str_c('"',variable,'":{"values":"',as.character(values),
                                '","name":"',as.character(nombre),
                                '","source":"',as.character(fuente),'"}'),
               plot = plots_metadata$plot[i]) %>%
      dplyr::group_by(nivel, nivel_clave, plot) %>%
      dplyr::summarise(values=paste(values, collapse=', ')) %>%
      dplyr::left_join(plots_metadata_d) %>%
      drop_na(values) %>%
      rowwise %>%
      dplyr::mutate(values = str_c('"',plot,'":{"values":{',values,
                                    '},"info":',metadata,'}')) %>%
      dplyr::select(-metadata) %>%
      dplyr::group_by(nivel, nivel_clave) %>%
      dplyr::summarise(values = str_flatten(values, collapse=', ')) %>%
      dplyr::mutate(values = paste0("{",values,'}'))

  copy_to(con, data,
          dbplyr::in_schema("tidy","perfil_ubicaciones"),
          temporary = FALSE, overwrite = TRUE)
  query <- c("DROP TABLE temp_perfil_ubicaciones_t2;")

  # DBI::dbGetQuery(con, query)
  print('Closed connection')
  dbDisconnect(con)
}
