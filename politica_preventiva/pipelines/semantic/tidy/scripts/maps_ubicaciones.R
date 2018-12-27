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
              help="semantic pipeline task", metavar="character"),
  make_option(c("--extra_parameters"), type="character", default="",
              help="Extra parameters", metavar="character")
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

  query <- c("DROP TABLE IF EXISTS tidy.maps_ubicaciones;")
  query_2 <- c("CREATE TABLE IF NOT EXISTS temp_maps_ubicaciones_t2 (
                    nivel TEXT,
                    nivel_clave TEXT,
                    plot TEXT,
                    values TEXT,
                    metadata TEXT);")
  DBI::dbGetQuery(con, query)
  DBI::dbGetQuery(con, query_2)

  dbDisconnect(con)
  for(level in as.list(strsplit(opt$extra_parameters, ",")[[1]])){
    print(glue::glue('Level: {level}'))
    if (level=='m'){
      key_var <- 'cve_muni'
      n <- 5
    } else{
      key_var <- 'cve_ent'
      n <-2
    }
    con <- DBI::dbConnect(RPostgres::Postgres(),
      host=PGHOST,
      port=PGPORT,
      dbname=PGDATABASE,
      user=POSTGRES_USER,
      password=POSTGRES_PASSWORD
    )

    # Create SQL queries programmatically using plots data
    plots_data <- tbl(con, dbplyr::sql(glue::glue("SELECT plot, plot_type, metadata \
                                                   FROM plots.{pipeline_task} \
                                                   WHERE nivel = '{level}' \
                                                   AND plot_type = 'map'"))) %>%
                  dplyr::collect()

    plots_metadata <- textConnection(dplyr::pull(plots_data, metadata) %>%
                                     gsub("\\n", "", .)) %>%
                      jsonlite::stream_in() %>%
                      dplyr::bind_cols(plots_data, .) %>%
                      dplyr::mutate(extra_grouping_var = gsub(key_var, '', grouping_var))

    plots_metadata_d <- plots_metadata %>%
                        dplyr::select(-extra_grouping_var) %>%
                        dplyr::rowwise() %>%
                        dplyr::mutate(metadata = toJSON(list(plot_type=plot_type,
                                                             metadata=metadata),
                                                        auto_unbox=T)) %>%
                        dplyr::select(plot, metadata)

    sql_queries <- plots_metadata %>%
                   dplyr::rowwise() %>%
                   dplyr::mutate(var_list = paste0(vars, collapse=', '),
                                 query_vars = paste0(c(extra_grouping_var,
                                                       key_var, var_list),
                                                     collapse=', '),
                                 query = glue::glue("SELECT {gsub('^, ', '', query_vars)} ",
                                                    "FROM {schema}.{table_name}")) %>%
                   dplyr::pull(query)

    for(i in 1:nrow(plots_metadata)){
        con <- DBI::dbConnect(RPostgres::Postgres(),
                              host=PGHOST,
                              port=PGPORT,
                              dbname=PGDATABASE,
                              user=POSTGRES_USER,
                              password=POSTGRES_PASSWORD
        )

        query <- sql_queries[i]
        print(glue::glue('Query number {i}'))
        print(query)
        print('Opened connection')
        data <- tbl(con, dbplyr::sql(query)) %>% dplyr::collect()

        extra_key <- plots_metadata$grouping_var[i]
        # Define key that links to profile
        key_var_name <- rlang::sym(key_var)
        key_var_quo <- rlang::quo(!! key_var_name)
        if (level == 'm' & extra_key == 'cve_ent'){
            # IN this case, the extra key (cve_ent) is a coarser aggregation level than
            # the original key_var (cve_muni), so we need to exchange them in
            # order to always have the coarser-level rows "containing" the finer-level
            # elements in a logical way
            grouping_var_name <- rlang::sym(extra_key)
            id_var_name <- key_var_name
        } else {
            grouping_var_name <- key_var_name
            id_var_name <- rlang::sym(extra_key)
        }
        grouping_var_quo <- rlang::quo(!! grouping_var_name)
        id_var_quo <- rlang::quo(!! id_var_name)

        not_gathered <- c(key_var, extra_key) %>% unique()
        data_largo <- tidyr::gather(data, variable, valor, -one_of(not_gathered))

        elements_data <- data_largo %>%
                         dplyr::mutate(nivel_clave = str_pad(!! key_var_quo, n, "left", '0')) %>%
                         dplyr::group_by_at(vars(one_of(not_gathered))) %>%
                         dplyr::arrange(variable) %>%
                         dplyr::mutate(element_id = !! id_var_quo) %>%
                         dplyr::ungroup() %>%
                         dplyr::rowwise() %>%
  # Create dictionary structure for each variable, valor pair
                         dplyr::mutate(values = str_c('"', variable, '":{"valor":"', valor,'"}'),
                                       nivel = level,
                                       plot = plots_metadata$plot[i]) %>%
                         tidyr::drop_na(valor) %>%
                         dplyr::group_by(nivel, nivel_clave, plot, element_id, !! grouping_var_quo) %>%
  # Collapse all variable-level dictionaries into one row
                         dplyr::summarise(values = paste(values, collapse=',')) %>%
  # Assign all those variable-value dictionaries to one element id (it makes sense to have more than one
  # var-value pair assigned to the same element_id, because our map tooltips contain several extra vars)
                         dplyr::mutate(values = paste0('"', element_id, '":{', values, '}'))
  # Aggregate all plot data for that municipio/estado
        if(extra_key == 'cve_ent' & level == 'm') {
            # In this case, we need to repeat values for all municipios of the
            # same state
            aggregate_data <- elements_data %>%
                              dplyr::group_by(!! grouping_var_quo) %>%
                              dplyr::summarise(values = paste(values, collapse=','))
            plot_data <- elements_data %>%
                            dplyr::select(-values) %>%
                            dplyr::left_join(aggregate_data) %>%
                            dplyr::ungroup() %>%
                            dplyr::select(nivel, nivel_clave, plot, values)
        } else {
            plot_data <- elements_data %>%
                            dplyr::group_by(nivel, nivel_clave, plot) %>%
                            dplyr::summarise(values = paste(values, collapse=','))
        }
        profile_data <- plot_data %>%
                        dplyr::left_join(plots_metadata_d) %>%
                        dplyr::mutate(values = str_c('"', plot, '":{"values":{', values,
                                                     '},"info":', metadata, '}')) %>%
                        dplyr::select(-metadata)

        RPostgres::dbWriteTable(conn=con,
                                name='temp_maps_ubicaciones',
                                value=profile_data, temporary=TRUE,
                                overwrite=T, row.names=FALSE)
        query <- c("INSERT INTO temp_maps_ubicaciones_t2
                   SELECT * from temp_maps_ubicaciones;")
        DBI::dbGetQuery(con, query)
        # Clean and disconnect
        gc()
        dbDisconnect(con)
   }
  }

  con <- DBI::dbConnect(RPostgres::Postgres(),
    host=PGHOST,
    port=PGPORT,
    dbname=PGDATABASE,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD
  )

  # Group by into clave (one row per mun/ent)
  tbl(con, dbplyr::sql(glue::glue("SELECT * FROM temp_maps_ubicaciones_t2"))) %>%
      dplyr::group_by(nivel, nivel_clave) %>%
      dplyr::summarise(values = str_flatten(values, collapse=', ')) %>%
      dplyr::mutate(values = paste0('{',values,'}')) %>%
      compute(name= in_schema("tidy", "maps_ubicaciones"), temporary=F)

  query <- c("DROP TABLE temp_maps_ubicaciones_t2;")
  DBI::dbGetQuery(con, query)
  print('Closed connection')
  dbDisconnect(con)
}

