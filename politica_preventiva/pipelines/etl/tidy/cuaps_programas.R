#!/usr/bin/env Rscript
library(DBI)
library(dbplyr)
library(dplyr)
library(optparse)
library(purrr)
library(rlang)
library(stringr)
library(tidyr)
library(RPostgres)
source("pipelines/etl/tools/tidy_tools.R")

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
              help="pipeline task", metavar="character")
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
    host = PGHOST,
    port = PGPORT,
    dbname = PGDATABASE,
    user = POSTGRES_USER,
    password = POSTGRES_PASSWORD
  )

  dependencias_federales = c('06', '08', '10', '11', '12', '14',
                             '15', '16', '20', '48', '50', '1900')

  cuaps_programas <- tbl(con,  sql('select * from clean.cuaps_programas')) %>%
                         collect() %>%
                         mutate(dependencia = filter_values(chr_cve_dependencia,
                                                            dependencias_federales),
                                cve_entidad_federativa = str_pad(cve_entidad_federativa, 2, pad = '0')) %>%
                         rename(cve_ent = cve_entidad_federativa)


  varnames <- c('orden_gob', 'cve_ent', 'dependencia', 'der_social')
  plotnames <- c('s01_orden_gob', 's02_estados', 's03_dependencias-federal', 's04_derechos')
  subsets <- list('orden_gob' = 1:3,
                'cve_ent' = str_pad(as.character(1:32), 2, pad = '0'),
                'dependencia' = dependencias_federales,
                'der_social' = 1)

  names_df <- create_varnames_data(cuaps_programas, varnames, plotnames, subsets) %>%
                filter(varname != 'der_social_ning')

  filtered_names_df <- names_df %>%
                        filter(!(varname %in% c('orden_gob', 'dependencia')), plotname != 's03_dependencias-federal')

  filtered_tidy <- map_df(1:nrow(filtered_names_df), function(x) tidy_count(data = cuaps_programas,
                                                      count_var = filtered_names_df$varname[x],
                                                      plotname = filtered_names_df$plotname[x],
                                                      uncounted = c('actualizacion_sedesol', 'data_date', 'orden_gob'),
                                                      subset = filtered_names_df$subset[[x]])) %>%
                            filter(!(variable %in% c('chr_descripcion_dependencia', 'chr_cve_dependencia')), !is.na(orden_gob), !(plot %in% c('s02_estados-municipal', 's02_estados-federal'))) %>%
                            mutate(plot = glue::glue('{plot}-{recode(orden_gob, "1"="federal", "2"="estatal", "3"="municipal")}')) %>%
                            select(-orden_gob)

  unfiltered_tidy <- map_df(1:nrow(names_df), function(x) tidy_count(data = cuaps_programas,
                                                     count_var = names_df$varname[x],
                                                     plotname = names_df$plotname[x],
                                                     subset = names_df$subset[[x]])) %>%
                            filter((!variable %in% c('chr_descripcion_dependencia', 'chr_cve_dependencia'))) %>%
                            bind_rows(tibble(variable = 'orden_gob',
                                             plot = 's01_orden_gob',
                                             categoria = '0',
                                             valor = nrow(cuaps_programas))) %>%
                            replace_na(list(actualizacion_sedesol = pull_filler(cuaps_programas, 'actualizacion_sedesol'),
                                            data_date = pull_filler(cuaps_programas, 'data_date')))

  cuaps_programas_tidy <- bind_rows(filtered_tidy, unfiltered_tidy) %>%
                            mutate(plot_prefix = gsub('(s.*)(-.*)', '\\1', plot))

  copy_to(con, cuaps_programas_tidy,
          name = dbplyr::in_schema('tidy', 'cuaps_programas'),
          temporary = FALSE, overwrite = TRUE)


  # disconnect from the database
  dbDisconnect(con)
}



