#!/usr/bin/env Rscript
library(DBI)
library(dbplyr)
library(dplyr)
library(optparse)
library(purrr)
library(rlang)
library(stringr)
library(tidyr)
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


  cuaps_componentes <- tbl(con,  sql('select * from clean.cuaps_componentes')) %>%
                         collect() %>%
                         mutate(c_indic_alimentaria = (indic_a + indic_b) > 0,
                                c_indic_vivienda = (indic_c + indic_d + indic_e + indic_f) > 0,
                                c_indic_servicios = (indic_g + indic_h + indic_i + indic_j) > 0,
                                c_indic_salud = indic_k,
                                c_indic_educacion = (indic_l + indic_m + indic_n) > 0,
                                c_indic_segsocial = (indic_o + indic_p + indic_q) > 0,
                                c_indic_ingreso_lb = indic_r,
                                c_indic_ingreso_lbm = indic_s) %>%
                         mutate_at(vars(starts_with('c_indic')), as.numeric)


  varnames <- c('c_indic', 'tipo_apoyo', 'tipo_pob_apo_cod', 'apoyo_gen_padron')
  plotnames <- c('s06_contribuciones', 's09_tipo_apoyos', 's09_tipo_poblaciones', 's10_padrones')
  subsets <- list('c_indic' = 1,
                  'tipo_apoyo' = 1:7,
                  'tipo_pob_apo_cod' = 1:6,
                  'apoyo_gen_padron' = 1:2)

  names_df <- create_varnames_data(cuaps_componentes, varnames, plotnames, subsets)

  cuaps_componentes_tidy <- map_df(1:nrow(names_df), function(x) tidy_count(data = cuaps_componentes,
                                                     count_var = names_df$varname[x],
                                                     plotname = names_df$plotname[x],
                                                     subset = names_df$subset[[x]]))

  copy_to(con, cuaps_componentes_tidy,
          dbplyr::in_schema('tidy', 'cuaps_componentes'),
          temporary = FALSE, overwrite = TRUE)


  # disconnect from the database
  dbDisconnect(con)
}
