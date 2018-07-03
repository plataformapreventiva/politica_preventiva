#!/usr/bin/env Rscript
library(optparse)
library(dbplyr)
library(rlang)
library(glue)
library(dplyr)
library(purrr)
library(stringr)
library(tidyr)
library(DBI)
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

  # Auxiliary subsetting helpers

  criterios_grupos_vulnerables <- list('36' = 'r_1',
                                    '1754' = 'r_1',
                                    '1755' = 'r_1',
                                    '1756' = 'r_1',
                                    '1757' = 'r_1',
                                    '37' = 'r_6',
                                    '38' = 'r_2',
                                    '39' = 'r_4',
                                    '40' = 'r_5',
                                    '41' = 'r_3',
                                    '42' = 'r_8',
                                    '167' = 'r_8',
                                    '168' = 'r_8',
                                    '169' = 'r_8',
                                    '43' = 'r_7',
                                    '170' = 'r_7',
                                    '171' = 'r_7',
                                    '172' = 'r_7',
                                    '173' = 'r_7',
                                    '174' = 'r_7',
                                    '175' = 'r_7',
                                    '176' = 'r_7',
                                    '44' = 'r_9')
  criterios_pobreza <- 31:35
  criterios_carencias <- list('19' = 'r_lb',
                              '20' = 'r_lbm',
                              '1758' = 'r_alim',
                              '1759' = 'r_alim',
                              '1760' = 'r_alim',
                              '1761' = 'r_viv',
                              '1762' = 'r_viv',
                              '1763' = 'r_viv',
                              '1764' = 'r_viv',
                              '1765' = 'r_viv',
                              '1766' = 'r_serv',
                              '1767' = 'r_serv',
                              '1768' = 'r_serv',
                              '1769' = 'r_serv',
                              '1770' = 'r_serv',
                              '1771' = 'r_salud',
                              '1772' = 'r_edu',
                              '1773' = 'r_edu',
                              '1774' = 'r_edu',
                              '1775' = 'r_edu',
                              '1776' = 'r_segsoc',
                              '1777' = 'r_segsoc',
                              '1778' = 'r_segsoc',
                              '1779' = 'r_segsoc',
                              '1780' = 'r_segsoc')


  criterios_localidades <- 130:133
  criterios_zap <- 158:160
  criterios_marginacion_mun <- 134:139
  criterios_marginacion_loc <- 140:145
  criterios_territorios <- 22:29

  # Fetch data
  orden_gob_df <- tbl(con, sql('select cuaps_folio, orden_gob from clean.cuaps_programas'))

  cuaps_criterios <- tbl(con, sql("select * from clean.cuaps_criterios")) %>%
      collect()

  cuaps_criterios <- cuaps_criterios %>%
                        mutate(atiende_grupos_vulnerables = recode(filter_values(csc_configuracion_foc,
                                                                                 names(criterios_grupos_vulnerables)),
                                                                   !!! criterios_grupos_vulnerables),
                               atiende_pobreza = filter_values(csc_configuracion_foc,
                                                               criterios_pobreza),
                               atiende_carencias = recode(filter_values(csc_configuracion_foc,
                                                                 names(criterios_carencias)),
                                                          !!! criterios_carencias),
                               atiende_territorios = filter_values(padre,
                                                                   criterios_territorios)) %>%
                      left_join(orden_gob_df, copy = TRUE)


  varnames <- c('atiende_grupos_vulnerables', 'atiende_pobreza',
                          'atiende_carencias', 'atiende_territorios')

  plotnames <- c('s09_grupos_vulnerables', 's08_pobreza', 's10_carencias', 's11_1_territorios')

  subsets <- list('atiende_grupos_vulnerables' = unique(unlist(criterios_grupos_vulnerables)),
                  'atiende_pobreza' = criterios_pobreza,
                  'atiende_carencias' = unique(unlist(criterios_carencias)),
                  'atiende_territorios' = criterios_territorios)

  names_df <- create_varnames_data(cuaps_criterios, varnames, plotnames, subsets)

  filtered_tidy <- map_df(1:nrow(names_df), function(x) tidy_count(data = cuaps_criterios,
                                                                  count_var = names_df$varname[x],
                                                                  plotname = names_df$plotname[x],
                                                                  uncounted = c('actualizacion_sedesol', 'data_date', 'orden_gob'),
                                                                  subset = names_df$subset[[x]])) %>%
  filter(!is.na(orden_gob)) %>%
  mutate(plot = glue::glue('{plot}-{recode(orden_gob, "1"="federal", "2"="estatal", "3"="municipal")}')) %>%
  select(-orden_gob)

  unfiltered_tidy <- map_df(1:nrow(names_df), function(x) tidy_count(data = cuaps_criterios,
                                                                          count_var = names_df$varname[x],
                                                                          plotname = names_df$plotname[x],
                                                                          subset = names_df$subset[[x]]))

  cuaps_criterios_tidy <- bind_rows(filtered_tidy, unfiltered_tidy) %>%
                            mutate(plot_prefix = gsub('(s.*)(-.*)', '\\1', plot))

  copy_to(con, cuaps_criterios_tidy,
          dbplyr::in_schema('tidy', 'cuaps_criterios'),
          temporary = FALSE, overwrite = TRUE)


  # disconnect from the database
  dbDisconnect(con)
}
