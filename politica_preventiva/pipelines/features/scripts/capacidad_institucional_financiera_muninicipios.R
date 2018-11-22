#!/usr/bin/env Rscript
library(optparse)
library(dbplyr)
library(dplyr)
library(DBI)
library(lubridate)
library(yaml)

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
              help="database host name", metavar="character")
)

opt_parser <- OptionParser(option_list=option_list)

opt <- tryCatch(
  {
    parse_args(opt_parser)
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
  
  if(opt$data_date == ""){
    stop("Did not receive a valid data date, stopping", call.=FALSE)
  }else{
    data_date <- opt$data_date
  }
  
  con <- DBI::dbConnect(RPostgres::Postgres(),
                        host = PGHOST,
                        port = PGPORT,
                        dbname = PGDATABASE,
                        user = POSTGRES_USER,
                        password = POSTGRES_PASSWORD
  )
  
  source("pipelines/features/tools/features_tools.R")
  
  print('Pulling datasets')
  
  data <- tbl(con, dbplyr::in_schema('features','inform_variables_municipios')) %>% retrieve_result()
  estructura <- read_yaml("data/estructura_indice.yaml") # preguntar de dónde leerlo
  
  #----------------------------------------------------------------------------------------
  
  # LIMPIEZA CATEGÓRICAS
  
  # Categóricas -> Numéricas
  # Clasifica las categóricas por tipo
  # Como ya están categorizadas las volvemos numéricas
  
  categoricas_5 <- get_var_from_type(estructura,"categorica_5")
  data[,categoricas_5]  <- data[,categoricas_5] %>% 
    mutate_all(funs(recode(var = ., 
                           recodes = "'Muy bajo'=0;
                           'Bajo'=25;
                           'Medio'=50;'Alto'=75;
                           'Muy alto'=100")))
  
  #Existencia de unidad de protección civil
  #{0-NA|1-si|2-no|3-en proceso de integracion|8-info no disponible|9-no se sabe}
  #categoricas_nd_spnn1 <- get_var_from_type(estructura,"categoricas_nd_spnn1")
  #data[,categoricas_nd_spnn1]  <- data[,categoricas_nd_spnn1] %>% 
  # mutate_all(funs(recode(var = ., 
  #                       recodes = "0=0;
  #                      1=0;2=100;
  #                       3=25;8=50;
  #                       9=50")))
  
  #Existencia de Atlas de Riesgo
  #Participacion ciudadana
  #{0-NA|1-si|2-no|8-info no disponible|9-no se sabe}
  categoricas_nd_nsnn1y4 <- get_var_from_type(estructura,"categorica_nd_nsnn1y4")
  data[,categoricas_nd_nsnn1y4]  <- data[,categoricas_nd_nsnn1y4] %>% 
    mutate_all(funs(recode(var = ., 
                           recodes = "0=0;
                           1=0;2=100;
                           8=50;9=50")))
  
  # Cobro predial
  # {0-NA|1-gobierno municipal|2-gobierno de la Entidad Federativa|3-No se cobra|9-No se sabe}
  categoricas_aut_cobr <- get_var_from_type(estructura,"categorica_aut_cobr")
  data[,categoricas_aut_cobr]  <- data[,categoricas_aut_cobr] %>% 
    mutate_all(funs(recode(var = ., 
                           recodes = "0=0;
                           1=0;2=25;
                           8=100;9=50")))
  
  #Info publica disponible
  
  #Mecanismos de transparencia
  #data <- data %>% 
  #  mutate(mec_tran = ifelse(mec_tran!=0,100,0))
  
  #----------------------------------------------------------------------------------------
  
  # LIMPIEZA NUMÉRICAS
  # Define las variables numéricas (Obtener de yaml)
  numericas <- get_var_from_type(estructura,"^numerica$")
  data[,numericas] <- lapply(data[, numericas], as.numeric) 
  
  # Queremos que inform siempre sea más es peor
  # Acá define las variables donde más sea mejor
  mas_mejor <- get_var_from_type(estructura,"^mas_mejor$")
  data[,mas_mejor] <- data[,mas_mejor]*-1  
  
  # Crear cortes
  # kmeans (por ahora)
  lapply(numericas, get_cuts, data=data)
  
  amenazas <- get_var_from_name(estructura, "Amenazas")
  vulnerabilidad <- get_var_from_name(estructura, "Vulnerabilidad")
  capacidades <- get_var_from_name(estructura, "Capacidades")
  data <- to_numeric(data, amenazas) 
  data <- to_numeric(data, vulnerabilidad)
  data <- to_numeric(data, capacidades) 
  
  #----------------------------------------------------------------------------------------
  
  # AGREGADO DE DIMENSIÓN
  # TODO() - recursive function
  # Try other aggregate functions
  dimensiones <- names(estructura$Inform$Dimensión)
  
  for (dimension in dimensiones) {
    subdimensiones <- estructura$Inform$Dimensión[[eval(dimension)]] %>% 
      purrr::flatten() %>% names()
    for (subdimension in subdimensiones) {
      Subsubdimensiones <- estructura$Inform$Dimensión[[
        eval(dimension)]]$Subdimensión[[eval(subdimension)]] %>% 
        purrr::flatten() %>% names()
      for (Subsubdimensión in Subsubdimensiones) {
        variables <- get_var_from_name(estructura,Subsubdimensión)
        data[Subsubdimensión] <- data[variables] %>% as.data.frame() %>% 
          rowMeans(na.rm = TRUE)
      }
      data[subdimension] <- data[Subsubdimensiones] %>% as.data.frame() %>% 
        rowMeans(na.rm = TRUE)
    }
    data[dimension] <- data[subdimensiones] %>% as.data.frame() %>% 
      rowMeans(na.rm = TRUE)
  }
  
  data["INFORM"] <-apply(data[dimensiones], 1,geometric.mean,na.rm=TRUE)
  capacidades <- data %>% dplyr::select(cve_muni,
                                        `2.1.1 Gestión de Riesgo de Desastre`,
                                        `2.1.3 Reglamentación Básica`,
                                        `2.2.1 Finanzas Públicas`,
                                        `2.2.2 Capacidad Económica`)
  
  # Acceso a la justicia -INEGI-nivel edo
  # Gobernabilidad -IMCO-nivel edo, participación social-arreglar clean
  # Cobertura Institucional - No hay fuente
  # Administrativa / operativa -ICI-nivel edo
  
  copy_to(con, capacidades,
          dbplyr::in_schema("features",'capacidad_institucional_financiera_municipios'),
          temporary = FALSE, overwrite = TRUE)
  dbDisconnect(con)
  
  print('Features written to: features.capacidad_institucional_financiera_municipios')
}