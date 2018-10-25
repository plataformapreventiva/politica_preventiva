#!/usr/bin/env Rscript
library(optparse)
library(dbplyr)
library(dplyr)
library(DBI)
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
  
  query1 <- 'SELECT SUBSTRING(LPAD(folioviv::text, 10, \'0\'), 1, 2) as ent,
            folioviv, foliohog,numren, sexo, edad, disc1, disc2, disc3, disc4,
            disc5, disc6, disc7, hor_4, min_4, usotiempo4, COUNT(*) as personas_hogar
            FROM clean.enigh_poblacion
            WHERE data_date like \'2016-a\'
            GROUP BY folioviv, foliohog, numren, sexo, edad, disc1, disc2, disc3, disc4,
            disc5, disc6, disc7, hor_4, min_4, usotiempo4'
  
  query2 <- 'SELECT SUBSTRING(LPAD(folioviv::text, 10, \'0\'), 1, 2) as ent,
            folioviv, foliohog,tot_integ, cast(factor_hog as integer), data_date
            FROM clean.enigh_concentrados
            WHERE data_date like \'2016-a\''
  
  #Total de habitantes por estado
  poblacion <- tbl(con, sql(query1)) 
  concentrados <- tbl(con, sql(query2))
  
  pob_ent <- left_join(poblacion,concentrados, by=c("ent","folioviv", "foliohog")) %>% 
      select(ent, folioviv, factor_hog, personas_hogar) %>% 
      mutate(personas = as.numeric(factor_hog)*personas_hogar) %>% 
      select(ent, personas) %>% 
      group_by(ent) %>% 
      summarise_all(sum)

  #Población discapacitados
  
  poblacion_disc <- poblacion %>% 
    select(folioviv,foliohog,ent,disc1,disc2,disc3,disc4,disc5,disc6,disc7) %>% 
    mutate(disc_int = ifelse(disc1 ==6 | disc2 ==6 | disc3 ==6 | disc4 ==6 | disc5 ==6| 
                               disc6 ==6 | disc7 ==6, 1, 0),
           disc_fis = ifelse(disc1 % in %c(1,5)  | disc2 % in %c(1,5) | disc3% in %c(1,5)  | 
                               disc4 % in %c(1,5)  | disc5 % in %c(1,5)  | disc6 % in % c(1,5) , 1, 0),
           disc_sens = ifelse(disc1 % in %c(2:4) | disc2 % in %c(2:4) | disc3 % in %c(2:4) | 
                                disc4 % in %c(2:4) | disc5 % in %c(2:4) | disc6 % in %c(2:4) | disc7 % in %c(2:4), 1, 0),
           disc_men = ifelse(disc1 ==7 | disc2 ==7 | disc3 ==7 | disc4 ==7 | disc5 ==7 | 
                               disc6 ==7 | disc7 ==7, 1, 0)) %>% 
    select(folioviv,foliohog,ent, disc_int, disc_fis, disc_sens, disc_men) %>% 
    group_by(folioviv, foliohog,ent) %>% 
    summarise_all(sum) %>% left_join(concentrados, by=c("ent","folioviv", "foliohog")) %>% 
    select(ent, folioviv, foliohog, factor_hog, disc_int, disc_fis, disc_sens, disc_men) %>%
    mutate(disc_inte = disc_int*factor_hog,
           disc_fisi = disc_fis*factor_hog,
           disc_senso = disc_sens*factor_hog,
           disc_ment = disc_men*factor_hog) %>% 
    select(ent, disc_inte, disc_fisi, disc_senso, disc_ment) %>% group_by(ent) %>% 
    summarise_all(sum) %>% left_join(pob_ent,by='ent') %>% 
    mutate(disc_int = 100*disc_inte/personas,
           disc_fis = 100*disc_fisi/personas,
           disc_sens = 100*disc_senso/personas,
           disc_men = 100*disc_ment/personas) %>% 
    select(ent, disc_int, disc_fis, disc_sens, disc_men)
  
#Acceso a guarderías
  
 query3 <- 'SELECT SUBSTRING(LPAD(folioviv::text, 10, \'0\'), 1, 2) as ent,
            folioviv, foliohog, numren, pres_6
            FROM clean.enigh_trabajos
            WHERE data_date like \'2016-a\''
  
 guarderias <- tbl(con, sql(query3)) %>%
    mutate(ac_guar = ifelse(pres_6==6, 1, 0)) %>%
    group_by(ent,folioviv, foliohog) %>% 
    summarise(personas_guar_hogar =n()) %>%
    left_join(concentrados,by=c('ent','folioviv','foliohog')) %>% 
    select(ent,foliohog, folioviv, factor_hog, personas_guar_hogar)  %>% 
    mutate(tot_hog_guar = factor_hog*foliohog) %>%  
    select(ent, tot_hog_guar) %>% 
    group_by(ent) %>% summarise_all(sum)%>% left_join(pob_ent,by='ent') %>% 
   mutate(proporcion_guar=tot_hog_guar/personas) %>% 
   select(ent, proporcion_guar)
  
#Brecha salarial
  
 query4 <- 'SELECT SUBSTRING(LPAD(folioviv::text, 10, \'0\'), 1, 2) as ent,
            folioviv, foliohog, numren, clave, ing_1
            FROM clean.enigh_ingresos
            WHERE data_date like \'2016-a\''

 ingresos_m <- tbl(con, sql(query4))  %>%
   filter(clave == "P001"|clave =="P011"|clave =="P018") %>% 
   select(ent, foliohog, folioviv, ing_1) %>%
   left_join(poblacion,by=c('ent','foliohog','folioviv')) %>%
   filter(sexo == 2) %>% select(ent, foliohog, folioviv, ing_1) %>% group_by(ent) %>% 
   summarise(media_salario_m = mean(ing_1, na.rm = T))
  
 ingresos_h <- tbl(con, sql(query4))  %>%
   filter(clave == "P001"|clave =="P011"|clave =="P018") %>% 
   select(ent, foliohog, folioviv, ing_1) %>%
   left_join(poblacion,by=c('ent','foliohog','folioviv')) %>%
   filter(sexo == 1) %>% select(ent, foliohog, folioviv, ing_1) %>% group_by(ent) %>% 
   summarise(media_salario_h = mean(ing_1, na.rm = T))
 
  brecha_salarial <- left_join(ingresos_m,ingresos_h, by = c("ent")) %>% 
    mutate(brecha_salario_mensual = media_salario_m/media_salario_h) %>% 
    select(ent,brecha_salario_mensual)
  
enigh_vulnerabilidades <- left_join(poblacion_disc,guarderias,by='ent') %>%
  left_join(brecha_salarial,by='ent') %>%
  dplyr::mutate(data_date = data_date,
                  actualizacion_sedesol = lubridate::today())
  
  copy_to(con, enigh_vulnerabilidades,
          dbplyr::in_schema("features",'enigh_vulnerabilidades'),
          temporary = FALSE, overwrite = TRUE)
  dbDisconnect(con)
  
  print('Features written to: features.enigh_vulnerabilidades')
}
