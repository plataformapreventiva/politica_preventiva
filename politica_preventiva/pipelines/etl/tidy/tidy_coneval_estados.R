brary(RPostgreSQL)
library(tidyverse)
library(dbplyr)
library(dplyr)

PGDATABASE="predictivadb"
POSTGRES_PASSWORD="Cd#Q4$bGE#uUyYr"
POSTGRES_USER="maestrosedesol"
PGHOST="predictivadb.cshyqil2j46y.us-west-2.rds.amazonaws.com"
PGPORT=5432

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = PGDATABASE,
                 host = PGHOST, port = PGPORT,
                 user = POSTGRES_USER, password = POSTGRES_PASSWORD)
dbListTables(con)
coneval_ent <- tbl(con, sql("select cve_ent, data_date, pobreza, pobreza_e, pobreza_m, factor,
                            vul_car, vul_ing, no_pobv, ic_rezedu, ic_asalud, ic_segsoc, 
                            ic_cv, ic_sbv, ic_ali, carencias, carencias3, plb, plb_m 
                            from raw.coneval_estados"))

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

key <- "variable"
value <- "valor"
not_gathered <- c("cve_ent", "data_date", "factor")

coneval_larga <- gather_db(coneval_ent, key, value, not_gathered) 

base <- coneval_larga %>% 
  group_by(cve_ent, data_date, variable) %>%
  summarise(num = sum(factor*valor))

pob <- coneval_larga %>%
  select(cve_ent,data_date,factor) %>%
  group_by(cve_ent,data_date) %>%
  summarise(pob_tot = sum(factor))

base <- base %>%
  left_join(pob, by = c("cve_ent", "data_date")) %>%
  mutate(pob_tot = 1.0*pob_tot) %>%
  mutate(porcentaje = num/pob_tot) %>%
  compute() 

base <- base %>% collect()

dbWriteTable(conn = con, c("tidy", "coneval_estados"), value = base,
             overwrite=TRUE, row.names=FALSE)

dbGetQuery(con, "create index on tidy.coneval_estados(cve_ent)")

# commit the change
dbCommit(con)

# disconnect from the database
dbDisconnect(con)

