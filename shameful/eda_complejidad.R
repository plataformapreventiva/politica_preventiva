rm(list=ls())
suppressPackageStartupMessages({
  library(tidyverse)
  library(stringr)
  source("utils_shameful.R")
  library(mice)
  library('RPostgreSQL')
}


##################
## Create Connection to DB
##################
conf <- fromJSON("../conf/conf_profile.json")
pg = dbDriver("PostgreSQL")
con = dbConnect(pg, user=conf$PGUSER, password=conf$PGPASSWORD,
                host=conf$PGHOST, port=5432, dbname=conf$PGDATABASE)



### ECI Economic Complexity Index 
# complexity of a country's exports. 

# the ECI is defined in terms of an eigenvector of a matrix connecting countries to countries, 
# which is a projection of the matrix connecting countries to the products they export. Since 
# the ECI considers information on the diversity of countries and the ubiquity of products, it 
# is able to produce a measure of economic complexity containing information about both the 
# diversity of a country's export and their sophistication. For example, Japan or Germany, with
# high ECIs, export many goods that are of low ubiquity and that are produced by highly diversified
# countries, indicating that these are diverse and sophisticated economies. Countries with low ECI, 
# like Angola or Zambia, export only a few products, which are of relatively high ubiquity and which 
# are exported by countries that are not necessarily very diversified, indicating that these are 
# countries that have little diversity and that the products that they export are not very sophisticated.

### PCI product complexity index

# Subir a Postgres las bases de datos de industria y productos

#estado
temp_complejidad<- as_tibble(read.csv("../data/complejidad/industries_department.csv",nrows =100 ))

# Producción interna, número de industrias y valor
temp_industries<- as_tibble(read.csv("../data/complejidad/industries_municipality.csv",nrows =100 )) 
temp_products<- as_tibble(read.csv("../data/complejidad/products_municipality.csv",nrows =100 )) 
rcpy<- as_tibble(read.csv("../data/complejidad/products_rcpy_municipality.csv",nrows =100 )) 


# Importación y exportación de productos y complejidad productiva
#valor/n_productos
temp_products<- as_tibble(read.csv("../data/complejidad/products_municipality.csv",nrows =100 )) 

# PCI
temp<- as_tibble(read.csv("../data/complejidad/products_rcpy_municipality.csv",nrows =100 )) 

rcp <- as_tibble(read.csv("../data/complejidad/products_rcpy_municipality.csv")) %>% count(country_name)
rcp_temp <- as_tibble(read.csv("../data/complejidad/products_rcpy_municipality.csv",nrows = 100))



#######################################################################################################################################

# Diccionario Edos
estados_dic<- read_csv("../data/utils/estados_dic.csv") %>%
  rename(nom_ent=NOM_ENT, cve_ent = CVE_ENT) %>% 
  mutate(nom_ent = str_to_lower(nom_ent)) %>%
  mutate(cve_ent = str_pad(cve_ent, 2, pad = "0"))

# Municipal
# load complejidad datasets
as_tibble(read.csv("../data/complejidad/products_rcpy_municipality.csv")) %>% group_by(location_code,location_name,year) %>% 
  summarise(media=mean(pci)) %>% 
  spread(key = year,value = media) %>% write.csv("pci_municipality.csv",row.names=FALSE) 

mun <- read_csv("pci_municipality.csv") %>%
  rename(cve_muni = location_code,nom_muni = location_name, complejidad_2004 = `2004`, complejidad_2005 = `2005`,complejidad_2006 = `2006`,
         complejidad_2007 = `2007`,complejidad_2008 = `2008`,complejidad_2009 = `2009`,
         complejidad_2010 = `2010`,complejidad_2011 = `2011`,complejidad_2012 = `2012`,
         complejidad_2013 = `2013`,complejidad_2014 = `2014`) %>% 
  mutate(cve_muni = str_pad(cve_muni, width=5, pad="0")) %>%
  mutate(cve_ent = str_extract(cve_muni,"^[0-9]{2}"))

summary(mun)

# impute missing values
md.pattern(mun)
tempData <- mice(mun,m=5,maxit=50,meth='pmm',seed=500)
summary(tempData)
completedData <- complete(tempData,1)

mun_complejidad <- completedData %>% mutate_each(funs(normalize), starts_with("complejidad"))

complejidad_dict <- read_csv("../data/complejidad/complejidad_municipios_dic.csv")


dbWriteTable(con, c("raw",'complejidad_municipios'),mun_complejidad, row.names=FALSE)
dbWriteTable(con, c("raw",'complejidad_municipios_dic'),complejidad_dict, row.names=FALSE)




# Estatal
temp<-as_tibble(read.csv("../data/complejidad/products_department.csv",nrows = 100))

as_tibble(read.csv("../data/complejidad/products_department.csv")) %>% 
  group_by(year,location_code,location_name) %>% 
  summarise(media = mean(eci)) %>% 
  spread(key = year,value = media) %>% 
  write.csv("pci_estados.csv",row.names=FALSE) 

ent <- read_csv("pci_estados.csv") %>%
  rename(cve_ent = location_code,nom_ent = location_name, complejidad_2004 = `2004`, complejidad_2005 = `2005`,complejidad_2006 = `2006`,
         complejidad_2007 = `2007`,complejidad_2008 = `2008`,complejidad_2009 = `2009`,
         complejidad_2010 = `2010`,complejidad_2011 = `2011`,complejidad_2012 = `2012`,
         complejidad_2013 = `2013`,complejidad_2014 = `2014`) %>% 
  mutate(cve_ent = str_pad(cve_ent, width=2, pad="0")) 

ggplot(ent,aes(complejidad_2014)) + geom_histogram()

summary(ent)

# impute missing values
md.pattern(ent)
tempData <- mice(ent,m=5,maxit=50,meth='pmm',seed=500)
summary(tempData)
completedData <- complete(tempData,1)

ent_complejidad <- completedData %>% mutate_each(funs(normalize), starts_with("complejidad"))
#ggplot(ent_complejidad,aes(complejidad_2010)) + geom_histogram()
complejidad_dict <- read_csv("../data/complejidad/complejidad_municipios_dic.csv")


dbWriteTable(con, c("raw",'complejidad_estados'),ent_complejidad, row.names=FALSE)
dbWriteTable(con, c("raw",'complejidad_dic'),complejidad_dict, row.names=FALSE)


