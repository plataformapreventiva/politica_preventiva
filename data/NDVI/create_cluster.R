## CREATE CLUSTER based on superficie sembrada

library(tidyverse)
library(ggplot2)
library(rgdal)
rm(list=ls())

## Read and transform produccion agricola
colClasses <- c(rep("character", 2), rep("numeric", 8), rep("character", 3))
avance <- read.csv('data/cierre_produccion.csv', colClasses = colClasses)
avance <- filter(avance, sup_sembrada > 0)
produccion <- avance %>%
  mutate(CVE_MUN = paste0(estado, municipio)) %>%
  select(CVE_MUN, anio, sup_sembrada, produccion) %>%
  filter(anio %in% c(2014, 2015))
produccion$rendim <- produccion$produccion/produccion$sup_sembrada
produccion <- produccion[!duplicated(produccion[,c('CVE_MUN', 'anio')]),]
produccion$rendim <- scale(produccion$rendim)

produccion <- produccion %>%
  select(CVE_MUN, rendim) %>%
  group_by(CVE_MUN) %>%
  summarise(rendim = mean(rendim, na.rm=T))

# Read and transpoform NDVI info
colClasses <- c(rep("character",2), "numeric", rep("character", 3))
ndvi <- read.csv('data/NDVI/municip_wide_ndvi.csv', colClasses = colClasses) %>%
  select(CVE_MUN, value) %>% group_by(CVE_MUN) %>% summarise(value = mean(value, na.rm=T)) 

produccion <- merge(produccion, ndvi, by='CVE_MUN')

# Obtain centroids info from shp file
#path.in.shp<-"data/NDVI/cluster/Centroids.shp"
#shp <-readOGR(path.in.shp, layer= sub("^([^.]*).*", "\\1", basename(path.in.shp)))
#rows <- shp@data$CVE_MUN
#coords <- shp@coords #%>% scale()
#coords <- data.frame(CVE_MUN = rows, coords)
#write.csv(coords, 'data/NDVI/cluster/centroids.csv', row.names=F)
coords <- read.csv('data/NDVI/cluster/centroids.csv')
coords <- merge(coords, produccion, by='CVE_MUN')

# Create clusters
clusts <- kmeans(coords[,-1], centers = 60, iter.max = 50)
#plot(coords[,c(2,3)], col = clusts$cluster)
#clusts$size
clusts <- data.frame('CVE_MUN' = coords$CVE_MUN, 'Clust'= clusts$cluster)

write.csv(clusts, 'data/NDVI/cluster/clusters_produccion.csv', row.names = F)
