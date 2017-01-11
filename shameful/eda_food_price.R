rm(list=ls())
suppressPackageStartupMessages({
  library(ggplot2)
  library(scales) # for "comma"
  library(tidyverse)
  library(lubridate)
  library(stringr)
  library('RPostgreSQL')
  library(sp)
  library(automap)
  library(maptools)
  library(rgdal)
  #library(proj4)
  #library(gstat)
  #library(raster)
  })

##################
## Load municipality lat/long
##################

pg = dbDriver("PostgreSQL")
# Local Postgres.app database; no password by default
# Of course, you fill in your own database information here.
con = dbConnect(pg, user="maestrosedesol", password="maestropassword",
                host="predictivadb.cshyqil2j46y.us-west-2.rds.amazonaws.com", port=5432, dbname="predictivadb")
mun_lat_long = as_tibble(dbGetQuery(con, "select cve_muni, latitud, longitud from raw.geom_municipales_mar2016;"))




# Load precios semanales - Central de Abasto
price <- read_csv("../data/carencia-alimenticia/precios_spider/precios_granos_semanales.csv") %>% 
  mutate(fecha=dmy(fecha))  
# precio por kilogramo vendido en bulto

# plot mean maize price per state
price %>% 
  mutate(mes = month(fecha)) %>% 
  mutate(año = year(fecha)) %>% 
  group_by(edo_destino,año,mes) %>% 
  summarise(precio_mean = mean(precio_frec, na.rm = TRUE),n = n()) %>% 
  View() 

  ggplot() +
  geom_point(aes(precio_mean,edo_destino,color=factor(año)))


central_abastos <- read.csv(file = "../data/IDW_test/central_abastos_test.csv", 
                            header = TRUE) %>% select(precio,lat,long)

##################
# Precio productor
##################

siap = read_csv("../data/siap/avance_agricola.csv") %>% 
  mutate(Clave_Estado = str_pad(Clave_Estado,2,pad="0"), 
                Clave_Municipio = str_pad(Clave_Municipio,3,pad="0")) %>%
  mutate(cve_muni = str_c(Clave_Estado,Clave_Municipio),PMR=PMR/1000) %>% 
  dplyr::select(cve_muni,PMR)  %>% left_join(mun_lat_long) %>%
  rename(lat = latitud, long=longitud, precio=PMR) %>% dplyr::select(precio,lat,long)

  
test<-as.data.frame(rbind(central_abastos,siap))
test<-test %>% filter(complete.cases(.))
test$x <- test$long  # define x & y as longitude and latitude
test$y <- test$lat

coordinates(test) = ~x + y


plot(test)

x.range <- as.numeric(c(-120.221469,-83.263461))  # min/max longitude of the interpolation area
y.range <- as.numeric(c(13.5911, 33.659085))  # min/max latitude of the interpolation area


grd <- expand.grid(x = seq(from = x.range[1], to = x.range[2], by = 0.1), y = seq(from = y.range[1], to = y.range[2], by = 0.1))
plot(grd)
proj4string(grd) = CRS("+proj=utm +ellps=WGS84 +datum=WGS84")

#Read shapefile and convert it to SpatialPolygons object.
#In the case of OP, the shapefile should be the administrative boundary of Germany.
shp = readOGR(dsn = "../data/shp/mexico_nb/mexico_nb.shp")
shp = shp@polygons
shp = SpatialPolygons(shp, proj4string=CRS("+proj=utm +ellps=WGS84 +datum=WGS84")) #make sure the shapefile has the same CRS from the data, and from the prediction grid.
#Clip the prediction grid with the shapefile.

elevation <- raster("/path/to/data/alt.bil")

grd.sub <- crop(grd, extent(shp))


coordinates(grd) <- ~x + y
gridded(grd) <- TRUE
plot(grd, cex = 1.5, col = "grey")
points(test, pch = 1, col = "red", cex = 1)

##################
# Ordinary kriging
##################

kriging_result = autoKrige(precio~1, test, grd)
plot(kriging_result)


coordinates(grd) <- ~ x + y # step 3 above
lzn.kriged <- krige(precio ~ 1, test, grd, model=lzn.fit)

lzn.kriged %>% as.data.frame %>%
  ggplot(aes(x=x, y=y)) + geom_tile(aes(fill=var1.pred)) + coord_equal() +
  scale_fill_gradient(low = "yellow", high="red") +
  scale_x_continuous(labels=comma) + scale_y_continuous(labels=comma) +
  theme_bw()


test %>% as.data.frame %>% 
  ggplot(aes(x, y)) + geom_point(aes(color=precio, size=precio), alpha=3/4) + 
  ggtitle("Zinc Concentration (ppm)") + coord_equal() + theme_bw()
coordinates(test) <- ~ x + y
str(test)

lzn.vgm <- variogram(log(precio)~1, test) # calculates sample variogram values 
lzn.fit <- fit.variogram(lzn.vgm, model=vgm(20, "Sph", 400, 20)) # fit model
plot(lzn.vgm, lzn.fit)




plot1 <- test %>% as.data.frame %>%
  ggplot(aes(x, y)) + geom_point(size=1) + coord_equal() + 
  ggtitle("Points with measurements")

# this is clearly gridded over the region of interest
plot2 <- grid %>% as.data.frame %>%
  ggplot(aes(x, y)) + geom_point(size=1) + coord_equal() + 
  ggtitle("Points at which to estimate")

library(gridExtra)
grid.arrange(plot1, plot2, ncol = 2)
