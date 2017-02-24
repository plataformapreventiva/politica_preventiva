##### FUNCTION FOR PERFORMING SPATIAL IMPUTAION ####
library(tidyverse)
library(RPostgreSQL)
pg = dbDriver("PostgreSQL")
con = dbConnect(pg, user="maestrosedesol", password="maestropassword",
                host="predictivadb.cshyqil2j46y.us-west-2.rds.amazonaws.com", port=5432, dbname="predictivadb")

#coords <- as_tibble(dbGetQuery(con, "SELECT cve_muni , latitud AS lat, longitud AS long FROM geoms.geom_municipios;"))


complete_entries <- function(coords, z, return_complete=TRUE, by_coords=NULL, by_z=NULL){
# Homologates entries as not have
  # coords: a dataframe with columns 'id', 'lat', 'long'
  # z: dataframe with columns 'id' and others
  # return_complete returns a merged dataframe
  #     if FALSE, it returns a dataframe with the same columns as z
  # if coods doesnt contain 'id', you can specify an 'id' column in by_coords and by_z
  
  if (is.null(by_coords) & is.null(by_z)){
    df <- merge(coords, z, all.x=TRUE, all.y=FALSE)
  }else{
    df <- merge(coords, z, by.x=by_coords, by.y=by_z, all.x=TRUE, all.y=FALSE)
    }
  if(!return_complete){df <- df %>% select(-lat, -long)}
  return(df)
}

kriging_imputation <- function(lat, long, z, id, type='Exp', plot=FALSE){
  # gstat::vgm() lists all available types
  library(sp)
  library(gstat)
  df <- data.frame(lat=lat, long=long, z=z, id=id)
  df_new <- dplyr::filter(df, is.na(z))
  if(nrow(df_new) > 0) {
    new_coords <- select(df_new, lat, long)
    old_coords <- select(df, lat, long)
    df <- dplyr::filter(df, !is.na(z))
    coordinates(df) <- ~ long + lat
    df_new$z <- NULL
    coordinates(df_new) <- ~ long + lat
    df_vgm <- variogram(z ~ 1, df)
    if (plot==TRUE){plot(df_vgm)}
    ## MISSING: compare different variogram models
    
    df_fit <- fit.variogram(df_vgm, model=vgm(type))
    ## Add 'try' for different functions in case one fails
    
    kriged <- krige(z ~ 1, df, newdata=df_new, model=df_fit)
    results <- data.frame(id = (df_new$id), z = kriged$var1.pred, kriged@coords)  %>%
      rbind(cbind(df@data, df@coords))
    return(results)
  } else {
    return(df)
  }
}

normalize <- function(x, top_is_better=TRUE){
  y <- (x - min(x, na.rm =TRUE))/(max(x, na.rm =TRUE)-min(x, na.rm =TRUE))
  y[which(y < .0001)] <- .0001
  y[which(y > .9999)] <- .9999
  if (top_is_better){
    return(1-y)
  }
  else{return(y)}
}
