library(RPostgreSQL)
library(rgeos)
library(sp)

GetConn <- function(dbname,host,user,password){
  # Function to get a connection to db
  #
  # Args:
  #   dbname: DB name
  #   host: DB Host
  #   user: DB User
  #   password: Password
  #
  # Returns:
  #   SpatialPolygonDataframe
  
  conn = dbConnect(
  dbDriver("PostgreSQL"), dbname=dbname, host=host, port=5432, 
  user=user, password=password)

  return (conn)
}

GetPoligonFromPostgis <- function (conn, schema, table, verbose = TRUE){
  # Function to get a poligon from postgis table
  #
  # Args:
  #   conn: Postgres connection
  #   schema: Schema where the table is stored
  #   table: Table with the Geometry
  #
  # Returns:
  #   SpatialPolygonDataframe
  
  query <- "SELECT ST_AsText(geom) AS wkt_geometry , * FROM geoms.%s"
  query <- sprintf(query, schema, table)
  
  dfQuery <- dbGetQuery(conn, query)
  dfQuery$geom <- NULL
  
  row.names(dfQuery) = dfQuery$gid
  
  # Create spatial polygons
  rWKT <- function (var1 , var2 ) { return (readWKT ( var1 , var2) @polygons) }
  spL <- mapply( rWKT , dfQuery$wkt_geometry ,  dfQuery$gid )
  spTemp <- SpatialPolygons( spL )
  
  # Create SpatialPolygonsDataFrame, drop WKT field from attributes
  spdf <- SpatialPolygonsDataFrame(spTemp, dfQuery[-1])
  
  if (verbose)
    cat("Descargando geometría; ", table, "del schema: ",schema, sep = "")  
  
  return (spdf)
}