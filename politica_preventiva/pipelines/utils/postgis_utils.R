library(RPostgreSQL)
library(rgeos)
library(sp)


GetConn <- function(dbname=PGDATABASE, host=PGHOST, user = POSTGRES_USER,
               password = POSTGRES_PASSWORD){
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

GetPoligonFromPostgis <- function (conn, table, schema= "geoms", verbose = TRUE){
  # Function to get a poligon from postgis table
  #
  # Args:
  #   conn: Postgres connection
  #   schema: Schema where the table is stored
  #   table: Table with the Geometry
  #
  # Returns:
  #   SpatialPolygonDataframe
  
  query <- "SELECT ST_AsText(geom) AS wkt_geometry , * FROM %s.%s"
  query <- sprintf(query, schema, table)
  
  df.query <- dbGetQuery(conn, query)
  df.query$geom <- NULL
  
  row.names(df.query) = df.query$gid
  
  # Create spatial polygons
  rWKT <- function (var1 , var2 ) { return (readWKT ( var1 , var2) @polygons) }
  spl <- mapply( rWKT , df.query$wkt_geometry ,  df.query$gid )
  sp.temp <- SpatialPolygons( spl )
  
  # Create SpatialPolygonsDataFrame, drop WKT field from attributes
  sp.df <- SpatialPolygonsDataFrame(sp.temp, df.query[-1])
  
  if (verbose)
    cat("Descargando geometría; ", table, "del schema: ",schema, sep = "")  
  
  return (sp.df)
}