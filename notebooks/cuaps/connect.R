dotenv::load_dot_env(file = '../../politica_preventiva/.env')

get_con <- function(){
  DBI::dbConnect(RPostgreSQL::PostgreSQL(),
                 host = Sys.getenv('PGHOST'),
                 user = Sys.getenv('POSTGRES_USER'),
                 password = Sys.getenv('POSTGRES_PASSWORD'),
                 dbname = Sys.getenv('PGDATABASE'))
}

con <- get_con()
