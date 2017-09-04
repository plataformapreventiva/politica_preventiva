library(plumber)
r <- plumb("./simulador_carencias.R") 
r$run(port=8000)
#curl --data "r_ic_rezedu=.01&r_ic_asalud=.01&r_ic_segsoc=.01&r_ic_sbv=.01&r_ic_cv=.01&r_ic_ali=.01}" http://localhost:8000/get_pobreza


