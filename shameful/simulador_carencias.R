####
# A este simulador le pasas un vector de reducción de carencias y vuelve a calcular la pobreza multidimensional.
# está en desarrollo la API con plumbre
####

rm(list=ls())
suppressPackageStartupMessages({
  library(tidyverse)
  library(doBy)
  library(reshape)
  library(data.table)
  library(stats)
})


#r_ic_rezedu=.10
#r_ic_asalud=.10
#r_ic_segsoc=.01
#r_ic_sbv=.20
#r_ic_cv=.01
#r_ic_ali=.01
#estado='01'

#######
# Functions
#######

#* @get /reduccion
reduccion <- function(base,estado,r_ic_rezedu ,r_ic_asalud,r_ic_segsoc ,r_ic_sbv , r_ic_cv ,r_ic_ali){
  # Obtiene la Base de Datos para el Estado después de la reducción porcentual de la carencia
  #
  # Args:
  #   base: Base Original a reducir
  #   estado: Estado para el que se va a hacer la simulación
  #   r_[carencia] <- porcentaje de disminución a la carencia. 
  # Returns:
  #   Base con reducción de pobreza.
  
  base_pobreza <- filter(base, ent == estado)
  for(variable in c("ic_rezedu" ,"ic_asalud","ic_segsoc" ,"ic_sbv" , "ic_cv" ,"ic_ali")){
    # sort to get the best candidates
    filter_inst = paste(variable," >0",sep="")
    temp <- base_pobreza %>% filter(i_privacion>0) %>% filter_(filter_inst)
    temp <- temp %>% arrange(i_privacion) 

    total_people <- sum(temp$factor_hog[temp[variable]==1], na.rm=TRUE)
    pct <- total_people * eval(as.name(paste("r_",variable,sep="")))
    # Obtienes los mejores índices

    i <- 1
    while(sum(temp$factor_hog[1:i][temp[variable]==1],na.rm = TRUE) < pct){
      i <- i + 1
    }
    folios_vivienda <- temp$folioviv[1:i]

    base_pobreza[variable][base_pobreza$folioviv %in% folios_vivienda,] <- 0
  }
return(base_pobreza)
}

#* @post /get_pobreza
get_pobreza <- function(estado,r_ic_rezedu ,r_ic_asalud,r_ic_segsoc ,r_ic_sbv , r_ic_cv ,r_ic_ali){
  base <- read_csv("../data/coneval/programas_calculo/R_2014/Base final/pobreza_14.csv")
  base_pobreza <- reduccion(base,estado,r_ic_rezedu ,r_ic_asalud,r_ic_segsoc ,r_ic_sbv , r_ic_cv ,r_ic_ali)
  
  #?ndice de Privaci?n Social
  base_pobreza$i_privacion=rowSums(data.frame(base_pobreza$ic_rezedu, base_pobreza$ic_asalud, base_pobreza$ic_segsoc, base_pobreza$ic_cv, base_pobreza$ic_sbv, base_pobreza$ic_ali), na.rm = TRUE)
  base_pobreza$i_privacion[(is.na(base_pobreza$ic_rezedu)==TRUE | is.na(base_pobreza$ic_asalud)==TRUE | is.na(base_pobreza$ic_segsoc)==TRUE | is.na(base_pobreza$ic_cv)==TRUE | is.na(base_pobreza$ic_sbv)==TRUE | is.na(base_pobreza$ic_ali)==TRUE)]=NA
  table(base_pobreza$i_privacion)
  #Pobreza
  base_pobreza$pobreza[((base_pobreza$i_privacion>=1 & is.na(base_pobreza$i_privacion)!=TRUE) & base_pobreza$plb==1)]=1
  base_pobreza$pobreza[(base_pobreza$plb==0 | base_pobreza$i_privacion==0) & (is.na(base_pobreza$plb)!=TRUE & is.na(base_pobreza$i_privacion)!=TRUE)]=0
  table(base_pobreza$pobreza)
  
  #Pobreza extrema
  base_pobreza$pobreza_e[(base_pobreza$i_privacion>=3 & is.na(base_pobreza$i_privacion)!=TRUE) & base_pobreza$plb_m==1]=1
  base_pobreza$pobreza_e[(base_pobreza$plb_m==0 | base_pobreza$i_privacion<3) & (is.na(base_pobreza$plb_m)!=TRUE & is.na(base_pobreza$i_privacion)!=TRUE)]=0
  table(base_pobreza$pobreza_e)
  
  #Pobreza moderada
  base_pobreza$pobreza_m[(base_pobreza$pobreza==1 & base_pobreza$pobreza_e==0)]=1
  base_pobreza$pobreza_m[ base_pobreza$pobreza==0 | (base_pobreza$pobreza==1 & base_pobreza$pobreza_e==1)]=0
  table(base_pobreza$pobreza_m)
  
  #Poblaci?n vulnerable
  
  #Vulnerables por carencias 
  base_pobreza$vul_car[((base_pobreza$i_privacion>=1 & is.na(base_pobreza$i_privacion)!=TRUE) & base_pobreza$plb==0)]=1
  base_pobreza$vul_car[((base_pobreza$plb==0 & base_pobreza$i_privacion==0) | base_pobreza$plb==1)]=0
  base_pobreza$vul_car[is.na(base_pobreza$pobreza)==TRUE]=NA
  table(base_pobreza$vul_car)
  
  #Vulnerables por ingresos
  base_pobreza$vul_ing[(base_pobreza$i_privacion==0 & base_pobreza$plb==1)]=1
  base_pobreza$vul_ing[base_pobreza$plb==0 | (base_pobreza$plb==1 & base_pobreza$i_privacion>=1) ]=0
  base_pobreza$vul_ing[is.na(base_pobreza$pobreza)==TRUE]=NA
  table(base_pobreza$vul_ing)
  
  #Poblaci?n no pobre y no vulnerable
  base_pobreza$no_pobv[(base_pobreza$i_privacion==0 & base_pobreza$plb==0)]=1
  base_pobreza$no_pobv[base_pobreza$plb==1 | (base_pobreza$plb==0 & base_pobreza$i_privacion>=1) ]=0
  base_pobreza$no_pobv[is.na(base_pobreza$pobreza)==TRUE]=NA
  table(base_pobreza$no_pobv)
  
  #Poblaci?n con carencias sociales#
  base_pobreza$carencias[base_pobreza$i_privacion>=1]=1
  base_pobreza$carencias[base_pobreza$i_privacion==0]=0
  base_pobreza$carencias[is.na(base_pobreza$pobreza)==TRUE]=NA
  table(base_pobreza$carencias)
  
  base_pobreza$carencias3[base_pobreza$i_privacion>=3]=1
  base_pobreza$carencias3[base_pobreza$i_privacion<=2]=0
  base_pobreza$carencias3[is.na(base_pobreza$pobreza)==TRUE]=NA
  table(base_pobreza$carencias3)
  
  #Cuadrantes
  base_pobreza$cuadrantes=NA
  base_pobreza$cuadrantes[(base_pobreza$i_privacion>=1 & is.na(base_pobreza$i_privacion)!=TRUE) & base_pobreza$plb==1]=1
  base_pobreza$cuadrantes[((base_pobreza$i_privacion>=1 & is.na(base_pobreza$i_privacion)!=TRUE) & base_pobreza$plb==0)]=2
  base_pobreza$cuadrantes[base_pobreza$i_privacion==0 & base_pobreza$plb==1]=3
  base_pobreza$cuadrantes[base_pobreza$i_privacion==0 & base_pobreza$plb==0]=4
  table(base_pobreza$cuadrantes)
  
  #Profundidad en el espacio del bienestar
  
  #FGT (a=1)
  
  lp1_urb = 1242.61 
  lp1_rur = 868.25
  
  lp2_urb = 2542.24
  lp2_rur = 1614.64
  
  #Distancia normalizada del ingreso respecto a la l?nea de bienestar
  base_pobreza$prof_b1[base_pobreza$rururb==1 & base_pobreza$plb==1 & is.na(base_pobreza$rururb)!=TRUE & is.na(base_pobreza$plb)!=TRUE]=(lp2_rur-base_pobreza$ictpc[base_pobreza$rururb==1 & base_pobreza$plb==1])/lp2_rur
  base_pobreza$prof_b1[base_pobreza$rururb==0 & base_pobreza$plb==1 & is.na(base_pobreza$rururb)!=TRUE & is.na(base_pobreza$plb)!=TRUE]=(lp2_urb-base_pobreza$ictpc[base_pobreza$rururb==0 & base_pobreza$plb==1])/lp2_urb
  base_pobreza$prof_b1[is.na(base_pobreza$ictpc)!=TRUE & is.na(base_pobreza$prof_b1)==TRUE]=0
  mean(base_pobreza$prof_b1[is.na(base_pobreza$prof_b1)!=TRUE])
  
  #Distancia normalizada del ingreso respecto a la l?nea de bienestar m?nimo
  base_pobreza$prof_bm1[base_pobreza$rururb==1 & base_pobreza$plb_m==1 & is.na(base_pobreza$rururb)!=TRUE & is.na(base_pobreza$plb_m)!=TRUE]=(lp1_rur-base_pobreza$ictpc[base_pobreza$rururb==1 & base_pobreza$plb_m==1])/lp1_rur
  base_pobreza$prof_bm1[base_pobreza$rururb==0 & base_pobreza$plb_m==1 & is.na(base_pobreza$rururb)!=TRUE & is.na(base_pobreza$plb_m)!=TRUE]=(lp1_urb-base_pobreza$ictpc[base_pobreza$rururb==0 & base_pobreza$plb_m==1])/lp1_urb
  base_pobreza$prof_bm1[is.na(base_pobreza$ictpc)!=TRUE & is.na(base_pobreza$prof_bm1)==TRUE]=0
  mean(base_pobreza$prof_bm1[is.na(base_pobreza$prof_bm1)!=TRUE])
  
  #Profundidad de la privaci?n social
  base_pobreza$profun=base_pobreza$i_privacion/6
  mean(base_pobreza$profun[is.na(base_pobreza$profun)!=TRUE])
  
  #Intensidad de la privaci?n social
  
  #Poblaci?n pobre
  #Intensidad de la privaci?n social: pobres
  base_pobreza$int_pob=base_pobreza$profun*base_pobreza$pobreza
  mean(base_pobreza$int_pob[is.na(base_pobreza$int_pob)!=TRUE])
  
  #Poblaci?n pobre extrema
  #Intensidad de la privaci?n social: pobres extremos
  base_pobreza$int_pobe=base_pobreza$profun*base_pobreza$pobreza_e
  mean(base_pobreza$int_pobe[is.na(base_pobreza$int_pobe)!=TRUE])
  
  #Poblaci?n vulnerable por carencias
  #Intensidad de la privaci?n social: poblaci?n vulnerable por carencias
  base_pobreza$int_vulcar=base_pobreza$profun*base_pobreza$vul_car
  mean(base_pobreza$int_vulcar[is.na(base_pobreza$int_vulcar)!=TRUE])
  
  #Poblaci?n carenciada
  #Intensidad de la privaci?n social: poblaci?n carenciada
  base_pobreza$int_caren=base_pobreza$profun*base_pobreza$carencias
  mean(base_pobreza$int_caren[is.na(base_pobreza$int_caren)!=TRUE])
  
  base_pobreza <- orderBy(~+proyecto+folioviv+foliohog, data=as.data.frame(base_pobreza))
  
  
  
  #Pobreza
  pobreza=by(base_pobreza,base_pobreza[ ,"ent"],
             function(base_pobreza)sum(base_pobreza$factor[base_pobreza$pobreza==1 & is.na(base_pobreza$pobreza)==FALSE], na.rm=TRUE))
  #Pobreza moderada
  pobreza_m=by(base_pobreza,base_pobreza[ ,"ent"], 
               function(base_pobreza)sum(base_pobreza$factor[base_pobreza$pobreza_m==1 & is.na(base_pobreza$pobreza)==FALSE], na.rm=TRUE))
  #Pobreza extrema
  pobreza_e=by(base_pobreza,base_pobreza[ ,"ent"], 
               function(base_pobreza)sum(base_pobreza$factor[base_pobreza$pobreza_e==1 & is.na(base_pobreza$pobreza)==FALSE], na.rm=TRUE))
  #Vulnerables por carencias sociales
  vul_car=by(base_pobreza,base_pobreza[ ,"ent"], 
             function(base_pobreza)sum(base_pobreza$factor[base_pobreza$vul_car==1 & is.na(base_pobreza$pobreza)==FALSE], na.rm=TRUE))
  #Vulnerables por ingresos
  vul_ing=by(base_pobreza,base_pobreza[ ,"ent"], 
             function(base_pobreza)sum(base_pobreza$factor[base_pobreza$vul_ing==1 & is.na(base_pobreza$pobreza)==FALSE], na.rm=TRUE))
  #No pobre multidimensional no vulnerable
  no_pobv=by(base_pobreza,base_pobreza[ ,"ent"], 
             function(base_pobreza)sum(base_pobreza$factor[base_pobreza$no_pobv==1 & is.na(base_pobreza$pobreza)==FALSE], na.rm=TRUE))
  #Poblacii?n con al menos una carencia social
  carencias=by(base_pobreza,base_pobreza[ ,"ent"], 
               function(base_pobreza)sum(base_pobreza$factor[base_pobreza$carencias==1 & is.na(base_pobreza$pobreza)==FALSE], na.rm=TRUE))
  #poblaci?n con al menos tres carencias sociales
  carencias3=by(base_pobreza,base_pobreza[ ,"ent"], 
                function(base_pobreza)sum(base_pobreza$factor[base_pobreza$carencias3==1 & is.na(base_pobreza$pobreza)==FALSE], na.rm=TRUE))
  #Rezago educativo
  ic_rezedu=by(base_pobreza,base_pobreza[ ,"ent"], 
               function(base_pobreza)sum(base_pobreza$factor[base_pobreza$ic_rezedu==1 & is.na(base_pobreza$pobreza)==FALSE], na.rm=TRUE))
  #Carencia por acceso a servicios de salud
  ic_asalud=by(base_pobreza,base_pobreza[ ,"ent"], 
               function(base_pobreza)sum(base_pobreza$factor[base_pobreza$ic_asalud==1 & is.na(base_pobreza$pobreza)==FALSE], na.rm=TRUE))
  #Carencias por acceso a seguridad social
  ic_segsoc=by(base_pobreza,base_pobreza[ ,"ent"], 
               function(base_pobreza)sum(base_pobreza$factor[base_pobreza$ic_segsoc==1 & is.na(base_pobreza$pobreza)==FALSE], na.rm=TRUE))
  #Carencias por calidad y espacios de la vivienda
  ic_cev=by(base_pobreza,base_pobreza[ ,"ent"], 
            function(base_pobreza)sum(base_pobreza$factor[base_pobreza$ic_cv==1 & is.na(base_pobreza$pobreza)==FALSE], na.rm=TRUE))
  #Carencias por acceso a los servicios b?sicos de la vivienda
  ic_sbv=by(base_pobreza,base_pobreza[ ,"ent"], 
            function(base_pobreza)sum(base_pobreza$factor[base_pobreza$ic_sbv==1 & is.na(base_pobreza$pobreza)==FALSE], na.rm=TRUE))
  #Carencias por acceso a la alimentaci?n
  ic_ali=by(base_pobreza,base_pobreza[ ,"ent"], 
            function(base_pobreza)sum(base_pobreza$factor[base_pobreza$ic_ali==1 & is.na(base_pobreza$pobreza)==FALSE], na.rm=TRUE))
  #Poblacion con un ingreso inferior a la l?nea de bienestar m?nimo
  plb_m=by(base_pobreza,base_pobreza[ ,"ent"], 
           function(base_pobreza)sum(base_pobreza$factor[base_pobreza$plb_m==1 & is.na(base_pobreza$pobreza)==FALSE], na.rm=TRUE))
  #Poblacion con un ingreso inferior a la l?nea de bienestar
  plb=by(base_pobreza,base_pobreza[ ,"ent"], 
         function(base_pobreza)sum(base_pobreza$factor[base_pobreza$plb==1 & is.na(base_pobreza$pobreza)==FALSE], na.rm=TRUE))
  
  
  tabstat_sum=as.data.frame(matrix(0,nr=1,nc=16))
  
  
  names(tabstat_sum)[1:16]=cbind("pobreza","pobreza_m","pobreza_e","vul_car","vul_ing","no_pobv","carencias","carencias3",
                                 "ic_rezedu","ic_asalud","ic_segsoc","ic_cev","ic_sbv","ic_ali", "plbm","plb")
  
  
  #row.names(tabstat_sum)[1:33]=cbind("Aguascalientes","Baja California","Baja California Sur","Campeche","Coahuila","Colima"
  #                                   ,"Chiapas","Chihuahua","Distrito Federal","Durango","Guanajuato","Guerrero","Hidalgo","Jalisco",
  #                                   "M?xico","Michoac?n","Morelos","Nayarit","Nuevo Le?n","Oaxaca","Puebla","Quer?taro","Quintana Roo",
  #                                   "San Luis Potos?","Sinaloa","Sonora","Tabasco","Tamaulipas","Tlaxcala","Veracruz","Yucat?n","Zacatecas","NACIONAL")
  
  tabstat_sum[1]=pobreza[[1]]
  tabstat_sum[2]=pobreza_m[[1]]
  tabstat_sum[3]=pobreza_e[[1]]
  tabstat_sum[4]=vul_car[[1]]
  tabstat_sum[5]=vul_ing[[1]]
  tabstat_sum[6]=no_pobv[[1]]
  tabstat_sum[7]=carencias[[1]]
  tabstat_sum[8]=carencias3[[1]]
  tabstat_sum[9]=ic_rezedu[[1]]
  tabstat_sum[10]=ic_asalud[[1]]
  tabstat_sum[11]=ic_segsoc[[1]]
  tabstat_sum[12]=ic_cev[[1]]
  tabstat_sum[13]=ic_sbv[[1]]
  tabstat_sum[14]=ic_ali[[1]]
  tabstat_sum[15]=plb_m[[1]]
  tabstat_sum[16]=plb[[1]]
  
  
  
  
  #Porcentaje de la poblaci?n total en pobreza
  p.pobreza<-by(base_pobreza,base_pobreza[ ,"ent"],function(base_pobreza)weighted.mean(base_pobreza$pobreza[is.na(base_pobreza$pobreza)==FALSE],base_pobreza$factor[is.na(base_pobreza$pobreza)==FALSE],na.rm=TRUE)*100)
  #Porcentaje de la poblaci?n total en pobreza moderada
  p.pobreza_m<-by(base_pobreza,base_pobreza[ ,"ent"],    function(base_pobreza)weighted.mean(base_pobreza$pobreza_m[is.na(base_pobreza$pobreza)==FALSE],base_pobreza$factor[is.na(base_pobreza$pobreza)==FALSE],na.rm=TRUE)*100)
  #Porcentaje de la poblaci?n total en pobreza extrema
  p.pobreza_e<-by(base_pobreza,base_pobreza[ ,"ent"],function(base_pobreza)weighted.mean(base_pobreza$pobreza_e[is.na(base_pobreza$pobreza)==FALSE],base_pobreza$factor[is.na(base_pobreza$pobreza)==FALSE],na.rm=TRUE)*100)
  #Porcentaje de la poblaci?n total vulnerable por carencias sociales
  p.vul_car<-by(base_pobreza,base_pobreza[ ,"ent"],function(base_pobreza)weighted.mean(base_pobreza$vul_car[is.na(base_pobreza$pobreza)==FALSE],base_pobreza$factor[is.na(base_pobreza$pobreza)==FALSE],na.rm=TRUE)*100)
  #Porcentaje de la poblaci?n total vulnerable por ingresos
  p.vul_ing<-by(base_pobreza,base_pobreza[ ,"ent"], function(base_pobreza)weighted.mean(base_pobreza$vul_ing[is.na(base_pobreza$pobreza)==FALSE],base_pobreza$factor[is.na(base_pobreza$pobreza)==FALSE],na.rm=TRUE)*100)
  #Porcentaje de la poblaci?n total no pobre y no vulnerable
  p.no_pobv<-by(base_pobreza,base_pobreza[ ,"ent"],function(base_pobreza)weighted.mean(base_pobreza$no_pobv[is.na(base_pobreza$pobreza)==FALSE],base_pobreza$factor[is.na(base_pobreza$pobreza)==FALSE],na.rm=TRUE)*100)
  #Porcentaje de la poblaci?n total con rezago educativo
  p.carencias<-by(base_pobreza,base_pobreza[ ,"ent"],function(base_pobreza)weighted.mean(base_pobreza$carencias[is.na(base_pobreza$pobreza)==FALSE],base_pobreza$factor[is.na(base_pobreza$pobreza)==FALSE],na.rm=TRUE)*100)
  #Porcentaje de la poblaci?n total con carencia por acceso a la salud
  p.carencias3<-by(base_pobreza,base_pobreza[ ,"ent"],    function(base_pobreza)weighted.mean(base_pobreza$carencias3[is.na(base_pobreza$pobreza)==FALSE],base_pobreza$factor[is.na(base_pobreza$pobreza)==FALSE],na.rm=TRUE)*100)
  #Porcentaje de la poblaci?n total con rezago educativo
  p.ic_rezedu<-by(base_pobreza,base_pobreza[ ,"ent"],function(base_pobreza)weighted.mean(base_pobreza$ic_rezedu[is.na(base_pobreza$pobreza)==FALSE],base_pobreza$factor[is.na(base_pobreza$pobreza)==FALSE],na.rm=TRUE)*100)
  #Porcentaje de la poblaci?n total con carencia por acceso a la salud
  p.ic_asalud<-by(base_pobreza,base_pobreza[ ,"ent"],    function(base_pobreza)weighted.mean(base_pobreza$ic_asalud[is.na(base_pobreza$pobreza)==FALSE],base_pobreza$factor[is.na(base_pobreza$pobreza)==FALSE],na.rm=TRUE)*100)
  #Porcentaje de la poblaci?n total con carencia por acceso a seguridad social
  p.ic_segsoc<-by(base_pobreza,base_pobreza[ ,"ent"],    function(base_pobreza)weighted.mean(base_pobreza$ic_segsoc[is.na(base_pobreza$pobreza)==FALSE],base_pobreza$factor[is.na(base_pobreza$pobreza)==FALSE],na.rm=TRUE)*100)
  #Porcentaje de la poblaci?n total con carencia por calidad y espacios de la vivienda
  p.ic_cev<-by(base_pobreza,base_pobreza[ ,"ent"],    function(base_pobreza)weighted.mean(base_pobreza$ic_cv[is.na(base_pobreza$pobreza)==FALSE],base_pobreza$factor[is.na(base_pobreza$pobreza)==FALSE],na.rm=TRUE)*100)
  #Porcentaje de la poblaci?n total con carencia por servicios b?sicos de la vivienda
  p.ic_sbv<-by(base_pobreza,base_pobreza[ ,"ent"],    function(base_pobreza)weighted.mean(base_pobreza$ic_sbv[is.na(base_pobreza$pobreza)==FALSE],base_pobreza$factor[is.na(base_pobreza$pobreza)==FALSE],na.rm=TRUE)*100)
  #Porcentaje de la poblaci?n total con carencia por acceso a la acc_alimentaci?n
  p.ic_ali<-by(base_pobreza,base_pobreza[ ,"ent"],    function(base_pobreza)weighted.mean(base_pobreza$ic_ali[is.na(base_pobreza$pobreza)==FALSE],base_pobreza$factor[is.na(base_pobreza$pobreza)==FALSE],na.rm=TRUE)*100)
  #Porcentaje de la poblaci?n total con carencia por servicios b?sicos de la vivienda
  p.plb_m<-by(base_pobreza,base_pobreza[ ,"ent"],    function(base_pobreza)weighted.mean(base_pobreza$plb_m[is.na(base_pobreza$pobreza)==FALSE],base_pobreza$factor[is.na(base_pobreza$pobreza)==FALSE],na.rm=TRUE)*100)
  #Porcentaje de la poblaci?n total con carencia por acceso a la acc_alimentaci?n
  p.plb<-by(base_pobreza,base_pobreza[ ,"ent"],    function(base_pobreza)weighted.mean(base_pobreza$plb[is.na(base_pobreza$pobreza)==FALSE],base_pobreza$factor[is.na(base_pobreza$pobreza)==FALSE],na.rm=TRUE)*100)
  
  
  tabstat_mean=as.data.frame(matrix(0,nr=1,nc=16))
  
  
  names(tabstat_mean)[1:16]=cbind("pobreza","pobreza_m","pobreza_e","vul_car","vul_ing","no_pobv","carencias","carencias3",
                                  "ic_rezedu","ic_asalud","ic_segsoc","ic_cev","ic_sbv","ic_ali", "plbm","plb")
  
  
  
  #ESTATAL
  
  tabstat_mean[1]=p.pobreza[[1]]
  tabstat_mean[2]=p.pobreza_m[[1]]
  tabstat_mean[3]=p.pobreza_e[[1]]
  tabstat_mean[4]=p.vul_car[[1]]
  tabstat_mean[5]=p.vul_ing[[1]]
  tabstat_mean[6]=p.no_pobv[[1]]
  tabstat_mean[7]=p.carencias[[1]]
  tabstat_mean[8]=p.carencias3[[1]]
  tabstat_mean[9]=p.ic_rezedu[[1]]
  tabstat_mean[10]=p.ic_asalud[[1]]
  tabstat_mean[11]=p.ic_segsoc[[1]]
  tabstat_mean[12]=p.ic_cev[[1]]
  tabstat_mean[13]=p.ic_sbv[[1]]
  tabstat_mean[14]=p.ic_ali[[1]]
  tabstat_mean[15]=p.plb_m[[1]]
  tabstat_mean[16]=p.plb[[1]]
  
  colnames(tabstat_mean) <- paste(colnames(tabstat_mean),"p", sep = "_")
  colnames(tabstat_sum) <- paste(colnames(tabstat_sum),"n", sep = "_")
  state <- cbind(tabstat_mean,tabstat_sum)
  
  return(state)
}  


#temp1 <- get_pobreza(estado,r_ic_rezedu ,r_ic_asalud,r_ic_segsoc ,r_ic_sbv , r_ic_cv ,r_ic_ali)
#compara <- get_pobreza("01",base)
#temp2 <- get_pobreza("01",temp1)


# tener un vector de pesos de preferencia (por carencia)
# Preguntar la cantidad de gente a la que quieres sacar de pobreza extrema.
# optimizar la selección de carencias a imputar.