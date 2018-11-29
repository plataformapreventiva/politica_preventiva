#!/usr/bin/env Rscript
# Intercensal
library(rlang)
library(tidyverse)

int_columns <- c('mun_asi', 'ent_pais_asi', 'mun_trab', 'ent_pais_trab', 'factor',
                 'sexo', 'edad', 'conact', 'acti_sin_pago1', 'acti_sin_pago2',
                 'acti_sin_pago3', 'acti_sin_pago4', 'acti_sin_pago5', 'acti_sin_pago6',
                 'acti_sin_pago7', 'acti_sin_pago8')
float_columns <- c('poblacion_mun','migrantes_internos_recientes',
                   'migrantes', 'mayores65', 'menores5', 'adultos', 'menores', 'mayores',
                   'indigenas', 'lengua_indigena', 'afrodescendientes', 'mujeres', 'hombres')
text_columns <- c('cve_muni')

query <- 'SELECT LPAD(ent::text, 2, \'0\') as cve_ent,
					LPAD(ent::text, 2, \'0\') || LPAD(mun::text, 3, \'0\') as cve_muni,
                    LPAD(ent_pais_asi::text, 2, \'0\') as ent_pais_asi,
                    LPAD(ent_pais_trab::text, 2, \'0\') as ent_pais_trab,
                    LPAD(mun_asi::text, 3, \'0\') as mun_asi,
                    LPAD(mun_trab::text, 3, \'0\') as mun_trab,
                    factor,
                    sexo,
                    edad,
                    conact,
                    acti_sin_pago1,
                    acti_sin_pago2,
                    acti_sin_pago3,
                    acti_sin_pago4,
                    acti_sin_pago5,
                    acti_sin_pago6,
                    acti_sin_pago7,
                    acti_sin_pago8,
                    sum (factor) as poblacion_mun,
                    sum(CASE
                        WHEN nom_mun != nom_mun_res10
                            THEN factor
                            ELSE 0
                        END) as migrantes_internos_recientes,
                    sum(CASE
                        WHEN CAST(ent_pais_res10 as integer) > 33
                            THEN factor
                            ELSE 0
                        END) as migrantes,
                    sum(CASE
                        WHEN CAST(edad as integer) > 64
                            THEN factor
                            ELSE 0
                        END) as mayores65,
                    sum(CASE
                        WHEN CAST(edad as integer) < 6
                            THEN factor
                            ELSE 0
                        END) as menores5,
                    sum(CASE
                        WHEN CAST(edad as integer) < 30 AND edad >18
                            THEN factor
                            ELSE 0
                        END) as adultos,
                    sum(CASE
                        WHEN CAST(edad as integer) < 18
                            THEN factor
                            ELSE 0
                        END) as menores,
                    sum(CASE
                        WHEN CAST(edad as integer) > 64
                            THEN factor
                            ELSE 0
                        END) as mayores,
                    sum(CASE
                        WHEN CAST(perte_indigena as integer) = 1 OR CAST(perte_indigena as integer) = 2
                            THEN factor
                            ELSE 0
                        END) as indigenas,
                    sum(CASE
                        WHEN CAST(hlengua as integer) = 1
                            THEN factor
                            ELSE 0
                        END) as lengua_indigena,
                    sum(CASE
                        WHEN CAST(afrodes as integer) = 1 OR CAST(afrodes as integer) = 2
                            THEN factor
                            ELSE 0
                        END) as afrodescendientes,
                    sum(CASE
                        WHEN CAST(sexo as integer) = 3
                            THEN factor
                            ELSE 0
                        END) as mujeres,
                    sum(CASE
                        WHEN CAST(sexo as integer) = 1
                            THEN factor
                            ELSE 0
                        END) as hombres
                    FROM raw.intercensal_personas_2015
                    GROUP BY ent, cve_muni, mun_asi,
                    ent_pais_asi,
                    mun_trab,
                    ent_pais_trab,
                    factor,
                    sexo,
                    edad,
                    conact,
                    acti_sin_pago1,
                    acti_sin_pago2,
                    acti_sin_pago3,
                    acti_sin_pago4,
                    acti_sin_pago5,
                    acti_sin_pago6,
                    acti_sin_pago7,
                    acti_sin_pago8'

make_clean <- function(pipeline_task, con){
  df <- tbl(con, sql(query))
  df %>%
    mutate_at(int_columns, funs(as.integer(.))) %>%
    mutate_at(float_columns, funs(as.numeric(.))) %>%
    mutate_at(text_columns, funs(as.character(.)))
}
