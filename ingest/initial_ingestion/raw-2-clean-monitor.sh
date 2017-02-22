""" 
Script to create clean.explorador_* tables - 'explorador' module

Dependence: S3-to-raw.sh

Assumes existence of:
  Municipios
    raw.pub_municipios + dic
    raw.general_municipios + dic
    raw.irs_municipios  + dic
    raw.complejidad_municipios + dic
  Estados
    raw.coneval_estados + dic
    raw.pub_estados + dic
"""


###############################
# Raw Explorador Municipios
###############################

## Delete raw explorador
PGOPTIONS="--search_path=raw"  psql --db postgresql://$PGUSER:$PGPASSWORD@$PGEC2/predictivadb -c 'DROP TABLE clean.explorador_municipios;'

## create raw explorador
PGOPTIONS="--search_path=raw"  psql --db postgresql://$PGUSER:$PGPASSWORD@$PGEC2/predictivadb -c \
'CREATE TABLE clean.explorador_municipios AS SELECT
  A.cve_ent, A.nom_ent, A.cve_muni, A.nom_mun, A.pobtot_ajustada, A.pobreza_p_10 , A.pobreza_e_p_10 , A.pobreza_m_p_10 , A.vul_car_p_10 , A.vul_ing_p_10 , A.no_pobv_p_10 , A.ic_rezedu_p_10 , A.ic_asalud_p_10 , A.ic_segsoc_p_10 , A.ic_cev_p_10 , A.ic_sbv_p_10 , A.ic_ali_p_10 , A.carencias_p_10 , A.carencias3_p_10 , A.plb_p_10 , A.pblm_p_10 , A.rankin_p_10, A.rankin_pe_10, A.pobreza_n_10, A.pobreza_e_n_10, A.pobreza_m_n_10, A.vul_car_n_10, A.vul_ing_n_10, A.no_pobv_n_10, A.ic_rezedu_n_10, A.ic_asalud_n_10, A.ic_segsoc_n_10, A.ic_cev_n_10, A.ic_sbv_n_10, A.ic_ali_n_10, A.carencias_n_10, A.carencias3_n_10, A.plb_n_10, A.pblm_n_10, A.perrankin_p_10, A.perrankin_pe_10, A.cppobreza , A.cppobreza_e , A.cppobreza_m , A.cpvul_car , A.cpic_rezedu , A.cpic_asalud , A.cpic_segsoc , A.cpic_cv , A.cpic_sbv , A.cpic_ali , A.cpcarencias , A.cpcarencias3 , A.cpplb , A.cpplbm , A.p_rez_edu_90 , A.p_rez_edu_00 , A.p_rez_edu_10 , A.p_ser_sal_00 , A.p_ser_sal_10 , A.p_viv_pisos_90 , A.p_viv_pisos_00 , A.p_viv_pisos_10 , A.p_viv_muros_90 , A.p_viv_muros_00 , A.p_viv_muros_10 , A.p_viv_techos_90 , A.p_viv_techos_00 , A.p_viv_techos_10 , A.p_viv_hacin_90 , A.p_viv_hacin_00 , A.p_viv_hacin_10 , A.p_viv_agu_entub_90 , A.p_viv_agu_entub_00 , A.p_viv_agu_entub_10 , A.p_viv_dren_90 , A.p_viv_dren_00 , A.p_viv_dren_000 , A.p_viv_elect_90 , A.p_viv_elect_00 , A.p_viv_elect_10 , A.pobreza_alim_90 , A.pobreza_alim_00 , A.pobreza_alim_10 , A.pobreza_cap_90 , A.pobreza_cap_00 , A.pobreza_cap_10 , A.pobreza_patrim_90 , A.pobreza_patrim_00 , A.pobreza_patrim_10 , A.gini_90 , A.gini_00 , A.gini_10,
  B."S052", B."0424" , B."S057" , B."S061" , B."S065" , B."S071" , B."S072" , B."S118" , B."S174" , B."S176" , B."S216" , B."S241", B."U009" , B."E003" , B."0196" , B."S017" , B."E016" , B."0342",
  D.pobtot_00, D.pobtot_05, D.pobtot_10, D.pobtot_15, D.porc_pob_15_analfa00, D.porc_pob_15_analfa05, D.porc_pob_15_analfa10, D.porc_pob_15_analfa15, D.porc_pob_snservsal00, D.porc_pob_snservsal05, D.porc_pob_snservsal10, D.porc_pob_snservsal15, D.porc_vivsnsan00, D.porc_vivsnsan05, D.porc_vivsnsan10, D.porc_vivsnsan15, D.porc_snaguaent00, D.porc_snaguaent05, D.porc_snaguaent10, D.porc_snaguaent15, D.porc_vivsndren00, D.porc_vivsndren05, D.porc_vivsndren10, D.porc_vivsndren15, D.porc_vivsnenergia00, D.porc_vivsnenergia05, D.porc_vivsnenergia10, D.porc_vivsnenergia15, D.irez_soc00, D.irez_soc05, D.irez_soc10, D.irez_soc15, D.gdo_rezsoc00, D.gdo_rezsoc05, D.gdo_rezsoc10, D.gdo_rezsoc15, D.l_ocupnac00, D.l_ocupnac05, D.l_ocupnac10, D.l_ocupnac15,
  E.complejidad_2014
  FROM raw.coneval_municipios A
  FULL OUTER JOIN raw.pub_municipios B on A.cve_muni = B.cve_muni
  FULL OUTER JOIN raw.general_municipios C on A.cve_muni = C.cve_muni
  FULL OUTER JOIN raw.irs_municipios D on A.cve_muni = D.cve_muni
  FULL OUTER JOIN raw.complejidad_municipios E on A.cve_muni = E.cve_muni;' 

# Create index explorador municipios
PGOPTIONS="--search_path=raw"  psql --db postgresql://$PGUSER:$PGPASSWORD@$PGEC2/predictivadb -c \
'CREATE INDEX raw_semantic_mun   ON clean.explorador_municipios   USING btree   (cve_muni COLLATE pg_catalog."default");' 

#CREATE INDEX semantic_mun_ent
#ON "semantic".semantic_municipios
#USING btree
#(cve_ent COLLATE pg_catalog."default");


## create raw explorador dic

PGOPTIONS="--search_path=raw"  psql --db postgresql://$PGUSER:$PGPASSWORD@$PGEC2/predictivadb -c \
'DROP TABLE clean.explorador_municipios_dic;'

## create raw semantic dic 
# CHECAR ** NO CORRE EN BASH
PGOPTIONS="--search_path=raw"  psql --db postgresql://$PGUSER:$PGPASSWORD@$PGEC2/predictivadb -c \
"CREATE TABLE clean.explorador_municipios_dic
  AS
  SELECT A.id, A.indicador as nombre, A.fuente, A.tipo, A.periodo, A.metadata from raw.coneval_municipios_dic A WHERE A.id IN (select column_name FROM information_schema.columns WHERE
  table_name='explorador_municipios' AND table_schema = 'clean')
  UNION
  SELECT B.cve_prog_dggpb as id, nom_prog_completo as nombre, B.fuente, B.tipo, B.periodo, B.metadata from raw.pub_diccionario_programas B WHERE B.cve_prog_dggpb IN (select column_name FROM information_schema.columns where
  table_name='explorador_municipios' AND table_schema =  'clean')
  UNION
  SELECT C.id, C.nombre, C.fuente, C.tipo, C.periodo, C.metadata from raw.general_municipios_dic C WHERE C.id IN (select column_name FROM information_schema.columns WHERE
  table_name='explorador_municipios' AND table_schema = 'clean')
  UNION
  SELECT D.id, D.nombre, D.fuente, D.tipo, D.periodo, D.metadata from raw.complejidad_dic D WHERE D.id IN (select column_name FROM information_schema.columns WHERE
  table_name='explorador_municipios' AND table_schema = 'clean')
  UNION
  SELECT E.id, E.nombre, E.fuente, E.tipo, E.periodo, E.metadata from "raw".irs_dic E WHERE E.id IN (select column_name FROM information_schema.columns WHERE
  table_name='explorador_municipios' AND table_schema = 'clean');"


###############################
# Raw Explorador Estados
###############################
#todo(change cve_edo por cve_ent)

PGOPTIONS="--search_path=raw"  psql --db postgresql://$PGUSER:$PGPASSWORD@$PGEC2/predictivadb -c \
'DROP TABLE clean.explorador_estados;'

PGOPTIONS="--search_path=raw"  psql --db postgresql://$PGUSER:$PGPASSWORD@$PGEC2/predictivadb -c \
'CREATE TABLE clean.explorador_estados
  AS
  SELECT

  A.cve_ent, A.pobreza_P_10, A.pobreza_N_10, A.pobreza_e_P_10, A.pobreza_e_N_10, A.pobreza_m_P_10, A.pobreza_m_N_10, A.vul_car_P_10,
  A.vul_car_N_10, A.vul_ing_P_10, A.vul_ing_N_10, A.no_pobv_P_10, A.no_pobv_N_10, A.ic_rezedu_P_10, A.ic_rezedu_N_10, A.ic_asalud_P_10,
  A.ic_asalud_N_10, A.ic_segsoc_P_10, A.ic_segsoc_N_10, A.ic_cev_P_10, A.ic_cev_N_10, A.ic_sbv_P_10, A.ic_sbv_N_10, A.ic_ali_P_10,
  A.ic_ali_N_10, A.carencias_P_10, A.carencias_N_10, A.carencias3_P_10, A.carencias3_N_10, A.plb_P_10, A.plb_N_10, A.plbm_P_10, 
  A.plbm_N_10, A.pobreza_N_12, A.pobreza_m_N_12, A.pobreza_e_N_12, A.vul_car_N_12, A.vul_ing_N_12, A.no_pobv_N_12, A.carencias_N_12,
  A.carencias3_N_12, A.ic_rezedu_N_12, A.ic_asalud_N_12, A.ic_segsoc_N_12, A.ic_cev_N_12, A.ic_sbv_N_12, A.ic_ali_N_12, A.plbm_N_12, 
  A.plb_N_12, A.pobreza_P_12, A.pobreza_m_P_12, A.pobreza_e_P_12, A.vul_car_P_12, A.vul_ing_P_12, A.no_pobv_P_12, A.carencias_P_12,
  A.carencias3_P_12, A.ic_rezedu_P_12, A.ic_asalud_P_12, A.ic_segsoc_P_12, A.ic_cev_P_12, A.ic_sbv_P_12, A.ic_ali_P_12, A.plbm_P_12, 
  A.plb_P_12, A.pobreza_N_14, A.pobreza_m_N_14, A.pobreza_e_N_14, A.vul_car_N_14, A.vul_ing_N_14, A.no_pobv_N_14, A.carencias_N_14,
  A.carencias3_N_14, A.ic_rezedu_N_14, A.ic_asalud_N_14, A.ic_segsoc_N_14, A.ic_cev_N_14, A.ic_sbv_N_14, A.ic_ali_N_14, A.plbm_N_14, 
  A.plb_N_14, A.pobreza_P_14, A.pobreza_m_P_14, A.pobreza_e_P_14, A.vul_car_P_14, A.vul_ing_P_14, A.no_pobv_P_14, A.carencias_P_14,
  A.carencias3_P_14, A.ic_rezedu_P_14, A.ic_asalud_P_14, A.ic_segsoc_P_14, A.ic_cev_P_14, A.ic_sbv_P_14, A.ic_ali_P_14, A.plbm_P_14, 
  A.plb_P_14,

  B.s052,   B.n_0424,   B.s057,   B.s061,   B.s065,   B.s071,   B.s072,   B.s118,   B.s174,   B.s176,   B.s216,   B.s241,   B.u009,   
  B.e003,   B.n_0196,   B.s017,   B.e016,   B.n_0342 

  FROM raw.coneval_estados A
  FULL OUTER JOIN raw.pub_estados B  on  A.cve_ent = B.cve_edo;'



# Create index explorador
PGOPTIONS="--search_path=raw"  psql --db postgresql://$PGUSER:$PGPASSWORD@$PGEC2/predictivadb -c \
'CREATE INDEX raw_semantic_ent   ON clean.explorador_estados   USING btree   (cve_ent COLLATE pg_catalog."default");' 


## create raw explorador dic
PGOPTIONS="--search_path=raw"  psql --db postgresql://$PGUSER:$PGPASSWORD@$PGEC2/predictivadb -c \
'DROP TABLE clean.explorador_estados_dic;'

## create raw semantic dic 
# CHECAR ** NO CORRE EN BASH
PGOPTIONS="--search_path=raw"  psql --db postgresql://$PGUSER:$PGPASSWORD@$PGEC2/predictivadb -c \
"CREATE TABLE clean.explorador_estados_dic
  AS
  SELECT A.id, A.nombre, A.fuente, A.tipo, A.periodo, A.metadata from raw.coneval_estados_dic A WHERE A.id IN (select column_name FROM information_schema.columns WHERE
  table_name='explorador_estados' AND table_schema = 'clean')
  UNION
  SELECT B.cve_prog_dggpb as id, nom_prog_completo as nombre, B.fuente, B.tipo, B.periodo, B.metadata from raw.pub_diccionario_programas B WHERE B.cve_prog_dggpb IN (select column_name FROM information_schema.columns where
  table_name='explorador_estados' AND table_schema =  'clean');"
