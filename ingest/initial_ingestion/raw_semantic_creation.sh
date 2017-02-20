""" 
Script to create raw.semantic tables for the 'explorador' module

"""

###############################
# Raw Semantic Municipios
###############################

## Delete raw semantic
$( PGOPTIONS="--search_path=raw"  psql --db postgresql://$PGUSER:$PGPASSWORD@predictivadb.cshyqil2j46y.us-west-2.rds.amazonaws.com/predictivadb -c 'DROP TABLE raw.semantic_municipios;' )

## create raw semantic
$( PGOPTIONS="--search_path=raw"  psql --db postgresql://$PGUSER:$PGPASSWORD@predictivadb.cshyqil2j46y.us-west-2.rds.amazonaws.com/predictivadb -c \
'CREATE TABLE raw.semantic_municipios AS SELECT
  A.cve_ent, A.nom_ent, A.cve_muni, A.nom_mun, A.pobtot_ajustada, A.pobreza_p_10 , A.pobreza_e_p_10 , A.pobreza_m_p_10 , A.vul_car_p_10 , A.vul_ing_p_10 , A.no_pobv_p_10 , A.ic_rezedu_p_10 , A.ic_asalud_p_10 , A.ic_segsoc_p_10 , A.ic_cev_p_10 , A.ic_sbv_p_10 , A.ic_ali_p_10 , A.carencias_p_10 , A.carencias3_p_10 , A.plb_p_10 , A.pblm_p_10 , A.rankin_p_10, A.rankin_pe_10, A.pobreza_n_10, A.pobreza_e_n_10, A.pobreza_m_n_10, A.vul_car_n_10, A.vul_ing_n_10, A.no_pobv_n_10, A.ic_rezedu_n_10, A.ic_asalud_n_10, A.ic_segsoc_n_10, A.ic_cev_n_10, A.ic_sbv_n_10, A.ic_ali_n_10, A.carencias_n_10, A.carencias3_n_10, A.plb_n_10, A.pblm_n_10, A.perrankin_p_10, A.perrankin_pe_10, A.cppobreza , A.cppobreza_e , A.cppobreza_m , A.cpvul_car , A.cpic_rezedu , A.cpic_asalud , A.cpic_segsoc , A.cpic_cv , A.cpic_sbv , A.cpic_ali , A.cpcarencias , A.cpcarencias3 , A.cpplb , A.cpplbm , A.p_rez_edu_90 , A.p_rez_edu_00 , A.p_rez_edu_10 , A.p_ser_sal_00 , A.p_ser_sal_10 , A.p_viv_pisos_90 , A.p_viv_pisos_00 , A.p_viv_pisos_10 , A.p_viv_muros_90 , A.p_viv_muros_00 , A.p_viv_muros_10 , A.p_viv_techos_90 , A.p_viv_techos_00 , A.p_viv_techos_10 , A.p_viv_hacin_90 , A.p_viv_hacin_00 , A.p_viv_hacin_10 , A.p_viv_agu_entub_90 , A.p_viv_agu_entub_00 , A.p_viv_agu_entub_10 , A.p_viv_dren_90 , A.p_viv_dren_00 , A.p_viv_dren_000 , A.p_viv_elect_90 , A.p_viv_elect_00 , A.p_viv_elect_10 , A.pobreza_alim_90 , A.pobreza_alim_00 , A.pobreza_alim_10 , A.pobreza_cap_90 , A.pobreza_cap_00 , A.pobreza_cap_10 , A.pobreza_patrim_90 , A.pobreza_patrim_00 , A.pobreza_patrim_10 , A.gini_90 , A.gini_00 , A.gini_10,
  B."S052", B."0424" , B."S057" , B."S061" , B."S065" , B."S071" , B."S072" , B."S118" , B."S174" , B."S176" , B."S216" , B."S241", B."U009" , B."E003" , B."0196" , B."S017" , B."E016" , B."0342",
  D.pobtot_00, D.pobtot_05, D.pobtot_10, D.pobtot_15, D.porc_pob_15_analfa00, D.porc_pob_15_analfa05, D.porc_pob_15_analfa10, D.porc_pob_15_analfa15, D.porc_pob_snservsal00, D.porc_pob_snservsal05, D.porc_pob_snservsal10, D.porc_pob_snservsal15, D.porc_vivsnsan00, D.porc_vivsnsan05, D.porc_vivsnsan10, D.porc_vivsnsan15, D.porc_snaguaent00, D.porc_snaguaent05, D.porc_snaguaent10, D.porc_snaguaent15, D.porc_vivsndren00, D.porc_vivsndren05, D.porc_vivsndren10, D.porc_vivsndren15, D.porc_vivsnenergia00, D.porc_vivsnenergia05, D.porc_vivsnenergia10, D.porc_vivsnenergia15, D.irez_soc00, D.irez_soc05, D.irez_soc10, D.irez_soc15, D.gdo_rezsoc00, D.gdo_rezsoc05, D.gdo_rezsoc10, D.gdo_rezsoc15, D.l_ocupnac00, D.l_ocupnac05, D.l_ocupnac10, D.l_ocupnac15,
  E.complejidad_2014
  FROM raw.coneval_municipios A
  FULL OUTER JOIN raw.pub_municipios B on A.cve_muni = B.cve_muni
  FULL OUTER JOIN raw.general_municipios C on A.cve_muni = C.cve_muni
  FULL OUTER JOIN raw.irs_municipios D on A.cve_muni = D.cve_muni
  FULL OUTER JOIN raw.complejidad_municipios E on A.cve_muni = E.cve_muni;' )

# Create index
$( PGOPTIONS="--search_path=raw"  psql --db postgresql://$PGUSER:$PGPASSWORD@predictivadb.cshyqil2j46y.us-west-2.rds.amazonaws.com/predictivadb -c \
'CREATE INDEX raw_semantic_mun   ON raw.semantic_municipios   USING btree   (cve_muni COLLATE pg_catalog."default");' )

#CREATE INDEX semantic_mun_ent
#ON "semantic".semantic_municipios
#USING btree
#(cve_ent COLLATE pg_catalog."default");


## create raw semantic dic

$( PGOPTIONS="--search_path=raw"  psql --db postgresql://$PGUSER:$PGPASSWORD@predictivadb.cshyqil2j46y.us-west-2.rds.amazonaws.com/predictivadb -c \
'DROP TABLE raw.semantic_municipios_dic;' )

## create raw semantic dic 
# CHECAR ** NO CORRE EN BASH
$( PGOPTIONS="--search_path=raw"  psql --db postgresql://$PGUSER:$PGPASSWORD@predictivadb.cshyqil2j46y.us-west-2.rds.amazonaws.com/predictivadb -c \
'CREATE TABLE raw.semantic_municipios_dic
  AS
  SELECT A.id, A.indicador as nombre, A.fuente, A.tipo, A.periodo, A.metadata from raw.coneval_municipios_dic A WHERE A.id IN (select column_name FROM information_schema.columns WHERE
  table_name='semantic_municipios' AND table_schema = 'raw')
  UNION
  SELECT B.cve_prog_dggpb as id, nom_prog_completo as nombre, B.fuente, B.tipo, B.periodo, B.metadata from raw.pub_diccionario_programas B WHERE B.cve_prog_dggpb IN (select column_name FROM information_schema.columns where
  table_name='semantic_municipios' AND table_schema = 'raw')
  UNION
  SELECT C.id, C.nombre, C.fuente, C.tipo, C.periodo, C.metadata from raw.general_municipios_dic C WHERE C.id IN (select column_name FROM information_schema.columns WHERE
  table_name='semantic_municipios' AND table_schema = 'raw')
  UNION
  SELECT D.id, D.nombre, D.fuente, D.tipo, D.periodo, D.metadata from raw.complejidad_dic D WHERE D.id IN (select column_name FROM information_schema.columns WHERE
  table_name='semantic_municipios' AND table_schema = 'raw')
  UNION
  SELECT E.id, E.nombre, E.fuente, E.tipo, E.periodo, E.metadata from "raw".irs_dic E WHERE E.id IN (select column_name FROM information_schema.columns WHERE
  table_name='semantic_municipios' AND table_schema = 'raw');' )


###############################
# Raw Semantic Estados
###############################

$( PGOPTIONS="--search_path=raw"  psql --db postgresql://$PGUSER:$PGPASSWORD@predictivadb.cshyqil2j46y.us-west-2.rds.amazonaws.com/predictivadb -c  \ 
"DROP TABLE semantic.SEMANTIC_ESTADOS;" )

$( PGOPTIONS="--search_path=raw"  psql --db postgresql://$PGUSER:$PGPASSWORD@predictivadb.cshyqil2j46y.us-west-2.rds.amazonaws.com/predictivadb -c \
CREATE TABLE semantic.semantic_estados
  as
  select
  A.cve_ent, A.pobreza_P_10, A.pobreza_N_10, A.pobreza_e_P_10, A.pobreza_e_N_10, A.pobreza_m_P_10, A.pobreza_m_N_10, A.vul_car_P_10, A.vul_car_N_10, A.vul_ing_P_10, A.vul_ing_N_10, A.no_pobv_P_10, A.no_pobv_N_10, A.ic_rezedu_P_10, A.ic_rezedu_N_10, A.ic_asalud_P_10, A.ic_asalud_N_10, A.ic_segsoc_P_10, A.ic_segsoc_N_10, A.ic_cev_P_10, A.ic_cev_N_10, A.ic_sbv_P_10, A.ic_sbv_N_10, A.ic_ali_P_10, A.ic_ali_N_10, A.carencias_P_10, A.carencias_N_10, A.carencias3_P_10, A.carencias3_N_10, A.plb_P_10, A.plb_N_10, A.plbm_P_10, A.plbm_N_10, A.pobreza_N_12, A.pobreza_m_N_12, A.pobreza_e_N_12, A.vul_car_N_12, A.vul_ing_N_12, A.no_pobv_N_12, A.carencias_N_12, A.carencias3_N_12, A.ic_rezedu_N_12, A.ic_asalud_N_12, A.ic_segsoc_N_12, A.ic_cev_N_12, A.ic_sbv_N_12, A.ic_ali_N_12, A.plbm_N_12, A.plb_N_12, A.pobreza_P_12, A.pobreza_m_P_12, A.pobreza_e_P_12, A.vul_car_P_12, A.vul_ing_P_12, A.no_pobv_P_12, A.carencias_P_12, A.carencias3_P_12, A.ic_rezedu_P_12, A.ic_asalud_P_12, A.ic_segsoc_P_12, A.ic_cev_P_12, A.ic_sbv_P_12, A.ic_ali_P_12, A.plbm_P_12, A.plb_P_12, A.pobreza_N_14, A.pobreza_m_N_14, A.pobreza_e_N_14, A.vul_car_N_14, A.vul_ing_N_14, A.no_pobv_N_14, A.carencias_N_14, A.carencias3_N_14, A.ic_rezedu_N_14, A.ic_asalud_N_14, A.ic_segsoc_N_14, A.ic_cev_N_14, A.ic_sbv_N_14, A.ic_ali_N_14, A.plbm_N_14, A.plb_N_14, A.pobreza_P_14, A.pobreza_m_P_14, A.pobreza_e_P_14, A.vul_car_P_14, A.vul_ing_P_14, A.no_pobv_P_14, A.carencias_P_14, A.carencias3_P_14, A.ic_rezedu_P_14, A.ic_asalud_P_14, A.ic_segsoc_P_14, A.ic_cev_P_14, A.ic_sbv_P_14, A.ic_ali_P_14, A.plbm_P_14, A.plb_P_14,
  B."S052", B."0424", B."S057", B."S061", B."S065", B."S071", B."S072", B."S118", B."S174", B."S176", B."S216", B."S241", B."U009", B."E003", B."0196", B."S017", B."E016", B."0342"
  from raw.coneval_estados A
    join raw.pub_estados B on A.cve_ent = B.cve_ent; )


CREATE INDEX semantic_ent
  ON semantic.semantic_estados
  USING btree
  (cve_muni COLLATE pg_catalog."default");



drop table semantic.semantic_estados_dic;

CREATE TABLE semantic.semantic_estados_dic
  AS

  SELECT A.id, A.nombre, A.fuente, A.tipo, A.periodo, A.metadata from raw.coneval_estados_dic A WHERE A.id IN (select column_name FROM information_schema.columns WHERE table_name='semantic_estados' AND table_schema = 'semantic')

  UNION

  SELECT B.cve_prog_dggpb as id, nom_prog_completo as nombre, B.fuente, B.tipo, B.periodo, B.metadata from raw.pub_diccionario_programas B WHERE B.cve_prog_dggpb IN (select column_name FROM information_schema.columns WHERE table_name='semantic_estados' AND table_schema = 'semantic');




###############################
# Raw Semantic INDEX
###############################

DROP TABLE  raw.semantic_index

CREATE TABLE raw.semantic_index
AS
SELECT
A.cve_muni,
A.natur, A.inundacion, A.temperatura, A.sequia, A.human, A.indice_amenaza,
B.respuesta, B.institut, B.indice_norm, B. flexibilidad_financiera, B.infrast, B.comunic, B.infrast_fisic, B.salud, B.complejidad_2014,
C.vulnerable, C.vulnerable_groups, C.unprooted, C.o_vulnerable, C.socioeconomica, C.gini_10, C.irez_soc15

from raw.SEMANTIC_AMENAZAS A
  join raw.SEMANTIC_CAPACIDAD_RESPUESTA B on A.cve_muni = B.cve_muni
  join "raw".SEMANTIC_VULNERABILIDAD C on A.cve_muni = C.cve_muni;

CREATE INDEX semantic_index_mun ON "raw".semantic_index
USING btree (cve_muni COLLATE pg_catalog."default");

# CREATE SEMANTIC INDEX DIC
CREATE TABLE raw.semantic_index_dic
AS
SELECT A.id, A.nombre from raw.semantic_amenazas_dic A WHERE A.id IN (select column_name FROM information_schema.columns WHERE
table_name='semantic_index')
UNION
SELECT A.id, A.nombre from raw.semantic_capacidad_respuesta_dic A WHERE A.id IN (select column_name FROM information_schema.columns WHERE
table_name='semantic_index')
UNION
SELECT A.id, A.nombre from raw.semantic_vulnerabilidad_dic A WHERE A.id IN (select column_name FROM information_schema.columns WHERE
table_name='semantic_index');