#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

int_columns <- c("tipo_encue","programa","tipo_levan","ent_fed","munici",
                 "locali","doc_identi","doc_edad","per_viv","tot_hog",
                 "tot_per","per_gasto","per_alim","per_info","con_remesa",
                 "ut_cuida1","ut_cuida2","ut_volun1","ut_volun2","ut_repara1",
                 "ut_repara2","ut_limpia1","ut_limpia2","ut_acarre1",
                 "ut_acarre2","gas_alim","gas_vest", "gas_educ", "com_xdia",
                 "cereal", "verduras","frutas", "leguminosa", "caryhue",
                 "lacteos", "grasas", "seg_alim_6","seg_alim_1", "seg_alim_3",
                 "seg_alim_7", "seg_alim_8", "seg_alim_2","seg_alim_9",
                 "seg_alim_4", "seg_alim10", "seg_alim_5", "seg_alim11","seg_alim12",
                 "desaynin", "desay_lug", "n_desay1", "sit_viv","escritura1",
                 "escritura2","esp_nivele", "esp_constr", "esp_local","constr_med",
                 "local_med", "cuart", "cua_dor", "coc_duer", "piso_viv","condi_piso",
                 "cuar_pis_t", "tech_viv", "condi_tech", "muro_viv","condi_muro",
                "escusado",  "uso_exc", "agua", "agua_beb1", "agua_beb2","agua_beb3",
                 "agua_beb4", "agua_beb5","con_dren", "basura", "combustibl", "fogon_coc",
                 "ubic_fog","luz_ele", "est_gas", "sir_estgas", "refri", "sir_refr",
                 "micro","sir_micro", "lavadora", "sir_lava", "tinaco", "sir_tina",
                 "cal_agua","sir_cal", "televis", "sir_tv", "televi_dig", "sir_tvdig",
                 "ant_sky","sir_sky", "videocas", "sir_vid", "telefon", "sir_telef",
                 "celular","sir_cel", "compu", "sir_compu", "clima", "sir_clima", "vehi",
                "sir_vehi", "internet", "sir_inter", "tie_agri", "prop_tier1","prop_tier2",
                 "c_maiz", "c_frij", "c_cere", "c_frut", "c_cana","c_jito", "c_chil",
                 "c_limn", "c_papa", "c_cafe", "c_cate", "c_forr","c_otro","cul_riego",
                 "cul_maquin","cul_anim", "cul_ferorg","cul_ferqui", "cul_plagui",
                 "uso_hid_tr", "caballos", "burros","bueyes", "chivos", "reses", "gallinas",
                 "cerdos", "conejos","proyecto", "p_agri", "p_manu", "p_come", "p_tran",
                 "p_prof","p_educ", "p_sald", "p_recr", "p_aloj", "p_comu", "p_otro",
                "fam_entr1", "tot_entr1", "ren_entr1", "fam_entr2", "tot_entr2",
                "ren_entr2", "fam_entr3", "tot_entr3", "ren_entr3", "fam_entr4","tot_entr4",
                 "ren_entr4", "fam_entr5", "tot_entr5", "ren_entr5","fam_entr6", "tot_entr6",
                 "ren_entr6", "id_corte", "emigro","seg_jefa", "tit_jefa", "jornalero",
                 "tit_jorn", "pet", "tit_pet","leche", "tit_lech", "guarderia", "tit_guard",
                 "est_infan", "tit_estanc","procampo","tit_proc","desp_dif", "tit_dif",
                 "pronabes", "tit_prona","apoy_viv", "tit_apoy", "desp_ini", "tit_ini",
                 "desa_esc", "tit_desy","probecat", "tit_prob", "prog_opr", "tit_pop",
                 "fonaes", "tit_fona","bec_esc", "tit_bec", "bec_trans", "tit_trans",
                 "mic_emp", "tit_mic","otr_prog", "tit_otrpr", "gas_trans", "gas_hig",
                 "piso_gub", "escusa_gub", "focos", "gasto_luz", "licuadora","sir_lic",
                 "fregadero", "sir_freg", "plancha", "sir_plan", "radio","sir_radio",
                 "compu_gub", "tractor", "sir_trac", "pre_agri","ropa", "zapatos", "pan",
                 "fruta", "verdura", "oleche", "huevos","carne", "medicinas", "util_esc",
                 "uni_esc", "tra_esc", "ahorrar","material", "enseres", "p_compra1",
                 "p_compra2", "p_compra3","nin_s_beca", "no_beca", "no_beca1", "no_beca2",
                 "per_auto","per_noaut", "vtot_cua", "vcuarto_d", "vcua_dor", "vpiso_viv",
                "vcua_pis_t", "vtech_viv", "vmuro_viv", "vagua", "vescusado","vuso_exc",
                "vcon_dren", "vluz_ele", "vfocos", "vestgas", "vsir_est","vrefri", "vsir_refr",
                "vlicuadora", "vsir_lic", "vmicro", "vsir_micro","vfregadero", "vsir_freg",
                "vlavadora", "vsir_lava", "vplancha","vsir_plan", "vtinaco", "vsir_tina",
                "vcal_agua", "vsir_cal","vradio", "vsir_radio", "vtelevis", "vsir_tv",
                "vtelev_dig","vsir_tvdig", "vant_sky", "vsir_sky", "vvideocas", "vsir_vid",
               "vtelefon", "vsir_telef", "vcelular", "vsir_cel", "vcompu", "vsir_compu",
               "vclima", "vsir_clima", "vvehiculo", "vsir_vehi", "vtractor","vsir_trac",
                "vinternet", "vsir_inter", "vtraduc", "vverdad","vpobre", "dia_1vis",
                "mes_1vis", "ani_1vis", "dia_uvis", "mes_uvis", "ani_uvis","info_adec",
                "clave_mzna", "ubic_ageb", "ubic_mza", "vial","carr_adm", "carr_tran",
                "carr_num", "carr_km","carr_mts", "cam_tipo", "cam_marg", "cam_km", "cam_mts",
               "tipo_vial", "tipo_vial1", "tipo_vial2", "tipo_vial3", "tipo_asen", "phone",
               "tipo_phone", "tipo_viv", "res_entr", "res_entr1", "duracion", "loc_id",
               "ageb_id","colonia_id", "titular", "punt_i", "ing_est_i", "punt_v",
                "ing_est_v","res_encue", "id_clasifi", "incorporad", "corte_inco", "encuesta_i",
               "elegible", "vialidad_i", "ent_vi_id1", "ent_vi_id2", "vial_post_")

text_columns <- c("folio","folio_sede","folio_enca","folio_iden",
                 "folio_edad", "agua_esp", "p_proy_esp", "desc_otrpr", "otra_no", "observa",
                "entrevis","hora_1ini", "hora_ini", "ageb", "carr_tram", "cam_tram", "calle",
                "num_ext", "num_ext1", "num_int","entre_ca1", "entre_ca2", "calle_atr",
                "referencia", "colonia", "cp", "num_phone", "mail", "hora_1fin","hora_fin")

float_columns <- c( "coord_x","coord_y")

replace_na <- c('&')

# The ETL pipeline will call this function to run UpdateCleanDB task

make_clean <- function(pipeline_task, con){
  df <- tbl(con, dbplyr::in_schema('raw', pipeline_task))
  df %>%
      mutate_at(vars(one_of(int_columns)), as.double) %>%
      mutate_at(vars(one_of(int_columns)), as.integer) %>%
      mutate_at(vars(one_of(float_columns)), as.double) %>%
      mutate(familia_id = str_replace_all(familia_id, '.0$', '')) %>%
      mutate(hogar_id = paste(folio, folio_enca, familia_id, sep= '-')) %>%
      mutate(fecha_creacion = substr(data_date, 1, 4))
}
