
drop table if exists clean.cuaps_componentes cascade;

create temp table temp_tipo_apoyos (
    tipo_pob_apo_cod INT,
    tipo_poblacion TEXT
);

insert into temp_tipo_apoyos values
    (1, 'Personas'),
    (2, 'Hogar'),
    (3, 'Vivienda'),
    (4, 'Actor social'),
    (5, 'Área de atención social'),
    (6, 'Otro');


create table clean.cuaps_componentes as
    select
        cuaps_folio,
        chr_nombre_programa_cuaps,
        replace(to_char(tiene_componentes, '1'), '2', '0') as tiene_componentes,
        replace(to_char(id_componente, '99'), '0', '1') as id_componente,
        nombre_componente,
        obj_compo,
        pob_compo,
        ap_compo,
        id_apoyo,
        nombre_apoyo,
        descr_apoyo,
        tipo_pob_apo_cod,
        tipo_poblacion,
        tipo_pob_apo_otro,
        tipo_apoyo_mon,
        tipo_apoyo_sub,
        tipo_apoyo_esp,
        tipo_apoyo_obra,
        tipo_apoyo_serv,
        tipo_apoyo_cap,
        tipo_apoyo_otro,
        otro_tipo_apoyo,
        period_apoyo,
        otro_period_apoyo,
        monto_apoyo_otor,
        period_monto,
        indic_a,
        indic_b,
        indic_c,
        indic_d,
        indic_e,
        indic_f,
        indic_g,
        indic_h,
        indic_i,
        indic_j,
        indic_k,
        indic_l,
        indic_m,
        indic_n,
        indic_o,
        indic_p,
        indic_q,
        indic_r,
        indic_s,
        indic_t,
        tem_apoyo,
        tem_apoyo_otra_esp,
        apoyo_gen_padron,
        tipo_padron,
        otro_tipo_padron,
        periodicidad_padron,
        actualiza_padron,
        otro_actualiza_padron,
        cuenta_info_geo,
        instru_socioe,
        instru_socioe_cual,
        actualizacion_sedesol,
        data_date
    from raw.cuaps_componentes
    join temp_tipo_apoyos using (tipo_pob_apo_cod)
    where csc_estatus_cuaps_fk = 1
;
