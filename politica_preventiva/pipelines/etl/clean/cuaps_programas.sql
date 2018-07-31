
drop table if exists clean.cuaps_programas cascade;

create table clean.cuaps_programas as
    select
        cuaps_folio,
        fh_levantamiento,
        chr_nombre_programa_cuaps,
        chr_clave_prespupuestal_pro,
        chr_descripcion_dependencia,
        chr_cve_dependencia,
        chr_unidad_administrativa_cuaps,
        chr_cve_unidad_adm_cuaps,
        orden_gob,
        part_ua,
        anio_inicio_prog,
        doc_norma,
        obj_gral_prog,
        obj_esp_prog_1,
        obj_esp_prog_2,
        obj_esp_prog_3,
        obj_esp_prog_4,
        obj_esp_prog_5,
        obj_esp_prog_6,
        obj_esp_prog_7,
        obj_esp_prog_8,
        obj_esp_prog_9,
        obj_esp_prog_10,
        obj_esp_prog_otros,
        pob_obj_prog,
        cobertura_geo,
        cve_entidad_federativa,
        nom_entidad_federativa,
        cve_municipio,
        nom_municipio,
        pob_poten,
        pob_poten_um,
        num_pob_objet,
        pob_aten_efa,
        pob_aten_efa_um,
        coparticipacion,
        cop_tipart_1,
        cop_tipart_2,
        cop_tipart_3,
        cop_tipart_4,
        cop_tipart_5,
        cop_tipart_otros,
        cop_npart_1,
        cop_npart_2,
        cop_npart_3,
        cop_npart_4,
        cop_npart_5,
        cop_npart_otros,
        cop_ordgob_1,
        cop_ordgob_2,
        cop_ordgob_3,
        cop_ordgob_4,
        cop_ordgob_5,
        cop_ordgob_otros,
        cop_tipint_1,
        cop_tipint_2,
        cop_tipint_3,
        cop_tipint_4,
        cop_tipint_5,
        cop_tipint_otros,
        der_social_edu,
        der_social_sal,
        der_social_alim,
        der_social_viv,
        der_social_mam,
        der_social_tra,
        der_social_segsoc,
        der_social_nodis,
        der_social_beco,
        der_social_ning,
        tiene_componentes,
        actualizacion_sedesol,
        data_date
    from raw.cuaps_programas
;