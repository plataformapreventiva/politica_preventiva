create temp table temp_tipo_apoyos
        (
        tipo_pob_apo INT
        tipo_pob_apo_txt TEXT
        )

insert into temp_tipo_apoyos values
        (1, 'Personas')
        (2, 'Hogar')
        (3, 'Vivienda')
        (4, 'Actor social')
        (5, 'Área de atención social')
        (6, 'Otro')


create table clean.cuaps_componentes as(
        select
                cuaps_folio,
                nom_prog,
                replace(to_char(tiene_componentes, '1'), '2', '0') as tiene_componentes,
                replace(to_char(id_componente, '99'), '0', '1') as id_componente,
                nombre_componente,
                obj_compo,
                pob_compo,
                ap_compo,
                id_apoyo,
                nombre_apoyo,
                descr_apoyo,
                tipo_pob_apo_txt,
                otro_tipo_pob_esp,
                tipo_apoyo_*,
                period_apoyo,

        from raw.cuaps_componentes
        join temp_tipo_apoyos using (tipo_pob_apo)
);
