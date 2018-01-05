drop table if exists clean.defunciones_fetales cascade;

create table clean.defunciones_fetales as (
select  lpad(ent_ocurr::TEXT, 2, '0') || lpad(mun_ocurr::TEXT, 3, '0') as cve_muni,
        causa_def,
        sex_prod,
        eda_prod,
        pes_prod,
        de_un_emba,
        atn_pren,
        emba_fue,
        ocu_part,
        dia_regis,
        mes_regis,
        anio_regis,
        dia_ocurr,
        mes_ocurr,
        anio_ocur,
        sitio_ocur,
        certific,
        atencion,
        tip_abor,
        pro_expu,
        nac_vivo,
        nac_muer,
        derecho,
        eda_madr,
        edo_civil,
        ocu_madr,
        esc_madr,
        horas,
        minutos,
        cond_mad,
        dia_cert,
        mes_cert,
        anio_cert,
        violencia,
        par_agre,
        lengua_ind,
        cond_act,
        nacionalid,
        paren_info,
        consultas,
        edo_piel,
        dis_re_oax,
        actualizacion_sedesol,
        data_date
from raw.defunciones_fetales
);