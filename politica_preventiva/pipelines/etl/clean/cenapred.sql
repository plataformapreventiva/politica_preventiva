
drop table if exists clean.cenapred cascade;

create table clean.cenapred as (
select  lpad(cve_muni::TEXT, 5, '0') as cve_muni,
		data_date,
		gp_bajaste,
		gp_ciclnes,
        gp_granizo,
        gp_inundac,
        gp_nevadas,
        gp_sequia2,
        gp_sismico,
        gp_susinfl,
        gp_sustox,
        gp_tormele,
        gp_tsunami,
        gp_ondasca, 
        actualizacion_sedesol
from raw.cenapred
);
