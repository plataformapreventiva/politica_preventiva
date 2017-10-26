-------------------------------------------
-- semantic table coneval (estados y municipios)
-- Sedesol Oct 24 2017
-------------------------------------------

drop table if exists semantic.coneval cascade;

create table semantic.coneval as (
    select cve_ent,
	   '' as cve_muni,
	   variable,
	   tipo,
	   valor,
	   pob_tot,
	   data_date,
	   actualizacion_sedesol
    from tidy.coneval_estados
    union
    select ent as cve_ent,
	   cve_muni,
	   variable,
	   tipo,
	   valor,
	   pob_tot,
	   data_date,
	   actualizacion_sedesol
    from tidy.coneval_municipios
);
