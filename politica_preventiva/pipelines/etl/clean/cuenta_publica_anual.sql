-------------------------------------------
-- tidy for cuenta publica anual
-- Sedesol Oct 20 2017
-------------------------------------------
drop table if exists clean.cuenta_publica_anual cascade;

alter table semantic.cuenta_publica_anual add column cve_programa varchar;
update semantic.cuenta_publica_anual set cve_programa = id_modalidad || lpad(id_pp, 3, '0');
	