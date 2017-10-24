-------------------------------------------
-- tidy for cuenta publica trimestral
-- Sedesol Oct 20 2017
-------------------------------------------
drop table if exists clean.cuenta_publica_trimestral cascade;

alter table semantic.cuenta_publica_trimestral add column cve_programa varchar;
update semantic.cuenta_publica_trimestral set cve_programa = id_modalidad || lpad(id_pp, 3, '0');
