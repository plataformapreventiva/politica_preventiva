-------------------------------------------
-- semantic table cuenta_publica
-- Sedesol Oct 20 2017
-------------------------------------------

drop table if exists semantic.cuenta_publica cascade;
drop table if exists tbl cascade;

create temp table tbl as(
(select ciclo,
		id_ramo,
		desc_ramo,
		id_modalidad,
		desc_modalidad,
		desc_objeto_del_gasto as desc_concepto,
		desc_tipogasto,
		monto_modificado,
		id_modalidad || lpad(id_pp, 3, '0') as cve_programa 
from raw.cuenta_publica_anual)
union
(select ciclo,
		id_ramo,
		desc_ramo,
		id_modalidad,
		desc_modalidad,
		desc_concepto,
		desc_tipogasto,
		monto_modificado,
		cve_programa
from semantic.cuenta_publica_trimestral)
);


create table semantic.cuenta_publica as (
select ciclo, cve_programa, json_build_object('tipo',desc_tipogasto,'value',
	sum(monto_modificado),'hijos',
	array_to_json(array_agg(json_build_object('tipo', desc_concepto, 'value', monto_modificado))))
from (
	select  ciclo, cve_programa, desc_tipogasto, desc_concepto, sum(monto_modificado) as monto_modificado
	from tbl
	group by ciclo, cve_programa, desc_tipogasto, desc_concepto) as A
group by ciclo, cve_programa, desc_tipogasto
);

