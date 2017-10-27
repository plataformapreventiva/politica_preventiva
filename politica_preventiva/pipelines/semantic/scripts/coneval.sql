-------------------------------------------
-- semantic table coneval (estados y municipios)
-- Sedesol Oct 24 2017
-------------------------------------------

drop table if exists semantic.coneval cascade;

CREATE TABLE semantic.coneval AS (
WITH dicc AS (
	SELECT id, nombre, fuente
	FROM raw.coneval_estados_dic
	GROUP BY 1,2,3),
all_coneval AS (
	SELECT cve_ent AS clave,
	   variable,
	   tipo,
	   'estatal' as nivel,
	   valor,
	   pob_tot,
	   data_date,
	   substring(data_date, 1,4)::int,
	   actualizacion_sedesol
	FROM tidy.coneval_estados
	UNION
	SELECT 
	   	cve_muni AS clave,
	   	variable,
	   	tipo,
		'municipal' as nivel,
	   	valor,
	   	pob_tot,
	   	data_date,
	   	substring(data_date, 1,4)::int,
	   	actualizacion_sedesol
	FROM tidy.coneval_municipios)
SELECT * 
FROM all_coneval
LEFT JOIN dicc
ON dicc.id = all_coneval.variable
);
