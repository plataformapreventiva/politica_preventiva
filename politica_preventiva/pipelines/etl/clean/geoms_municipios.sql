drop table if exists clean.geoms_municipios cascade;

create table clean.geoms_municipios as (
select cve_ent || lpad(cve_mun, 3, '0') as cve_muni,
	   nom_muni,
	   wkt,
	   ST_AsText(ST_centroid(wkt)) AS centroide,
	   ST_X(ST_centroid(wkt)) as longitud,
	   ST_Y(ST_centroid(wkt)) as latitud,
	   actualizacion_sedesol,
	   data_date
from raw.geoms_municipios
order by cve_muni
);
