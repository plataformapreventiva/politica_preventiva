drop table if exists clean.geoms_estados cascade;

create table clean.geoms_estados as (

select cve_ent,
	   wkt,
	   ST_X(ST_centroid(wkt)) as lon,
	   ST_Y(ST_centroid(wkt)) as lat,
	   actualizacion_sedesol,
	   data_date
from raw.geoms_estados
order by cve_ent
);
