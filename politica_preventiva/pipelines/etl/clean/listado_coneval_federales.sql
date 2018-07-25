drop table if exists clean.listado_coneval_federales cascade;

create table clean.listado_coneval_federales as(
    select * from raw.listado_coneval_federales
);
