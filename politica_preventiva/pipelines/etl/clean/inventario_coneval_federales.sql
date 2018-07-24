drop table if exists clean.inventario_coneval_federales cascade;

create table clean.inventario_coneval_federales as(
    select * from raw.inventario_coneval_federales
);
