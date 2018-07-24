drop table if exists clean.inventario_coneval_estatales cascade;

create table clean.inventario_coneval_estatales as(
    select * from raw.inventario_coneval_estatales
);
