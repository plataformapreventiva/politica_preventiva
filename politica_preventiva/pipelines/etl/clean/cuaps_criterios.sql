
drop table if exists clean.cuaps_criterios;

create table clean.cuaps_criterios as
    select * from raw.cuaps_criterios
;
