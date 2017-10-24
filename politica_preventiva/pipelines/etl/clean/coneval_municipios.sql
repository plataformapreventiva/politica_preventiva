
drop table if exists clean.coneval_municipios cascade;

create table clean.coneval_municipios as (
select *
from raw.coneval_municipios
);
