
drop table if exists clean.coneval_municipios_2010 cascade;

create table clean.coneval_municipios_2010 as (
select *
from raw.coneval_municipios_2010
);
