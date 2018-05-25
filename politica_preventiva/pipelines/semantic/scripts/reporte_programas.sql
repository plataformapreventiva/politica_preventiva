----------------------------------------
-- Semantic table for Reporte Programas
-- Source: CUAPS
----------------------------------------

drop table if exists semantic.reporte_programas cascade;

CREATE TABLE semantic.reporte_programas AS (
SELECT * FROM (
    (SELECT * from tidy.cuaps_programas)
    UNION
    (SELECT * from tidy.cuaps_componentes)
    UNION
    (SELECT * from tidy.cuaps_criterios)) AS cols
 RIGHT JOIN semantic.labels AS labels
 USING (plot, variable, categoria));
