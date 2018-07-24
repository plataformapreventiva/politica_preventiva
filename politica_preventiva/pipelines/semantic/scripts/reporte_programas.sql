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
    (SELECT * from tidy.cuaps_criterios)
    UNION
    (SELECT actualizacion_sedesol, data_date, categoria, valor, plot, variable, plot_prefix from tidy.inventario_coneval_federales)
    UNION
    (SELECT actualizacion_sedesol, data_date, categoria, valor, plot, variable, plot_prefix from tidy.inventario_coneval_estatales)
    UNION
    (SELECT actualizacion_sedesol, data_date, categoria, sum(valor) as valor, plot_prefix as plot, variable, plot_prefix
        from tidy.cuaps_programas
        where plot_prefix = 's02_estados'
        and variable = 'cve_ent',
        group by categoria)) AS cols
 LEFT JOIN semantic.reporte_programas_labels AS labels
 USING (plot_prefix, variable, categoria));
