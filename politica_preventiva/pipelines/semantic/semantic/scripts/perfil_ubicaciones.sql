DROP TABLE IF EXISTS semantic.perfil_ubicaciones;

CREATE TEMP TABLE labels AS(
    var_id TEXT,
    label TEXT
);

"""
Obtener schema y table_name.
Si schema es clean o raw, buscar en raw. Si es features, buscar en features.
Obtener [(id, label)] de schema.table_name_dic, y appendearlo a la tabla temporal
Hacer un join entre labels y tidy
"""

CREATE OR REPLACE FUNCTION append_labels(
    table_schema TEXT,
    table_name TEXT,
    varlist TEXT[]
)
RETURNS VOID AS $$
BEGIN
    EXECUTE('SELECT ')
RETURN
END; LANGUAGE plpgsql;








CREATE TABLE semantic.perfil_ubicaciones AS(

);
