CREATE OR REPLACE FUNCTION analyse_schema(table_schema_var text)
  RETURNS TEXT AS
$BODY$
DECLARE
  rec               RECORD;
  row               RECORD;
  column_type       TEXT;
  numeric_precision INT;
  table_name_var    TEXT;
  column_name       TEXT;
BEGIN
  -- create a table for the results
  DROP TABLE IF EXISTS public.table_stats;
  CREATE TABLE public.table_stats (
    table_schema TEXT,
    table_name   TEXT,
    column_name  TEXT,
    data_type    TEXT,
    measure      TEXT,
    value        TEXT
  );

  FOR rec IN
  SELECT *
  FROM information_schema.columns
  WHERE table_schema = table_schema_var
  ORDER BY table_name
  LOOP
    column_type = rec.data_type::TEXT;
    numeric_precision = rec.numeric_precision;
    column_name = rec.column_name;
    table_name_var = rec.table_name;

    IF numeric_precision NOTNULL
    THEN
      EXECUTE
      'SELECT
        min("' || column_name || '")::TEXT AS min,
          max("' || column_name || '")::TEXT AS max
        FROM  ' || table_schema_var || '."' || table_name_var || '"'
      INTO row;
      INSERT INTO public.table_stats VALUES
        (table_schema_var,
         table_name_var,
         column_name,
         column_type,
         'min',
         row.min :: TEXT);
      INSERT INTO public.table_stats VALUES
        (table_schema_var,
         table_name_var,
         column_name,
         column_type,
         'max',
         row.max :: TEXT);
    END IF;
    -- now what we do for every columns
    EXECUTE
    'SELECT
      count(*) AS num_records,
      count( DISTINCT "' || column_name || '") AS num_distinct_values,
      sum("' || column_name || '" ISNULL::INT) AS count_nulls,
      mode() WITHIN GROUP (ORDER BY "' || column_name || '")::TEXT as mode_value
    FROM  ' || table_schema_var || '."' || table_name_var || '"'
    INTO row;
    INSERT INTO public.table_stats VALUES
        (table_schema_var,
         table_name_var,
         column_name,
         column_type,
         'num_records',
         row.num_records :: TEXT);
    INSERT INTO public.table_stats VALUES
        (table_schema_var,
         table_name_var,
         column_name,
         column_type,
         'num_distinct_values',
         row.num_distinct_values :: TEXT);
    INSERT INTO public.table_stats VALUES
        (table_schema_var,
         table_name_var,
         column_name,
         column_type,
         'count_nulls',
         row.count_nulls :: TEXT);
    INSERT INTO public.table_stats VALUES
        (table_schema_var,
         table_name_var,
         column_name,
         column_type,
         'mode',
         row.mode_value :: TEXT);

    RAISE NOTICE 'Column type: %', column_type;


  END LOOP;
  RETURN 'Results saved in public.table_stats';
END
$BODY$
LANGUAGE plpgsql VOLATILE;
