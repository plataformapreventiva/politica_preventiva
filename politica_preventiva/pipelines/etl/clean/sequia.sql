/*SQL script to update clean sequia database */

DROP TABLE IF EXISTS clean.sequia cascade;

-- Select last update
CREATE TABLE clean.sequia 
AS (SELECT cve_muni,
           org_cuenca,
           clv_oc,
           con_cuenca,
           cve_conc,
           fecha,
           declaratoria 
    FROM raw.sequia
    WHERE  data_date = (SELECT DISTINCT data_date 
    		        FROM raw.sequia 
    		        ORDER BY data_date 
    		        LIMIT 1 ));
