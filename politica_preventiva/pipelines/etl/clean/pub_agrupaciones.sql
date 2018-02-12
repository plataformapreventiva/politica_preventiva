DROP TABLE IF EXISTS clean.pub_agrupaciones;
CREATE TABLE clean.pub_agrupaciones AS (
    WITH clean1 as (
       SELECT *
       FROM raw.pub_agrupaciones p
       WHERE (p.cve_muni IS NULL
       OR  ((p.cve_muni ~ '^\s*\d+\s*$')
       AND substring(p.cve_muni, 1,2)::int >=1
       AND substring(p.cve_muni, 1,2)::int <= 32
       AND (
       -- Aguascalientes
           (p.cve_ent like '%01%' and substring(p.cve_muni, 3,5)::INT >= 1
       	AND substring(p.cve_muni, 3,5)::INT <= 11)
        OR
         -- Baja California
          (p.cve_ent like '%02%' and substring(p.cve_muni, 3,5)::INT >= 1
       	and substring(p.cve_muni, 3,5)::INT <= 5)
        OR
        -- Baja California Sur
           (p.cve_ent like '%03%' and substring(p.cve_muni, 3,5)::INT IN (1,2,3,8,9))
       OR
       	-- Campeche
           (p.cve_ent like '%04%' and substring(p.cve_muni, 3,5)::INT >= 1
       	and substring(p.cve_muni, 3,5)::INT <= 11)
       OR
       	-- Coahuila
           	(p.cve_ent like '%05%' and substring(p.cve_muni, 3,5)::INT >= 1
       	and substring(p.cve_muni, 3,5)::INT <= 38)
       OR
       	-- Colima
         	(p.cve_ent like '%06%' and substring(p.cve_muni, 3,5)::INT >= 1
       	and substring(p.cve_muni, 3,5)::INT <= 10)
       OR
       	-- Chiapas
         	(p.cve_ent like '%07%' and substring(p.cve_muni, 3,5)::INT >= 1
       	and substring(p.cve_muni, 3,5)::INT <= 123)
       OR
       	-- Chihuahua
         	(p.cve_ent like '%08%' and substring(p.cve_muni, 3,5)::INT >= 1
       	and substring(p.cve_muni, 3,5)::INT <= 67)
       OR
       	-- Ciudad de Mexico
       	(p.cve_ent like '%09%' and substring(p.cve_muni, 3,5)::INT >= 2
       	and substring(p.cve_muni, 3,5)::INT <= 17)
       OR
       	-- Durango
       	(p.cve_ent like '%10%' and substring(p.cve_muni, 3,5)::INT >= 1
       	and substring(p.cve_muni, 3,5)::INT <= 39)
       OR
       	-- Guanajuato
       	(p.cve_ent like '%11%' and substring(p.cve_muni, 3,5)::INT >= 1
       	and substring(p.cve_muni, 3,5)::INT <= 46)
       OR
           -- Guerrero
       	(p.cve_ent like '%12%' and substring(p.cve_muni, 3,5)::INT >= 1
       	and substring(p.cve_muni, 3,5)::INT <= 81)
       OR 
       	-- Hidalgo
       	(p.cve_ent like '%13%' and substring(p.cve_muni, 3,5)::INT >= 1
       	and substring(p.cve_muni, 3,5)::INT <= 84)
       OR 
       	-- Jalisco
       	(p.cve_ent like '%14%' and substring(p.cve_muni, 3,5)::INT >= 1
       	and substring(p.cve_muni, 3,5)::INT <= 125)
       OR
       	-- Estado de Mexico
       	(p.cve_ent like '%15%' and substring(p.cve_muni, 3,5)::INT >= 1
       	and substring(p.cve_muni, 3,5)::INT <= 125)
       OR
       	-- Michoacan
       	(p.cve_ent like '%16%' and substring(p.cve_muni, 3,5)::INT >= 1
       	and substring(p.cve_muni, 3,5)::INT <= 113)
       OR 
       	-- Morelos
       	(p.cve_ent like '%17%' and substring(p.cve_muni, 3,5)::INT >= 1
       	and substring(p.cve_muni, 3,5)::INT <= 33)
       OR
       	-- Nayarit
       	(p.cve_ent like '%18%' and substring(p.cve_muni, 3,5)::INT >= 1
       	and substring(p.cve_muni, 3,5)::INT <= 20)
       OR
       	-- Nuevo Leon
       	(p.cve_ent like '%19%' and substring(p.cve_muni, 3,5)::INT >= 1
       	and substring(p.cve_muni, 3,5)::INT <= 51)
       OR
       	-- Oaxaca
       	(p.cve_ent like '%20%' and substring(p.cve_muni, 3,5)::INT >= 1
       	and substring(p.cve_muni, 3,5)::INT <= 570)
       OR
       	-- Puebla
       	(p.cve_ent like '%21%' and substring(p.cve_muni, 3,5)::INT >= 1
       	and substring(p.cve_muni, 3,5)::INT <= 217)
       OR
       	-- Queretaro
       	(p.cve_ent like '%22%' and substring(p.cve_muni, 3,5)::INT >= 1
       	and substring(p.cve_muni, 3,5)::INT <= 18)
       OR
       	-- Quintana Roo
       	(p.cve_ent like '%23%' and substring(p.cve_muni, 3,5)::INT >= 1
       	and substring(p.cve_muni, 3,5)::INT <= 11)
       OR
       	-- San Luis Potosi
       	(p.cve_ent like '%24%' and substring(p.cve_muni, 3,5)::INT >= 1
       	and substring(p.cve_muni, 3,5)::INT <= 17)
       OR
       	-- Sinaloa
       	(p.cve_ent like '%25%' and substring(p.cve_muni, 3,5)::INT >= 1
       	and substring(p.cve_muni, 3,5)::INT <= 18)
       OR
       	-- Sonora
       	(p.cve_ent like '%26%' and substring(p.cve_muni, 3,5)::INT >= 1
       	and substring(p.cve_muni, 3,5)::INT <= 72)
       OR
       	-- Tabasco
       	(p.cve_ent like '%27%' and substring(p.cve_muni, 3,5)::INT >= 1
       	and substring(p.cve_muni, 3,5)::INT <= 17)
       OR
       	-- Tamaulipas
       	(p.cve_ent like '%28%' and substring(p.cve_muni, 3,5)::INT >= 1
       	and substring(p.cve_muni, 3,5)::INT <= 43)
       OR
       	-- Tlaxcala
       	(p.cve_ent like '%29%' and substring(p.cve_muni, 3,5)::INT >= 1
       	and substring(p.cve_muni, 3,5)::INT <= 60)
       OR
       	-- Veracruz
       	(p.cve_ent like '%30%' and substring(p.cve_muni, 3,5)::INT >= 1
       	and substring(p.cve_muni, 3,5)::INT <= 212)
       OR
       	-- Yucatan
       	(p.cve_ent like '%31%' and substring(p.cve_muni, 3,5)::INT >= 1
       	and substring(p.cve_muni, 3,5)::INT <= 106)
       OR
       	-- Zacatecas
       	(p.cve_ent like '%32%' and substring(p.cve_muni, 3,5)::INT >= 1
       	and substring(p.cve_muni, 3,5)::INT <= 58)
       ) ))
  ) 
  SELECT anio, --1
	 cve_ent, --2
	 cve_muni, --3
	 cve_programa,  --4
	 cve_padron, --5
	 tipo, --6
	 temporalidad, --7
	 mes, --8
	 nivel, --9
	 grupo, --10
         n_beneficiarios, --11
	 max(suma_importe) AS suma_importe, --12
         actualizacion_sedesol, --13
	 data_date --14
  FROM clean1
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,13,14
);
