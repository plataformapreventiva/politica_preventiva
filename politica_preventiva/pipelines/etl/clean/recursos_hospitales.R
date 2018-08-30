#!/usr/bin/env Rscript
library(rlang)
library(tidyverse)

text_columns <- c('clues','entidad','municipio','localidad','tipodeestablecimiento','tipologia','nombredelestablecimiento')

int_columns <- c('claveentidad','clavemunicipio','clavelocalidad','unidades','e13','c1301','c1302',
                 'c1303','c1304','c1305','c1306','c1307','c1308','c1309','c1310','c1311','c1312','c1313',
                 'c1314','c1315','c1316','c1317','c1318','c1319','c1320','c1321','e14','c1400','c1401',
                 'c1402','c1404','c1404','c1405','c1406','c1408','e15','c1501','c1502',
                 'c1505','c1505','c1505','c1506','c1507','c1508','c1509','c1601','c1602','c1701','c1702',
                 'e1819','c1801','c1803','c1804','c1805','c1806','c1807','c1808','c1809','c1810','c1811',
                 'c1812','c1813','c1814','c1815','c1816','c1817','c1818','c1819','c1820','c1821','c1822',
                 'c1823','c1824','c1825','c1826','c1901','c1902','c1903','c1904','e20','c2001','c2002',
                 'c2003','c2004','c2005','e21','c2101','c2102','c2103','c2104','e22','c2201','c2202','c2204',
                 'e23','c2301','c2302','c2303','c2304','c2305','c2306','c2308', 'e24','c2401','c2402','c2403',
                 'c2404','c2405','c2406','c2407','c2408','c2409','c2410','c2411','c2412','c2413','c2414','c2415',
                 'e25','c2501','c2502','c2503','c2504','c2506','c1706','c1707','c1710','c1711','c1725','c1729',
                 'c1735','c1738','c1744','c1745','c1746','c1747','c1749','c1750')

query <- 'SELECT *, LPAD(claveentidad::text, 2, \'0\') || LPAD(clavemunicipio::text, 3, \'0\') as cve_muni FROM raw.recursos_hospitales'

make_clean <- function(pipeline_task, con){
  df <- tbl(con, sql(query))
  df %>%
    mutate_at(int_columns, funs(as.integer(.))) %>%
    mutate_at(text_columns, funs(as.character(.)))
}
