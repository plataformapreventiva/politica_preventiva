
--#############################
-- Clean PUB Municipios 
--    transposing programs ids to columns
--#############################

-- Create Pivot table in a TEMP table called temp_pub_municipios
SELECT  * FROM public.colpivot('temp_pub_municipios',
						'SELECT * from semantic.padron_municipios 
						 WHERE type = ''total'' and cve_ent is not null',
                        array['cve_ent', 'cve_muni'], --key
                        ARRAY['cve_padron'],--class_cols
                        '#.n_beneficiarios', --the actuale value transposed
                        null);

-- drop and create clean.pub_municipios
DROP TABLE if exists clean.pub_municipios;

CREATE TABLE clean.pub_municipios AS (
    SELECT * FROM temp_pub_municipios
    WHERE cve_muni is not null
);  
 
-- clean column names
SELECT make_cols_not_string('pub_municipios', 'clean');


--#################################
-- Clean PUB Estados
--################################
DROP TABLE if exists clean.pub_estados;

CREATE TABLE clean.pub_estados AS (
SELECT cve_ent, 
       sum("'0017'") AS "'0017'",  
       sum("'0018'") AS "'0018'",  
       sum("'0019'") AS "'0019'",  
       sum("'0034'") AS "'0034'",  
       sum("'0048'") AS "'0048'",  
       sum("'0059'") AS "'0059'",  
       sum("'0066'") AS "'0066'",  
       sum("'0079'") AS "'0079'",  
       sum("'0085'") AS "'0085'",  
       sum("'0092'") AS "'0092'",  
       sum("'0101'") AS "'0101'",  
       sum("'0102'") AS "'0102'",  
       sum("'0103'") AS "'0103'",  
       sum("'0109'") AS "'0109'",  
       sum("'0110'") AS "'0110'",  
       sum("'0118'") AS "'0118'",  
       sum("'0130'") AS "'0130'",  
       sum("'0131'") AS "'0131'",  
       sum("'0132'") AS "'0132'",  
       sum("'0133'") AS "'0133'",  
       sum("'0136'") AS "'0136'",  
       sum("'0138'") AS "'0138'",  
       sum("'0139'") AS "'0139'",  
       sum("'0140'") AS "'0140'",  
       sum("'0148'") AS "'0148'",  
       sum("'0170'") AS "'0170'",  
       sum("'0171'") AS "'0171'",  
       sum("'0172'") AS "'0172'",  
       sum("'0196'") AS "'0196'",  
       sum("'0197'") AS "'0197'",  
       sum("'0200'") AS "'0200'",  
       sum("'0202'") AS "'0202'",  
       sum("'0204'") AS "'0204'",  
       sum("'0219'") AS "'0219'",  
       sum("'0220'") AS "'0220'",  
       sum("'0221'") AS "'0221'",  
       sum("'0228'") AS "'0228'",  
       sum("'0263'") AS "'0263'",  
       sum("'0278'") AS "'0278'",  
       sum("'0286'") AS "'0286'",  
       sum("'0342'") AS "'0342'",  
       sum("'0372'") AS "'0372'",  
       sum("'0373'") AS "'0373'",  
       sum("'0374'") AS "'0374'",  
       sum("'0375'") AS "'0375'",  
       sum("'0376'") AS "'0376'",  
       sum("'0377'") AS "'0377'",  
       sum("'0424'") AS "'0424'",  
       sum("'0430'") AS "'0430'",  
       sum("'0431'") AS "'0431'",  
       sum("'0453'") AS "'0453'",  
       sum( "E016") AS  "E016",  
       sum( "S016") AS  "S016",  
       sum( "S017") AS  "S017",  
       sum( "S021") AS  "S021",  
       sum( "S038") AS  "S038",  
       sum( "S048") AS  "S048",  
       sum( "S052") AS  "S052",  
       sum( "S054") AS  "S054",  
       sum( "S057") AS  "S057",  
       sum( "S058") AS  "S058",  
       sum( "S061") AS  "S061",  
       sum( "S065") AS  "S065",  
       sum( "S071") AS  "S071",  
       sum( "S072") AS  "S072",  
       sum( "S117") AS  "S117",  
       sum( "S118") AS  "S118",  
       sum( "S174") AS  "S174",  
       sum( "S176") AS  "S176",  
       sum( "S203") AS  "S203",  
       sum( "S216") AS  "S216",  
       sum( "S241") AS  "S241",  
       sum( "S274") AS  "S274",  
       sum( "U005") AS  "U005"
 FROM clean.pub_municipios
 GROUP BY cve_ent
);
