# Platforma Preventiva - 

## About:
In order to improve the targeting of social programs, the System of Integral Social Information (Sistema de Información Social Integral - SISI) strives to create a platform to help analyse multi-dimensional data not usually taken into account when developing social policy in Mexico. The proposed approach is to create various compound indicators tailored to tackle different areas of interest in social policy; as such, all indicators would create a profile of geographical areas and help target social programs in a more thorough manner.


## Installation

The analysis pipeline can be run after cloning this repository and
calling `make init` and `make deploy`. 

### Dependencies

* Python 3.5.2
* luigi
* git
* psql (PostgreSQL) 9.5.4
* PostGIS 2.1.4
* ...and other Python packages (see `requirements.txt`)

## Data Pipeline

After you create the evironment set up the pipeline_tasks in luigi.cfg 
The general process of the pipeline is:

* **Ingest:**
* LocalIngest: Ingest data from multiple sources
* LocalToS3: Upload to S3 and save historical by date
* UpdateOutput: Preprocess to Output
* UpdateDB: Update Postgres tables and Create indexes (see commons/pg_raw_schemas)
* **ETL:**
* MergeDBs: ETL processes to clean
* CleanDB: SQL processes to clean tables creation
* SetNeo4J: Download clean tables and upload to neo4j
* **Model:**
*
*

### Contributors

| [![javurena7][ph-javurena7]][gh-javurena7] | [![rsanchezavalos][ph-rsanchez]][gh-rsanchez] | [![andreanr][ph-andreanr]][gh-andreanr]| [![sedesol ][ph-sedesol]][gh-sedesol] |
|                 :--:                 |                     :--:                      |                     :--:             |                     :--:             |              
|        [javurena7][gh-javurena7]         |         [rsanchezavalos][gh-rsanchez]           |          [andreanr][gh-andreanr] |          [sedesol][gh-sedesol]      |      


[ph-javurena7]: https://avatars0.githubusercontent.com/u/14095871?v=3&s=550
[gh-javurena7]: https://github.com/javurena7

[ph-andreanr]: https://avatars1.githubusercontent.com/u/5949086?v=3&s=460
[gh-andreanr]: https://github.com/andreanr

[ph-rsanchez]: https://avatars.githubusercontent.com/u/10931011?v=3&s=460
[gh-rsanchez]: https://github.com/rsanchezavalos

[ph-sedesol]: https://avatars0.githubusercontent.com/u/20521447
[gh-sedesol]: https://github.com/20521447
