# Platforma Preventiva -

## About:
In order to improve the targeting of social programs, the System of Integral Social Information (Sistema de Información Social Integral - SISI) strives to create a platform to help analyse multi-dimensional data not usually taken into account when developing social policy in Mexico. The proposed approach is to create various compound indicators tailored to tackle different areas of interest in social policy; as such, all indicators would create a profile of geographical areas and help target social programs in a more thorough manner.


## Installation

The Ingest pipeline can be run after cloning this repository and
calling `make init`, `make setup` and `make run`.

### Dependencies

* Python 3.5.2
* luigi
* git
* psql (PostgreSQL) 9.5.4
* PostGIS 2.1.4
* ...and other Python packages (see `requirements.txt`)

## Data Pipeline

After you create the environment set up the pipeline_tasks in luigi.cfg
The general process of the pipeline is:

* **StartPipeline:**
* RunPipelines [politica_preventiva/pipelines/politica_preventiva.py]
* **Ingest:** [politica_preventiva/pipelines/ingest/ingest_orchestra.py]
* LocalIngest: Ingest data from multiple sources
* LocalToS3: Upload to S3 and save historical by date
* UpdateDB: Update Postgres tables and Create indexes (see commons/pg_raw_schemas)
* **ETL:** [politica_preventiva/pipelines/etl/etl_orchestra.py]

### Contributors

| [![javurena7][ph-javurena7]][gh-javurena7] | [![rsanchezavalos][ph-rsanchez]][gh-rsanchez] | [![andreanr][ph-andreanr]][gh-andreanr]| [![andreuboada ][ph-andreuboada]][gh-andreuboada] |
|                 :--:                       |                     :--:                      |                     :--:               |                     :--:                          |
|        [javurena7][gh-javurena7]           |         [rsanchezavalos][gh-rsanchez]         |          [andreanr][gh-andreanr]       |          [andreuboada][gh-andreuboada]            | 


| [![monzalo14][ph-monzalo14]][gh-monzalo14] | [![abrownrb][ph-abrownrb]][gh-abrownrb] | [![ollin18][ph-ollin18]][gh-ollin18] |
|                 :--:                       |                 :--:                 |                 :--:                 |
|       [monzalo14][gh-monzalo14]            |        [abrownrb][gh-abrownrb]         |        [ollin18][gh-ollin18]         |

[ph-javurena7]: https://avatars2.githubusercontent.com/u/14095871?v=3&s=180
[gh-javurena7]: https://github.com/javurena7

[ph-andreanr]: https://avatars2.githubusercontent.com/u/5949086?v=3&s=180
[gh-andreanr]: https://github.com/andreanr

[ph-rsanchez]: https://avatars2.githubusercontent.com/u/10931011?v=3&s=180
[gh-rsanchez]: https://github.com/rsanchezavalos

[ph-andreuboada]: https://avatars2.githubusercontent.com/u/7883897?v=3&s=180
[gh-andreuboada]: https://github.com/andreuboada

[ph-monzalo14]: https://avatars1.githubusercontent.com/u/16139907?v=3&s=180
[gh-monzalo14]: https://github.com/monzalo14

[ph-abrownrb]: https://avatars0.githubusercontent.com/u/29579851?s=180&v=3
[gh-abrownrb]: https://github.com/abrownrb

[ph-ollin18]: https://avatars0.githubusercontent.com/u/5835469?v=3&s=180
[gh-ollin18]: https://github.com/ollin18
