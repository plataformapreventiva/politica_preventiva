FROM dpa/r:0.1

ENV PROJECT_NAME politica_preventiva


## Copiamos los scripts
ADD . /${PROJECT_NAME}/
WORKDIR /${PROJECT_NAME}

RUN apt-get -y clean &&  apt-get -y update &&  apt-get -y upgrade


RUN \
    apt-get -y -qq update; apt-get upgrade -y -qq; \
    apt-get install --no-install-recommends -y  curl 

RUN apt-get update \
    && apt-get install -y libblas-dev liblapack-dev gfortran \
                       libpq-dev postgresql postgresql-contrib

RUN Rscript -e 'install.packages("devtools")'\
    && Rscript -e 'devtools::install_github("RcppCore/Rcpp")'\
    && Rscript -e 'devtools::install_github("rstats-db/DBI")'\
    && Rscript -e 'devtools::install_github("rstats-db/RPostgres")'

RUN ["Rscript", "requirements.R"]
