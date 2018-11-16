#!/usr/bin/env Rscript

####### Dependency hotfix
install_oldpak <- function(package_name, older_version){
    if (!(package_name %in% installed.packages()[, 'Package'] & older_version %in% installed.packages()[,'Version'])) {
        version_url=paste0('https://cran.r-project.org/src/contrib/Archive/',
                             package_name, '/', package_name, '_', older_version, '.tar.gz')
        install.packages(version_url, repos=NULL, type='source')
    }
require(package_name, character.only=TRUE)
}

older_packages <- list('DBI'='0.8', 'RPostgres'='1.0-3')

sapply(1:length(older_packages),
       function(x) install_oldpak(package_name=names(older_packages)[x],
                                  older_version=unname(older_packages[[x]])))

# Install regular CRAN packages
ipak <- function(pkg){
  new.pkg <- pkg[!(pkg %in% installed.packages()[, 'Package'])]
  if (length(new.pkg))
    install.packages(new.pkg, dependencies=TRUE, repos='http://cran.us.r-project.org')
  sapply(pkg, require, character.only=TRUE)
}


packages <- c('optparse','tidyverse', 'dbplyr', 'stringr', 'data.table',
              'lubridate', 'readxl', 'aws.s3', 'gsubfn', 'geosphere',
              'lwgeom', 'mapsapi', 'sf', 'sp')

ipak(packages)

# Install Github packages
dev_pak <- function(github_path){
  package <- gsub('.*/(.*)', '\\1', github_path)
  new_package <- package[!(package %in% installed.packages()[, 'Package'])]
  if (length(new_package))
    devtools::install_github(github_path)
  sapply(package, require, character.only=TRUE)
}

github_packages <- c('gaborcsardi/dotenv')
dev_pak(github_packages)


