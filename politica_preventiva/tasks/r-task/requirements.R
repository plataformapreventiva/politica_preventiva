#!/usr/bin/env Rscript

# Install regular CRAN packages
install_stable <- function(package_list){
  to_install <- package_list[!(package_list %in% installed.packages()[, 'Package'])]
  if (length(to_install))
    install.packages(to_install, dependencies=TRUE,
                     repos='http://cran.us.r-project.org')
  sapply(package_list, require, character.only=TRUE)
}

packages <- c('Rcpp', 'optparse','tidyverse', 'dbplyr',
              'stringr', 'data.table', 'lubridate',
              'readxl', 'aws.s3', 'gsubfn', 'geosphere', 'sp')
install_stable(packages)

# Dependency hotfix
install_version <- function(package_name, older_version){

    package_info <- as.data.frame(installed.packages()[, c('Package',
                                                           'Version', 'LibPath')],
                                  row.names = FALSE)
    installed_versions <- package_info[package_info$Package == package_name ,]
    lib_path <- installed_versions$LibPath[installed_versions$Version == older_version]
    if (!older_version %in% installed_versions$Version) {
        version_url <- paste0('https://cran.r-project.org/src/contrib/Archive/',
                              package_name, '/', package_name,
                              '_', older_version, '.tar.gz')
        install.packages(version_url, repos=NULL, type='source')
        lib_path <- NULL
    }
  require(package_name, character.only=TRUE, lib.loc=lib_path)
}

older_packages <- list('DBI'='0.8', 'RPostgres'='1.0-3')
sapply(1:length(older_packages),
       function(x) install_version(package_name=names(older_packages)[x],
                                   older_version=unname(older_packages[[x]])))


# Install Github packages
install_develop <- function(github_path){
  package <- gsub('.*/(.*)', '\\1', github_path)
  new_package <- package[!(package %in% installed.packages()[, 'Package'])]
  if (length(new_package))
    devtools::install_github(github_path)
  sapply(package, require, character.only=TRUE)
}

github_packages <- c('gaborcsardi/dotenv')
install_develop(github_packages)
