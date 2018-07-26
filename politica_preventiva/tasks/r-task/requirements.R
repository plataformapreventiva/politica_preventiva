#!/usr/bin/env Rscript
# Install

ipak <- function(pkg){
  new.pkg <- pkg[!(pkg %in% installed.packages()[, "Package"])]
  if (length(new.pkg))
    install.packages(new.pkg, dependencies = TRUE, repos='http://cran.us.r-project.org')
  sapply(pkg, require, character.only = TRUE)
}


packages <- c('optparse','tidyverse', 'dbplyr', 'stringr', 'lubridate',
	      'readxl', 'RPostgres', 'aws.s3', 'gsubfn')

ipak(packages)

dev_pak <- function(github_path){
  package <- gsub('.*/(.*)', '\\1', github_path)
  new_package <- package[!(package %in% installed.packages()[, "Package"])]
  if (length(new_package))
    devtools::install_github(github_path)
  sapply(package, require, character.only = TRUE)
}

github_packages <- c('gaborcsardi/dotenv')
dev_pak(github_packages)
