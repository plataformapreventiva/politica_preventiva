#!/bin/bash

# Check parameters
#echo 'token' $token
#echo 'path' $path
#echo 'cmd' $cmd

# Clone repository with your github token
git clone https://${token}@github.com/plataformapreventiva/${path}.git

# Change your workspace
cd ${path}

# Install extra requirements
#RUN ["Rscript", "requirements.R"]

# Run model
eval $cmd
