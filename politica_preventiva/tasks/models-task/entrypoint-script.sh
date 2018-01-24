#!/bin/bash
set -e
token=$1
path=$2
cmd=$3

echo $token
echo $path
echo $cmd

git clone https://${token}@github.com/plataformapreventiva/${path}.git

eval $cmd