#!/bin/bash

##############################
# PUB
# Upload files to AWS S3
##############################


origdir=$HOME"/PUB"
cd "$origdir"

# Loop over al rar files
for file in *.rar; do
	print $file;
	name=$(unrar lb "$file");
	echo "$name";
	unrar e -r -p'Informacion2013Al2017' ${file};
	arriba=$(echo ${name} | grep -oP '_\d{4}\.');
	echo "Comprimiendo archivo "{$name};
	#--keep
	pigz --force --verbose --fast $name;
	aws s3 cp $name".gz" "s3://pub-raw/new_raw/pub"$arriba"txt.gz";
	rm $name".gz";
done;


cd "$HOME"

