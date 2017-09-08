#!/bin/bash
# Bash Script de descarga DENUE.

WORK_DIR=$1
OUTPUT=$2

echo 'Downloading DENUE'

# TODO() Create script to scrape new files (if updated).
files='denue_00_93 denue_00_93_0116 denue_00_93_1016 denue_93_25022015  denue_00_11 denue_00_11_0116 denue_00_11_1016 denue_11_25022015  denue_00_43 denue_00_43_0116 denue_00_43_1016 denue_43_25022015  denue_00_46111  denue_00_46111_0116 denue_00_46111_1016  denue_00_46112-46311  denue_00_46112-46311_0116 denue_00_46112-46311_1016  denue_00_46321-46531  denue_00_46321-46531_0116 denue_00_46321-46531_1016  denue_00_46591-46911  denue_00_46591-46911_0116 denue_00_46591-46911_1016  denue_46111_25022015  denue_46112-46311_25022015  denue_46321-46531_25022015  denue_46591-46911_25022015  denue_00_23 denue_00_23_0116 denue_00_23_1016 denue_23_25022015  denue_00_55 denue_00_55_0116 denue_00_55_1016 denue_55_25022015  denue_00_22 denue_00_22_0116 denue_00_22_1016 denue_22_25022015  denue_00_31-33  denue_00_31-33_0116 denue_00_31-33_1016  denue_31-33_25022015  denue_00_51 denue_00_51_0116 denue_00_51_1016 denue_51_25022015  denue_00_21 denue_00_21_0116 denue_00_21_1016 denue_21_25022015  denue_00_81 denue_00_81_0116 denue_00_81_1016 denue_81_25022015  denue_00_72 denue_00_72_0116 denue_00_72_1016 denue_72_25022015  denue_00_56 denue_00_56_0116 denue_00_56_1016 denue_56_25022015  denue_00_71 denue_00_71_0116 denue_00_71_1016 denue_71_25022015  denue_00_62 denue_00_62_0116 denue_00_62_1016 denue_62_25022015  denue_00_61 denue_00_61_0116 denue_00_61_1016 denue_61_25022015  denue_00_52 denue_00_52_0116 denue_00_52_1016 denue_52_25022015  denue_00_53 denue_00_53_0116 denue_00_53_1016 denue_53_25022015  denue_00_54 denue_00_54_0116 denue_00_54_1016 denue_54_25022015  denue_00_48-49  denue_00_48-49_0116 denue_00_48-49_1016  denue_48-49_25022015'


# Download each file and save in temporal folder
for file in $files;
do
    echo '############## DOWNLOADING '${file}' ################'

    wget -P $WORK_DIR "http://www.beta.inegi.org.mx/contenidos/masiva/denue/"${file}"_csv.zip"
    unzip -o $WORK_DIR"/"${file}"_csv" -d $WORK_DIR"/"${file}

    rm $WORK_DIR"/"${file}"_csv.zip"
    find $WORK_DIR -regex '.*denue_diccionario.*' -delete

    full_path=$( find  $WORK_DIR/${file}"/" -name "*.csv" -print ) 
    filename=$(basename ${file%.*}).csv
    cat $full_path | awk -F'"' -v OFS='' '{ for (i=2; i<=NF; i+=2) gsub(",", "", $i) } 1' |\
	    sed  's/\([a-zA-Z0-9]\"\)\(\"[a-zA-Z0-9]\)/\1\n\2/;s/"//g' |\
	    csvformat -d "," -D "|" > $WORK_DIR/$filename 
    rm -r $WORK_DIR"/"${file}

done

# Merge files
array=$( find  $WORK_DIR/ -regex '.*csv' ) 
head=$( head -n1 $(ls | head -1 ))

for fname in $array
do
	tail -n+3 $fname | awk -F'|' 'NF==41' >> $2
done

