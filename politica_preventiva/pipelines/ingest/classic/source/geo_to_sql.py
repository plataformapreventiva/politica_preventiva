####### BORRAR ESTA FUNCIÓN O PASARLA A ingest_utils.py
####### BORRAR ESTA FUNCIÓN O PASARLA A ingest_utils.py
####### BORRAR ESTA FUNCIÓN O PASARLA A ingest_utils.py
####### BORRAR ESTA FUNCIÓN O PASARLA A ingest_utils.py
####### BORRAR ESTA FUNCIÓN O PASARLA A ingest_utils.py


import os
import sys

###############################################################################
# Convert a directory containing shapefiles to a SQL database
#
# Description:
#
# This script uses the shp2pgsql command in Postgis to convert a
# directory containing shapefiles into a single SQL database, which
# can be uploaded to Postgres using, for example,
#
# psql -h cluster -U username -W -f manzanas.sql
#
# Parameters:
#
# The script requires the following commandline arguments:
# (1) path to directory with  all the shapefiles
# (2) schema name
# (3) path to output sql file
#
# This is an example call
# python3 ~/Desktop/geo_to_sql.py ~/Desktop/Manzanas/ raw.manzanas ~/Desktop/Manzanas/manzanas.sql
#
# Returns:
#
# The merged SQL database is located in the path specified by the
# third commandline argument. The intermediate SQL files are located
# in the original shapefile directory.
###############################################################################

# get a list of paths to shapefiles, without their extensions
all_files = os.listdir(sys.argv[1])
all_files = set([sys.argv[1]+ "/" + os.path.splitext(f)[0] for f in all_files])
all_files = list(all_files)
all_files.sort()

# loop over shapefiles, using -a to describe files that will be appended
for (ix, cur_file) in enumerate(all_files):
    append_str = ""
    if ix != 0:
        append_str = "-a"
    shp2pgsql_cmd = "shp2pgsql %s %s %s > %s" % \
                    (append_str, cur_file, sys.argv[2], cur_file + ".sql")
    print(shp2pgsql_cmd)
    os.system(shp2pgsql_cmd)

# combine the intermediate SQL databases
os.system("cat %s/*.sql > %s" % (sys.argv[1], sys.argv[3]))
