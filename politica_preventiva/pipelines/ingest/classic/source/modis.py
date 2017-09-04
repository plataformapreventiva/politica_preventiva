from pymodis import downmodis, convertmodis
import itertools
import os
import glob
import timeit
import re

# Main path to data
dest = "/home/javier/MODIS_DATA"

# Modis Transformation Tool Path
mrt_path = '/home/javier/MRT'

# If no path, create
if(not os.path.exists(dest)):
    os.makedirs(dest)
# Get tiles for Mexico
h = {'h07', 'h08', 'h09'}
v = {'v05', 'v06', 'v07'}
ti = [h1+v1 for h1 in h for v1 in v]
ti.remove('h09v05')
# Write tiles as a sstring
tiles = ','.join(ti)

# MYD13A2: 1000m
# MYD13Q1: 250m
product_m = "MOD13A2.005"
today = "2015-07-20"
#Inicio: 4 julio 2002
enday = "2015-07-30"
# number of days to download

user = 'javurena7'
password = 'Mesalina1305'

# Connect to MODIS API
modis = downmodis.downModis(destinationFolder=dest, product=product_m, tiles=tiles, today=today, enddate=enday, user=user, password=password)
modis.connect()

#Download all available days for specified interval
modis.downloadsAllDay()

#################### MOSAIC #########################

# Convert tiles into full maps (mosaics). Takes all available files in 
# 'dest', finds which tiles belong to the same mosaic, and converts them
# NOTE: it only saves NDVI and pixel-reliability. For other products, change
# sting subset 

# Regex to find the year and day in filename
re_downloaded = re.compile('A([0-9]{7})')
re_mosaiked = re.compile('AL([0-9]{7})_files')

#List all downloaded files 
day_files = glob.glob(os.path.join(dest , '*.hdf'))
downloaded_files = {re_downloaded.search(x).group(1) for x in day_files if re_downloaded.search(x)}

#List mosaiked files
mosaiked_files = glob.glob(os.path.join(dest, '*files.txt'))
mosaiked_files = {re_mosaiked.search(x).group(1) for x in mosaiked_files if re_mosaiked.search(x)}

# List missing files to mosaic 
missing_files = downloaded_files.difference(mosaiked_files)


for file in missing_files:
    re_yd = re.compile('A' + file)

    # Create list of all tiles in a map
    mosaic_list = [x for x in day_files if re_yd.search(x)]

    # Write a file containing all tiles in a map
    listfile = dest + "/AL" + file + '_files.txt'
    thefile = open(listfile, 'w')
    for item in mosaic_list:
        thefile.write("%s\n" % item)
    thefile.close()

    # Use create Mosaic to store
    outprefix = dest + '/Mosaics/AM' + file + '_mosaic' 

    # Select bands. NDVI and pixel reliability
    subset = '1 0 0 0 0 0 0 0 0 0 0 1'

    # Run mosaic
    mosaic = convertmodis.createMosaic(listfile=listfile, outprefix=outprefix, mrtpath=mrt_path, subset=subset)
    mosaic.run()


###### FIND MISSING DAYS
#day_files = glob.glob(os.path.join(dest + '/Mosaics/' , '*.hdf'))
day_files = glob.glob(os.path.join(dest + '/Parameter_Files/' , '*.prm'))
re_year = re.compile('([0-9]{4})([0-9]{3})')
#years_total = {re_year.search(day_file).group(1) for day_file in day_files if re_year.search(day_file)}
#days_n = {re_year.search(day_file).group(2) for day_file in day_files}
#dates = {re_year.search(date).group(0) for date in day_files if re_year.search(date)}
#dates = {year + day_n for year, day_n in itertools.product(years_total, days_n)}
dates = glob.glob(os.path.join(dest + '/Mosaics/' , '*.hdf'))
dates = {re_year.search(date).group(0) for date in dates if re_year.search(date)}
available = {date for date, x in itertools.product(dates, day_files) if re.search('AM' + date, x)}
missing = dates.difference(available)
### CREATE PARAMETER FILES AND CONVERT
# read 
dates = glob.glob(os.path.join(dest + '/Mosaics/' , '*.hdf'))
day_files = {date for date in dates if re_year.search(date).group(0) in missing}
with open(dest + '/Parameter_Files/template2.prm') as f:
    file_template = f.readlines()
f.close()
re_file = re.compile('(AM[0-9]{4}([0-9]{3})_mosaic)')
j = 97
for file in day_files:
    print('MAPA ' + str(j))
    file_name = re_file.search(file).group(0)
    year = re_file.search(file).group(1)
    day = re_file.search(file).group(2)
    day_path = dest + '/Geotiffs/DOY_' + day 
    if(not os.path.exists(day_path)):
        os.makedirs(day_path)
    par_file_path = dest + '/Parameter_Files/P' + file_name + '.prm'
    f = open(par_file_path, 'w')
    re_sub1 = re.compile('entrada_reemp')
    re_sub2 = re.compile('salida_reemp')
    for item in file_template:
        # Reemplazar el input file por el file actual
        if re_sub1.search(item):
        # Reemplazar el output file por una carpeta con la fecha 
            f.write(re_sub1.sub(file, item))
        elif re_sub2.search(item):
            f.write(re_sub2.sub(day_path + '/' + day + '_' + year + '.tif', item))
        else:
            f.write(item)
    f.close()
    gtif = convertmodis.convertModis(file, par_file_path ,mrt_path)
    gtif.run()
    j += 1
    

    ### Ahorita se llaman 209_AM2015209_mosaic.1_km_16_days_NDVI.tif, cambiar a DOY_YYYY.tif


    ################## RENOMBRAR ARCHIVOS; SOLO UNA VEZ 
    path = dest + "/Geotiffs/"
    day_dirs = os.listdir(path)
    re_day = re.compile('DOY_([0-9]{3})')
    re_ndvi = re.compile('NDVI')
    re_year = re.compile('AM([0-9]{4})')
    for day_dir in day_dirs:
        path_day = path + day_dir + "/"
        day_n = re_day.search(day_dir).group(1)
        dir_files = glob.glob(os.path.join(path_day, '*.tif'))
        dir_files = [file for file in dir_files if re_ndvi.search(file)]
        for file in dir_files:
            file_year = re_year.search(file).group(1)
            new_name = path_day + day_n + "_" + file_year + '.tif'
            os.rename(file, new_name)

# * Generated CRS (+proj=lcc +lat_1=17.5 +lat_2=29.5 +lat_0=12 +lon_0=-102 +x_0=2500000 +y_0=0 +ellps=WGS84 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs)
#  * Generated CRS (+proj=lcc +lat_1=17.5 +lat_2=29.5 +lat_0=12 +lon_0=-102 +x_0=2500000 +y_0=0 +datum=WGS84 +units=m +no_defs)
#   * Generated CRS (+proj=lcc +lat_1=17.5 +lat_2=29.5 +lat_0=12 +lon_0=-102 +x_0=2500000 +y_0=0 +ellps=WGS84 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs)