#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import requests
import numpy as np
import pandas as pd
import json
from requests.auth import HTTPDigestAuth
import datetime
from itertools import product
from bs4 import BeautifulSoup
from ftplib import FTP
import requests

"""
issues:
check if shp files exist in the directory  - Roberto

"""

# Setup utility functions and password / configuration info
conf = {}
with open("../conf/conf_profile.json", "r") as f:
    conf_profile = json.load(f)
    conf["SMN_USER"] = conf_profile["SMN_USER"]
    conf["SMN_PASSWORD"] = conf_profile["SMN_PASSWORD"]


def get_cenapred_data(servicio="ANR", subservicio="MuniAPPInfo", geometria="si"):
    """
    Returns a DataFrame with municipality level information from CENAPRED
    services

    Args:
        servicio (str): Define service to query:
            List[]
        subservicio (str): Define Subservices to query from:
            List[]
    Returns:
        dataframe: Pandas Dataframe with municipality level information.

    """

    url = "http://servicios2.cenapred.unam.mx:6080/arcgis/rest/services/{0}/{1}/MapServer/0/query?f=json".\
    format(servicio,subservicio)

    state = ("&where=NOM_ENT_%20IN%20(%27Aguascalientes%27%2C%27Baja%20California%27%2C%27Baja%20California%"
        "20Sur%27%2C%27Campeche%27%2C%27Chiapas%27%2C%27Chihuahua%27%2C%27Coahuila%20de%20Zaragoza%27%2C%"
        "27Colima%27%2C%27Distrito%20Federal%27%2C%27Durango%27%2C%27M%C3%A9xico%27%2C%27Guanajuato%27%2C%"
        "27Guerrero%27%2C%27Hidalgo%27%2C%27Jalisco%27%2C%27Michoac%C3%A1n%20de%20Ocampo%27%2C%27Morelos%27%"
        "2C%27Nayarit%27%2C%27Nuevo%20Le%C3%B3n%27%2C%27Oaxaca%27%2C%27Puebla%27%2C%27Quer%C3%A9taro%27%2C%"
        "27Quintana%20Roo%27%2C%27San%20Luis%20Potos%C3%AD%27%2C%27Sinaloa%27%2C%27Sonora%27%2C%27Tabasco%27%"
        "2C%27Tamaulipas%27%2C%27Tlaxcala%27%2C%27Veracruz%20de%20Ignacio%20de%20la%20Llave%27%2C%27Yucat%C3%"
        "A1n%27%2C%27Zacatecas%27)")

    params = ("&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=102100&spatialRel"
        "=esriSpatialRelIntersects&relationParam=")

    dict_subservicio = {
        #ANR/MuniAPP - Servicio usado en
        "MuniAPPInfo":("&outFields=POBTOT_%2CNOM_MUN%2CNOM_ENT_%2CTVIVHAB_%2CSEP%2CHOSPITALES"
        "%2CBancos%2CGasolinera%2Choteles%2CSupermerca%2CAeropuerto%2Cnum_col%2CShape_Area%"
        "2CGVS_ISE10%2CG_RezagoSo%2CG_Marginac%2CG_Resilien%2CGP_ondasca%2CGP_Inundac%2CGP_Ciclnes%"
        "2CGP_BajasTe%2CGP_Nevadas%2CGP_Granizo%2CGP_TormEle%2CGP_Sequia2%2CGP_Sismico%2CGP_SusTox%"
        "2CGP_Tsunami%2CGP_SusInfl%2CG_SusceLad%2Curl%2CA_REGLAM%2CTITULO_REG%2Catlas_mun%2CCC_HIDRO%"
        "2CD_GEO%2CD_HIDRO%2CD_QUI%2CE_GEO%2CE_HIDRO%2CE_QUI%2CE_SANI%2CDECLARATOR%2CPop2030%2CV_CC%"
        "2CNum_Us_CFE%2CPOBFEM_%2CPOBMAS_%2COBJECTID_12")
        #
        }

    dict_geometry= {
        "si":("&returnGeometry=false&maxAllowableOffset=3000&geometryPrecision=%7B%22xmin%22%"
        "3A-13775786.985668927%2C%22ymin%22%3A2198940.452952425%2C%22xmax%22%3A-12523442.71424493%"
        "2C%22ymax%22%3A3451284.724376421%2C%22spatialReference%22%3A%7B%22wkid%22%3A102100%2C%"
        "22latestWkid%22%3A3857%7D%7D&outSR=102100")
        }

    out="&outSR=102100"

    return_id="&returnIdsOnly=true"

    get_ids = url + state + params + dict_subservicio[subservicio] + dict_geometry[geometria]  + out + return_id

    myResponse_ids = requests.get(get_ids)
    myResponse_ids.raise_for_status()

    #Get ID's
    if(myResponse_ids.ok):
        # Loads (Load String) takes a Json file and converts into python data structure (dict or list, depending on JSON)

        jIds = json.loads(myResponse_ids.content.decode('utf-8'))
        jIds = jIds["objectIds"]
        print('query of Ids successful - {0} ids found'.format(len(jIds)))

        #arcgis only gives 1000 ids at time
        #split the ids into groups of 1000
        chunks = int(np.ceil(len(jIds)/1000.0))
        print("splitted into {} chunks".format(chunks))
        chunks_of_ids = np.array_split(jIds,chunks)

        features = []
        i=1
        for chunk in chunks_of_ids:
            #get id params
            objectids = ('&objectIds=+' + '+%2C+'.join(str(i) for i in chunk))
            get_dict = url + state + dict_geometry[geometria] + dict_subservicio[subservicio] + out + objectids

            myResponse = requests.get(get_dict)


            if(myResponse.ok):
                # Loads (Load String) takes a Json file and converts into python data structure (dict or list, depending on JSON)
                jData = json.loads(myResponse.content.decode('utf-8'))
                features.extend(jData['features'])
                print('Chunk {0} of {1}:  query successful'.format(i,chunks))
                i+=1

            else:
                # If response code is not ok (200), print the resulting http error code with description
                print(myResponse.raise_for_status())

        #convert list of dicts into dataframe
        db = pd.DataFrame([x["attributes"] for x in  features])

    else:
        # If response code is not ok (200), print the resulting http error code with description
        print(myResponse_ids.raise_for_status())
        print("query of Id's unsuccessful")


    return db


def get_inpc_ciudad_data(year = "2016"):
    """
    Returns a Pandas with INPC [nourishment] information from INEGI services

    Args:
        (str): [Pendiente descripción].
        (str): [Pendiente].

    Returns:
        Returns two objects
        - Pandas dataframe data
        - metadata: .

    e.g.
        - data, metadata = get_inpc_ciudad_data()

    """

    data = pd.DataFrame()
    metadata = {}

    #dict of ciudades with state code
    dict_ciudades = {
        "7. Area Metropolitana de la Cd. de México":"09",
        "Acapulco,%20Gro.":"12",
        "Aguascalientes, Ags.":"01",
        "Campeche, Camp.":"04",
        "Cd. Acuña, Coah.":"05",
        "Cd. Jiménez, Chih.":"08",
        "Cd. Juárez, Chih.":"08",
        "Colima, Col.":"06",
        "Córdoba, Ver.":"30",
        "Cortazar, Gto.":"11",
        "Cuernavaca, Mor.":"17",
        "Culiacán, Sin.":"25",
        "Chetumal, Q.R.":"23",
        "Chihuahua, Chih.":"08",
        "Durango, Dgo.":"10",
        "Fresnillo, Zac.":"32",
        "Guadalajara, Jal.":"14",
        "Hermosillo, Son.":"26",
        "Huatabampo, Son.":"26",
        "Iguala, Gro.":"12",
        "Jacona, Mich.":"16",
        "La Paz, B.C.S.":"03",
        "León, Gto.":"11",
        "Matamoros, Tamps.":"28",
        "Mérida, Yuc.":"31",
        "Mexicali, B.C.":"02",
        "Monclova, Coah.":"05",
        "Monterrey, N.L.":"19",
        "Morelia, Mich.":"16",
        "Oaxaca, Oax.":"20",
        "Puebla, Pue.":"21",
        "Querétaro, Qro.":"22",
        "San Andrés Tuxtla, Ver.":"30",
        "San Luis Potosí, S.L.P.":"24",
        "Tampico, Tamps.":"28",
        "Tapachula, Chis.":"07",
        "Tehuantepec, Oax.":"20",
        "Tepatitlán, Jal.":"14",
        "Tepic, Nay.":"18",  #error downloading data from tepic
        "Tijuana, B.C.":"02",
        "Tlaxcala, Tlax.":"29",
        "Toluca, Edo. de Méx.":"15",
        "Torreón, Coah.":"05",
        "Tulancingo, Hgo.":"13",
        "Veracruz, Ver.":"30",
        "Villahermosa, Tab.":"27"
        }

    for ciudad in dict_ciudades:
        ciudad_encoded = ciudad.replace(" ","+").encode("utf-8")
        ciudad_id = dict_ciudades[ciudad]
        base = ("http://www.inegi.org.mx/sistemas/indiceprecios/Exportacion.aspx?INPtipoExporta=CSV"
        "&_formato=CSV")

        year_query = "&_anioI=1969&_anioF={0}".format(year)

        #tipo niveles
        tipo = ("&_meta=1&_tipo=Niveles&_info=%C3%8Dndices&_orient=vertical&esquema=0&"
            "t=%C3%8Dndices+de+Precios+al+Consumidor&")

        lugar = "st={0}".format(ciudad_encoded)

        serie = ("&pf=inp&cuadro=0&SeriesConsulta=e%7C240123%2C240124%2C240125%"
        "2C240126%2C240146%2C240160%2C240168%2C240186%2C240189%2C240234"
        "%2C240243%2C240260%2C240273%2C240326%2C240351%2C240407%2C240458"
        "%2C240492%2C240533%2C260211%2C260216%2C260260%2C320804%2C320811%2C320859%2C")

        url = base + year_query + tipo + lugar + serie


        try:
            #download metadata
            metadata[ciudad] = pd.read_csv(url,error_bad_lines=False,nrows=5,usecols=[0],header=None).values
            #download new dataframe
            print('trying to download data from {}'.format(ciudad))

            temp = pd.read_csv(url,error_bad_lines=False,skiprows=14,usecols=[1,2,3],header=None,\
                names=["INPC-general_{}".format(ciudad_id),"INPC-alimentos-bebidas-tabaco_{}".\
                format(ciudad_id),"INPC-alimentos_{}".format(ciudad_id)])

            #Just keep one fecha column
            try:
                data["fecha"] = temp["fecha"]
                del temp["fecha"]
            except:
                pass

            data = pd.concat([data, temp], axis=1)
            print("Query succesful for city {}".format(ciudad))

        except:
            print ("Error downloading data for : {}".format(ciudad))


    return data, metadata


def get_avance_agricola(cultivo = "MAIZ GRANO"):
    """
    Returns a Pandas with Avance Nacional de Siembra for crop 'cultivo'
    from SAGARPA-SIEP. The information is divided by municipality, and
    contains info for hydrolocial mode and cicle of agricultural year.

    Args:
        (cultivo): Crop to monitor. List available from dict_cultivo-

    Returns:
        Pandas dataframe with columns:
            (estado):
            (distrito): division above municipality for agro-purposes
            (municipio):
            (sup_sembrada): sowed land (hc)
            (sup_cosech): harvested land (hc)
            (sup_siniest): lost land (hc)
            (prod): produce (tons)
            (rendim): yield (tons/hc)
            (mes):
            (anio):
            (moda_hidr): hydrological mode
                R: irrigated (riego)
                T: rainfall (temporal)
            (ciclo): cicle of agricultural year
                OI: Fall-Winter
                PV: Spring-Summer
            (cultivo): crop to monitor (same as arg)
    """
    # Define necessary dictionaries
    dict_moda = {1:"R", 2:"T"}

    dict_ciclo = {1: 'OI', 2: 'PV'}

    dict_cultivo = {'AJO': '700',
     'AJONJOLI': '800',
     'ALGODON HUESO': '1800',
     'AMARANTO': '2800',
     'ARROZ PALAY': '3300',
     'AVENA FORRAJERA EN VERDE': '3900',
     'AVENA GRANO': '4000',
     'BERENJENA': '4600',
     'BROCOLI': '5100',
     'CALABACITA': '5800',
     'CARTAMO': '6900',
     'CEBADA GRANO': '7300',
     'CEBOLLA': '7400',
     'CHILE VERDE': '11400',
     'COLIFLOR': '9000',
     'CRISANTEMO': '10130',
     'ELOTE': '12700',
     'FRESA': '14000',
     'FRIJOL': '14200',
     'GARBANZO': '14700',
     'GLADIOLA': '15400',
     'LECHUGA': '18500',
     'MAIZ FORRAJERO EN VERDE': '19800',
     'MAIZ GRANO': '19700',
     'MELON': '21200',
     'PAPA': '24400',
     'PEPINO': '24900',
     'SANDIA': '28700',
     'SORGO FORRAJERO EN VERDE': '29300',
     'SORGO GRANO': '29500',
     'SOYA': '29700',
     'TABACO': '30000',
     'TOMATE ROJO': '30800',
     'TOMATE VERDE': '31000',
     'TRIGO GRANO': '31500',
     'ZANAHORIA': '32900'}

    url = "http://infosiap.siap.gob.mx:8080/agricola_siap_gobmx/ResumenProducto.do"
    now = datetime.datetime.now()

    anios = list(range(2004,2017))
    meses = list(range(1,13))
    moda  = list(range(1,3))
    ciclo = list(range(1,3))
    results = []

    # Iterate over years, months, hidrologyc mode and cicle (otonio-invierno or primavera-verano)
    for year, month, moda, ciclo in product(anios, meses, moda, ciclo):

        #Test for dates that are yet to occur
        if month < now.month or year < now.year:
            print('Retrieving year={}, month={}, cicle={}, mode={}'.format(year,
                 month, dict_ciclo[ciclo], dict_moda[moda]))

            #Create payload to post
            payload = {'anio' :str(year), 'nivel' : '3', 'delegacion' : '0', 'municipio' : '-1',
            'mes' : str(month), 'moda' : str(moda), 'ciclo' : str(ciclo),
            'producto' : dict_cultivo[cultivo], 'tipoprograma' : '0',
            'consultar' : 'si', 'invitado' : 'false'}

            #Get post response
            try:
                response = requests.post(url, params=payload)
            except Exception:
                print('##### Connection error for year={}, month={}, cicle={}, mode={}'.format(year,
                 month, dict_ciclo[ciclo], dict_moda[moda]))
                response = False

            # Test for response
            if response:
                print('Successful response!')

                # Get information table from HTLM response
                soup = BeautifulSoup(response.text, 'html.parser')
                table = soup.find('table', attrs={'class': 'table table-responsive table-striped table-bordered'})

                # Iterate over table rows and extract information. Since the response lacks 'estado' for
                # a state's second and subsequent occurances, we add 'estado' with the
                # help of a boolean variable 'keep' and  a response variable 'keep_state'
                if table:
                    print(':D       Table found')
                    records = []
                    keep = True

                    # Iterate over rows
                    for row in table.findAll('tr'):
                        tds = row.find_all('td')

                        # Table format contains summaries of the data in the middle of the table;
                        # since they are not <td>, we can simply test for their absence
                        if tds:
                            test = "".join(tds[0].text.split())
                            if keep and test:
                                keep_state = tds[0]
                                keep = False
                            tds[0] = keep_state
                            records.append([' '.join(elem.text.lower().split()) for elem in tds])
                        else:
                            keep = True

                    #Add payload information to the table
                    for row in records:
                        row.extend([month, year, dict_moda[moda], dict_ciclo[ciclo], cultivo.lower()])

                    # Add successful response to the main table
                    results.extend(records)
                else:
                    print(':/       No table found')

    col_names = ['estado', 'distrito', 'municipio', 'sup_sembrada', 'sup_cosech',
     'sup_siniest', 'prod', 'rendim', 'mes', 'anio', 'moda_hidr', 'ciclo', 'cultivo']

    return pd.DataFrame(results, columns=col_names)


def get_cierre_produccion(cultivo = "MAIZ GRANO"):
    """
    Returns a Pandas with Avance Nacional de Siembra for crop 'cultivo'
    from SAGARPA-SIEP. The information is divided by municipality, and
    contains info for hydrolocial mode and cicle of agricultural year.

    Args:
        (cultivo): Crop to monitor. List available from dict_cultivo-

    Returns:
        Pandas dataframe with columns:
            (estado):
            (distrito): division above municipality for agro-purposes
            (municipio):
            (sup_sembrada): sowed land (hc)
            (sup_cosech): harvested land (hc)
            (sup_siniest): lost land (hc)
            (prod): produce (tons)
            (rendim): yield (tons/hc)
            (mes):
            (anio):
            (moda_hidr): hydrological mode
                R: irrigated (riego)
                T: rainfall (temporal)
            (ciclo): cicle of agricultural year
                OI: Fall-Winter
                PV: Spring-Summer
            (cultivo): crop to monitor (same as arg)
    """
    # Define necessary dictionaries
    dict_moda = {1:"R", 2:"T"}

    dict_ciclo = {1: 'OI', 2: 'PV'}

    dict_cultivo = {'AJO': '700',
     'AJONJOLI': '800',
     'ALGODON HUESO': '1800',
     'AMARANTO': '2800',
     'ARROZ PALAY': '3300',
     'AVENA FORRAJERA EN VERDE': '3900',
     'AVENA GRANO': '4000',
     'BERENJENA': '4600',
     'BROCOLI': '5100',
     'CALABACITA': '5800',
     'CARTAMO': '6900',
     'CEBADA GRANO': '7300',
     'CEBOLLA': '7400',
     'CHILE VERDE': '11400',
     'COLIFLOR': '9000',
     'CRISANTEMO': '10130',
     'ELOTE': '12700',
     'FRESA': '14000',
     'FRIJOL': '14200',
     'GARBANZO': '14700',
     'GLADIOLA': '15400',
     'LECHUGA': '18500',
     'MAIZ FORRAJERO EN VERDE': '19800',
     'MAIZ GRANO': '19700',
     'MELON': '21200',
     'PAPA': '24400',
     'PEPINO': '24900',
     'SANDIA': '28700',
     'SORGO FORRAJERO EN VERDE': '29300',
     'SORGO GRANO': '29500',
     'SOYA': '29700',
     'TABACO': '30000',
     'TOMATE ROJO': '30800',
     'TOMATE VERDE': '31000',
     'TRIGO GRANO': '31500',
     'ZANAHORIA': '32900'}
    dict_edos = {
    "1":  "11",
    "2":  "5",
    "3":  "5",
    "4":  "11",
    "5":  "38",
    "6":  "10",
    "7":  "118",
    "8":  "67",
    "9":  "16",
    "10":  "39",
    "11":  "46",
    "12":  "81",
    "13":  "84",
    "14":  "125",
    "15":  "125",
    "16":  "113",
    "17":  "33",
    "18":  "20",
    "19":  "51",
    "20":  "570",
    "21":  "217",
    "22":  "18",
    "23":  "9",
    "24":  "58",
    "25":  "18",
    "26":  "72",
    "27":  "17",
    "28":  "43",
    "29":  "60",
    "30":  "212",
    "31":  "106",
    "32":  "58"}

    url = "http://infosiap.siap.gob.mx/aagricola_siap_gb/ientidad/index.jsp"
    now = datetime.datetime.now()

    anios = list(range(2004,2017))
    meses = list(range(1,13))
    moda  = list(range(1,3))
    ciclo = list(range(1,3))
    municipios = list(range(1,500))
    estados = list(range(1,33))

    results = []

    # Iterate over years, months, hidrologyc mode and cicle (otonio-invierno or primavera-verano)
    for year, month, moda, ciclo, estado in product(anios, meses, moda, ciclo,estados):
        for municipio in list(range(1,int(dict_edos[str(estado)]))):

            #Test for dates that are yet to occur
            if month < now.month or year < now.year:
                print('Retrieving year={}, month={}, cicle={}, mode={},estado={},municipality={}'.format(year,
                     month, dict_ciclo[ciclo], dict_moda[moda],estado,municipio))

                #Create payload to post

                payload = {'pComponente' : '', 'pCveCiclo':str(ciclo), 'pAnio':str(year),
                'pCveEdo':str(estado),'pCveDDR':'0','pCveMpio':str(municipio),'pCveModalidad':str(moda),
                'pTpoCultivo':"0",'pOrden':'0'}


                #Get post response
                try:
                    response = requests.post(url, params=payload)
                except Exception:
                    print('##### Connection error for year={}, month={}, cicle={}, mode={}, state={}, mun={}'.format(year,
                     month, dict_ciclo[ciclo], dict_moda[moda],estado,municipio))
                    response = False

                # Test for response
                if response:
                    print('Successful response!')

                    # Get information table from HTLM response
                    soup = BeautifulSoup(response.text, 'html.parser')
                    #tree = html.fromstring(response.content)
                    table = soup.find('table', attrs={'class': 'table table-striped table-bordered'})

                    # help of a boolean variable 'keep' and  a response variable 'keep_state'
                    if table:
                        print(':D       Table found')
                        records = []
                        keep = True

                        edo = soup.find("div", {"class": "textoTablaTitulo"}).text
                        edo = re.findall("Estado ([A-Za-z]*)",edo)
                        mun = soup.find("div", {"class": "textoTablaSubtitulo2"}).text
                        mun = re.findall("Municipio: ([A-Za-z.]*.*)",mun)

                        # Iterate over rows
                        for row in table.findAll('tr'):
                            tds = row.findAll('td')

                            # Table format contains summaries of the data in the middle of the table;
                            # since they are not <td>, we can simply test for their absence
                            if tds:
                                test = "".join(tds[0].text.split())
                                if keep and test:
                                    keep_state = tds[0]
                                    keep = False
                                tds[0] = keep_state
                                records.append([' '.join(elem.text.lower().split()) for elem in tds])
                            else:
                                keep = True

                        #Add payload information to the table
                        for row in records:
                            row.extend([month, year, dict_moda[moda], dict_ciclo[ciclo],edo[0],mun[0]])

                        # Add successful response to the main table
                        results.extend(records)
                    else:
                        print(':/       No table found')


    col_names = ['n','A','B','cultivo','variedad', 'sup_sembrada_ha', 'sup_cosech_ha',
     'Sup_Siniestrada','Producción_Ton', 'Rendimiento_Ton_Ha', 'PMR_$_Ton', 'valor_produccion_k','mes', 
     'anio', 'moda_hidr', 'ciclo', 'estado', 'municipio']
    temp = pd.DataFrame(results, columns=col_names)
    temp.drop('B', 1,inplace=True)
    temp.drop('A', 1,inplace=True)
    return pd.DataFrame(results, columns=col_names)


def get_smn_data(year='2016',location="s3"):
    """Downloads shp files from CONAGUA Monitor de Sequía de Mexico (smn) into
        specified location

    Args:
        (year): give year wanted
        (location):
            local - it gets stored in /data/smn/
            s3 - it gets stored in our favorite s3 bucket

    """

    ftp = FTP('200.4.8.36')     # connect to host, default port
    ftp.login(conf["SMN_USER"],conf["SMN_PASSWORD"])
    ftp.cwd(year)
    filenames = ftp.nlst()
    #Load all files into folder
    #it should check if already exists
    for filename in filenames:
        local_filename = os.path.join('../data/SMN', filename)
        file = open(local_filename, 'wb')
        ftp.retrbinary('RETR '+ filename, file.write)

    ftp.quit()

def get_muni_description():
    dict_edos = {
        "1":  "11",
        "2":  "5",
        "3":  "5",
        "4":  "11",
        "5":  "38",
        "6":  "10",
        "7":  "118",
        "8":  "67",
        "9":  "16",
        "10":  "39",
        "11":  "46",
        "12":  "81",
        "13":  "84",
        "14":  "125",
        "15":  "125",
        "16":  "113",
        "17":  "33",
        "18":  "20",
        "19":  "51",
        "20":  "570",
        "21":  "217",
        "22":  "18",
        "23":  "9",
        "24":  "58",
        "25":  "18",
        "26":  "72",
        "27":  "17",
        "28":  "43",
        "29":  "60",
        "30":  "212",
        "31":  "106",
        "32":  "58"}
    estados = list(range(1,33))
    full_data = []
    # Iterate over years, months, hidrologyc mode and cicle (otonio-invierno or primavera-verano)
    for estado in estados:
        for municipio in list(range(1,1+int(dict_edos[str(estado)]))):
            cookies = {
                'jquery-ui-theme': 'Smoothness',
            }

            headers = {
                'Origin': 'http://www.snim.rami.gob.mx',
                'Accept-Encoding': 'gzip, deflate',
                'Accept-Language': 'es-MX,es;q=0.8,es-419;q=0.6,en;q=0.4',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.59 Safari/537.36',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept': '*/*',
                'Referer': 'http://www.snim.rami.gob.mx/',
                'X-Requested-With': 'XMLHttpRequest',
                'Connection': 'keep-alive',
            }

            data = {
              'edo': '{0}'.format(str(estado)),
              'mun': '{0}'.format(str(municipio)),
              'tipo': 'm',
              'reporte': 'dg2010'
            }


            try:
                response = requests.post('http://www.snim.rami.gob.mx/tbl_poblacion.php', headers=headers, cookies=cookies, data=data)
            except:
                pass

            #parse text
            soup = BeautifulSoup(response.text, 'html.parser').find('tbody')
            headers = [h.text.encode('utf-8') for h in soup.find_all("th")]
            values = [h.text.encode('utf-8') for h in soup.find_all("td")]
            json_data = {}

            json_data["cve_muni"] = str(estado).zfill(2) + str(municipio).zfill(3) 
            for header,value in zip(headers,values):
                json_data[header] = value 
            full_data.append(json_data)

            print("getting data from municipality n: " + str(estado).zfill(2) + str(municipio).zfill(3))

    return pn.DataFrame(full_data)
