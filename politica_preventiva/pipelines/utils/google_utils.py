# coding: utf-8
# Run as: luigid & PYTHONPATH='.' python pipeline.py  RunPipelines --workers 3
# pip install GoogleMaps  
import os
from os.path import join, dirname
from datetime import datetime
from dotenv import load_dotenv
import logging
import psycopg2
import luigi
from luigi import configuration
import luigi.s3
from luigi.s3 import S3Target, S3Client
from dotenv import load_dotenv,find_dotenv
import luigi

from googleplaces import GooglePlaces, types, lang
import googlemaps
from  googlemaps import distance_matrix

## Variables de ambiente
load_dotenv(find_dotenv())

PLACES_API_KEY =  os.environ.get('PLACES_API_KEY')

def info_to_google_services(latitud=19.361013,longitud=-99.149722,keyword='Hospital'):

	now = datetime(2017, 1, 1, 13, 33)
	gmaps = googlemaps.Client(key=PLACES_API_KEY)
	google_places = GooglePlaces(PLACES_API_KEY)
	try:
		query_result = google_places.nearby_search(lat_lng={'lat':latitud, 'lng':longitud}, keyword=keyword,rankby='distance')
		top_services={}

		place=query_result.places[0]
		place.get_details()
		top_services["formatted_address_{0}".format(keyword)]=place.formatted_address
		#start = gmaps.reverse_geocode((latitud,longitud))[0]["formatted_address"]
		destination=top_services["formatted_address_{0}".format(keyword)]

		directions_result = gmaps.directions(origin={'lat':latitud,'lng':longitud},destination={'lat':float(place.geo_location["lat"]),'lng':float(place.geo_location["lng"])},mode="driving",departure_time=now,units="metric")
		top_services["driving_time_{0}".format(keyword)] = directions_result[0]["legs"][0]["duration"]["text"]
		top_services["driving_dist_{0}".format(keyword)] = directions_result[0]["legs"][0]["distance"]["text"]		

		directions_result = gmaps.directions(origin={'lat':latitud,'lng':longitud},destination={'lat':float(place.geo_location["lat"]),'lng':float(place.geo_location["lng"])},mode="walking",departure_time=now,units="metric")
		top_services["walking_time_{0}".format(keyword)] = directions_result[0]["legs"][0]["duration"]["text"]
		top_services["walking_dist_{0}".format(keyword)] = directions_result[0]["legs"][0]["distance"]["text"]		

		top_services["name_{0}".format(keyword)] = place.name
		top_services["website_{0}".format(keyword)] = place.website
		top_services["local_phone_number_{0}".format(keyword)] = place.local_phone_number

	except:
		top_services = {'driving_dist_{0}'.format(keyword): "Establecimiento no Localizado",
			 'driving_time_{0}'.format(keyword): "Establecimiento no Localizado",
			 'formatted_address_{0}'.format(keyword): "Establecimiento no Localizado",
			 'local_phone_number_{0}'.format(keyword): "Establecimiento no Localizado",
			 'name_{0}'.format(keyword): "Establecimiento no Localizado",
			 'walking_dist_{0}'.format(keyword): "Establecimiento no Localizado",
			 'walking_time_{0}'.format(keyword): "Establecimiento no Localizado",
			 'website_{0}'.format(keyword): "Establecimiento no Localizado"}

	return top_services