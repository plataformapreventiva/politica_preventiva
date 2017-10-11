#!/bin/bash

# Descarga de datos - declaratoria de emergencia
wget -q -O-  'https://drive.google.com/uc?export=download&id=0B5wc002kIlphM1phbTQwNWFVczg' |\
	 csvformat -D '|' > $3 
