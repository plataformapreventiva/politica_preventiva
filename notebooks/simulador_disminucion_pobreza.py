"""
Falta modificar para que nos de las variables que queremos.
Pensar qué información quieres obtener.
"""


import pandas as pn
from itertools import product

## Input
# dado un estado
estado = "05"
dism_deseo = .1
#"i_privacion", 
vector = ["i_privacion","ic_rezedu","ic_asalud","ic_segsoc","ic_sbv","ic_cv","ic_ali"]


def programs(w_vector,estado,dism_deseo):
	# Function
	# state filter
	coneval_estatal = pn.read_csv("../data/coneval/programas_calculo/R_2014/Base final/pobreza_14.csv",dtype={'ent': str}) 
	coneval_estatal = coneval_estatal.query('ent == "{}" and pobreza_e>0'.format(estado))

	# get decrease goal [people] 
	disminucion_meta = int((sum(coneval_estatal["factor_hog"][(coneval_estatal["pobreza_e"]==1)]) * dism_deseo).round())

	# sort by the importance vector
	coneval_estatal = coneval_estatal.sort(w_vector,ascending = [1,0,0,0,0,0,0])
	coneval_estatal.reset_index(inplace=True)

	# get minimum people necesary
	i = 0
	while sum(coneval_estatal["factor_hog"][0:i]) < disminucion_meta:
		i += i + 1

	folios_disminucion = list(coneval_estatal["folioviv"][0:i])

	base_disminucion = coneval_estatal[coneval_estatal["folioviv"].isin(folios_disminucion)]
	total_personas = base_disminucion["factor_hog"].sum() 

	base_disminucion["programa"] = ""
	base_disminucion["programa_2"] = ""
	base_disminucion["programa_3"] = ""

	w_vector.pop(0)
	for carencia in w_vector:

		####################
		# 3 carencias
		base_disminucion["programa"][(base_disminucion["i_privacion"] == 3) & 
		(base_disminucion["programa"] == "") & (base_disminucion[carencia] == 1)] = carencia

		####################
		# 4 carencias
		base_disminucion["programa"][(base_disminucion["i_privacion"] == 4) & 
		(base_disminucion["programa"] == "") & (base_disminucion[carencia] == 1)] = carencia

		base_disminucion["programa_2"][(base_disminucion["i_privacion"] == 4) & 
		(base_disminucion["programa"] != "") & (base_disminucion["programa"] != carencia)&
		(base_disminucion[carencia] == 1)] = carencia

		####################
		# 5 carencias
		base_disminucion["programa"][(base_disminucion["i_privacion"] == 5) & 
		(base_disminucion["programa"] == "") & (base_disminucion[carencia] == 1)] = carencia

		base_disminucion["programa_2"][(base_disminucion["i_privacion"] == 5) & 
		(base_disminucion["programa"] != "") & (base_disminucion["programa"] != carencia)&
		(base_disminucion[carencia] == 1)] = carencia

		base_disminucion["programa_2"][(base_disminucion["i_privacion"] == 5) & 
		(base_disminucion["programa"] != "") & (base_disminucion["programa"] != carencia)&
		(base_disminucion["programa_2"] != "") & (base_disminucion["programa_2"] != carencia)&
		(base_disminucion[carencia] == 1)] = carencia

		####################
		# 6 carencias
		#base_disminucion["programa"][(base_disminucion["i_privacion"] == 6) & 
		#(base_disminucion["programa"] == "") & (base_disminucion[carencia] == 1)] = carencia

		#base_disminucion["programa_2"][(base_disminucion["i_privacion"] == 6) & 
		#(base_disminucion["programa"] != "") & (base_disminucion["programa"] != carencia)&
		#(base_disminucion[carencia] == 1)] = carencia

		#base_disminucion["programa_2"][(base_disminucion["i_privacion"] == 6) & 
		#(base_disminucion["programa"] != "") & (base_disminucion["programa"] != carencia)&
		#(base_disminucion["programa_2"] != "") & (base_disminucion["programa_2"] != carencia)&
		#(base_disminucion[carencia] == 1)] = carencia

		#base_disminucion["programa_3"][(base_disminucion["i_privacion"] == 6) & 
		#(base_disminucion["programa"] != "") & (base_disminucion["programa"] != carencia)&
		#(base_disminucion["programa_2"] != "") & (base_disminucion["programa_2"] != carencia)&
		#(base_disminucion["programa_3"] != "") & (base_disminucion["programa_3"] != carencia)&
		#(base_disminucion[carencia] == 1)] = carencia
	return 

test = programs(w_vector=["i_privacion","ic_rezedu","ic_asalud","ic_segsoc","ic_sbv","ic_cv","ic_ali"],estado="01",dism_deseo=.1)