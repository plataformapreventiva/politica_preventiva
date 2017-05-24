###############
# Transparencia Presupuestaria
###############
#sudo apt-get install python-dev python-pip python-setuptools build-essential
#pip install csvkit

#Todo() Descarga datos del presupuesto-mexico-2008-20164t-2017

echo "Descarga Presupuesto Desarrollo Social De 2008 a 2017"

# Descargar

mkdir -p $1
wget "https://s3.amazonaws.com/datastore.openspending.org/6018ab87076187018fc29c94a68a3cd2/presupuesto-mexico-2008-20164t-2017/data/presupuesto_mexico_2008_20164t_2017.csv" -O $2