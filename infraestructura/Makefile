.PHONY: start stop status logs restart nuke

########################################
##            Variables               ##
########################################
SHELL := /bin/bash 
PROJECT_NAME:= politica_preventiva
DATA_DIRECTORY:= /home/data
DOCKER_REGISTRY:= "localhost:5000"
IMAGES = python luigi-server-c 
#base python r java luigi-server luigi-worker spark-client hadoop-client
IMAGE_VERSION:= 0.1

########################################
##             Control                ##
########################################

start: .running

setup: create up


.running:
	@sudo cp ../${PROJECT_NAME}/.env ./.env
	@docker-compose --project-name ${PROJECT_NAME} up -d
	@touch .running

create: .images_built .network_built .data_built  

.images_built:
	@for image in $(IMAGES) ; do \
	 	 docker build --tag dpa/$$image:$(IMAGE_VERSION) --tag dpa/$$image:latest $$image ; \
	 done
	@touch .images_built

.data_built:
	echo ${DATA_DIRECTORY}
	[ -d ${DATA_DIRECTORY} ] && docker volume create --name ${PROJECT_NAME}_store --opt type=none --opt device=${DATA_DIRECTORY} --opt o=bind
	@touch .data_built

.network_built:
	@docker network create ${PROJECT_NAME}_net
	@touch .network_built

.infrastructure_built:
	@docker-compose --project-name ${PROJECT_NAME} build
	@touch .infrastructure_built

register_images:
	@echo TODO 

stop:
	@docker-compose --project-name ${PROJECT_NAME} stop
	@rm -rf .running || true

status:
	@docker-compose --project-name ${PROJECT_NAME} ps

logs:
	@docker-compose --project-name ${PROJECT_NAME} logs

restart: clean start status

clean: stop  clean_images clean_containers clean_data clean_network 

nuke: clean

clean_containers:
	@set +e
	@docker-compose --project-name ${PROJECT_NAME} down --volumes --remove-orphans
	@docker-compose --project-name ${PROJECT_NAME} rm -f
	@rm -rf .infrastructure_built || true
	@set -e
clean_images:
	@set +e
	@docker images -a | grep "dpa" | awk  '{print $3}' | xargs docker rmi -f
	#@docker images -a | grep "\-c" | awk  '{print $3}' | xargs docker rmi -f
	#@docker rmi -f $( docker images | grep '^dpa' | awk '{print $3}' )
	@rm -rf .images_built || true
	@set -e
clean_data:
	@set +e
	@docker volume rm ${PROJECT_NAME}_store
	@rm -rf .data_built || true
	@set -e
clean_network: stop
	@set +e
	@docker network rm ${PROJECT_NAME}_net
	@rm -rf .network_built || true
	@set -e
