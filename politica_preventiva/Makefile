SHELL := /bin/bash 
PROJECT_NAME:=$(shell cat ../.project-name)
DATA_DIRECTORY:=/data
IMAGES =  # docker-task

run:
	 @LUIGI_CONFIG_PATH=./pipelines/configs/luigi.cfg $(PROJECT_NAME) --server 0.0.0.0 --port 8082 --workers $(WORKERS) --level=$(level)

setup: .built .built-local-storage

restart: .clean

.built:
	@for image in $(IMAGES) ; do \
		 docker build --tag ${PROJECT_NAME}/task/$$image tasks/$$image ; \
	done
	@touch .built

.built-local-storage:
	@if [ -z $(docker volume ls | grep $(PROJECT_NAME)_store) ]; then \
      docker volume create --name $(PROJECT_NAME)_store --opt type=none --opt device=$(DATA_DIRECTORY) --opt o=bind ; \
   fi;
	@touch .built-local-storage

clean: clean_images clean_storage

clean_images:
	@for image in $(IMAGES) ; do \
		 docker rmi ${PROJECT_NAME}/task/$$image ; \
	done
	@[ -f .built ] && rm .built

clean_storage:
	@ docker volume rm $(PROJECT_NAME)_store
	@[ -f .built-local-storage ] && rm .built-local-storage
