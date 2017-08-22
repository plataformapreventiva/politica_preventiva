SHELL := /bin/bash 
PROJECT_NAME:=$(shell cat ../.project-name)
DATA_DIRECTORY:=$(CURDIR)/../data
IMAGES = python-task r-task 

run:
	@echo $(WORKERS) >> temp.txt
	@LUIGI_CONFIG_PATH=./pipelines/luigi.cfg  $(PROJECT_NAME) --server 0.0.0.0 --port 8082 --workers $(WORKERS)

build: .built .built-local-storage

restart: .clean

.built:
	@for image in $(IMAGES) ; do \
		sudo docker build --tag ${PROJECT_NAME}/$$image tasks/$$image ; \
	done
	@touch .built

.built-local-storage:
	@if [ -z $(docker volume ls | grep $(PROJECT_NAME)_store) ]; then \
     sudo docker volume create --name $(PROJECT_NAME)_store --opt type=none --opt device=$(DATA_DIRECTORY) --opt o=bind ; \
   fi;
	@touch .built-local-storage

clean: clean_images clean_storage

clean_images:
	@for image in $(IMAGES) ; do \
		sudo docker rmi ${PROJECT_NAME}/$$image ; \
	done
	@[ -f .built ] && rm .built

clean_storage:
	@sudo docker volume rm $(PROJECT_NAME)_store
	@[ -f .built-local-storage ] && rm .built-local-storage
