# Makefile for managing Docker containers and services

# PHONY targets for avoiding conflicts with files
.PHONY: all run stop start down pull up logs ps clear-images airflow airflow-init spark spark-worker-restart spark-scale postgres pgadmin mariadb-exec minio minio-init dl-jars

# Standard target
all: run

# Combined operations for everyday use
run: minio airflow spark postgres pgadmin

# Container Management
stop:
	docker-compose stop

start:
	docker-compose start

down:
	docker-compose down -v

pull:
	docker-compose pull

up:
	docker-compose up -d

logs:
	docker-compose logs -f

ps:
	docker-compose ps

clear-images:
	docker image prune --filter="dangling=true"

# Airflow Operations
airflow: airflow-init
	docker-compose up -d airflow-webserver
	docker-compose up -d airflow-scheduler
	docker-compose up -d airflow-worker

airflow-init:
	docker-compose up airflow-init

# Spark Operations
spark:
	docker-compose up -d spark-master
	sleep 5  # Wait until the master is ready
	docker-compose up -d spark-worker

spark-worker-restart:
	docker-compose restart spark-worker

spark-scale:
	docker-compose up --scale spark-worker=3 -d

# Database Management
postgres:
	docker-compose up -d postgres-dw

pgadmin:
	docker-compose up -d pgadmin

mariadb-exec:
	docker-compose exec mariadb mysql -u root -p -h localhost

# Minio Operations
minio:
	docker-compose up -d minio

minio-init:
	docker-compose exec minio bash ./init/minio-init.sh

# Auxiliary operations
dl-jars:
	mkdir -p ./airflow/dags/jars
	sh ./airflow/download_jar_files.sh