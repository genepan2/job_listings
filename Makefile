include .env

ifeq ($(OS),Windows_NT)
	SLEEP_COMMAND = timeout 10
else
	SLEEP_COMMAND = sleep 10
endif

.PHONY: airflow spark hive scale-spark minio superset down run init copy doris dl_doris postgres_dw pgadmin restart-spark-w stop start

stop:
	docker-compose stop

start:
	docker-compose start

restart-spark-w:
	docker compose restart spark-worker

copy:
	./copy-constants.sh

init:
	docker-compose up airflow-init

init_minio:
	docker-compose exec minio bash ./init/minio-init.sh

run: minio spark airflow postgres_dw pgadmin

pull:
	docker-compose pull

up:
	docker-compose up -d

logs:
	docker-compose logs -f

ps:
	docker-compose ps

down:
	docker-compose down -v

dl_jars:
	sh download_jarfiles.sh

clear-images:
	docker image prune --filter="dangling=true"


minio:
	docker-compose up -d minio

airflow:
	docker-compose up -d airflow-webserver
	docker-compose up -d airflow-scheduler
	docker-compose up -d airflow-worker

spark:
	docker-compose up -d spark-master
	${SLEEP_COMMAND}
	docker-compose up -d spark-worker

scale-spark:
	docker-compose scale spark-worker=3

hive:
	docker-compose up -d mariadb
	${SLEEP_COMMAND}
	docker-compose up -d hive

doris:
	docker-compose up -d doris-fe
	${SLEEP_COMMAND}
	docker-compose up -d doris-be

dl_doris:
	sh download_doris.sh

postgres_dw:
	docker-compose up -d postgres

pgadmin:
	docker-compose up -d pgadmin

# presto-cluster:
# 	docker-compose up -d presto presto-worker

# superset:
# 	docker-compose up -d superset

# presto-cli:
# 	docker-compose exec presto \
# 	presto --server localhost:8888 --catalog hive --schema default

# to-minio:
# 	sudo cp -r .storage/data/* .storage/minio/datalake/

exec-mariadb:
	docker-compose exec mariadb mysql -u root -p -h localhost

# run-spark:
# 	docker-compose exec airflow \
# 	spark-submit --master spark://spark-master:7077 \
# 	--deploy-mode client --driver-memory 2g --num-executors 2 \
# 	--packages io.delta:delta-core_2.12:1.0.0 --py-files dags/utils/common.py \
# 	--jars dags/jars/aws-java-sdk-1.11.534.jar,dags/jars/aws-java-sdk-bundle-1.11.874.jar,dags/jars/delta-core_2.12-1.0.0.jar,dags/jars/hadoop-aws-3.2.0.jar,dags/jars/mariadb-java-client-2.7.4.jar \
# 	dags/etl/spark_initial.py