docker compose exec -it --workdir="//opt/bitnami/spark" spark-master python jobs/load_staging.py \
    --user=root \
    --password=root \
    --host=data-warehouse \
    --port=5432 \
    --db=dana

docker compose run --workdir="//usr/app/dbt/dana_etl" --rm dbt deps
docker compose run --workdir="//usr/app/dbt/dana_etl" --rm dbt debug
docker compose run --workdir="//usr/app/dbt/dana_etl" --rm dbt build