# MLSystemDesignHW2

В корень проекта необходимо добавить .env с параметрами вида:

AIRFLOW_UID=1114266
MINIO_ACCESS_KEY="minioaccesskey"
MINIO_SECRET_KEY="miniosecretkey"
BUCKET_NAME="movielens2"

Такой же .env надо продублировать в src.

docker compose build

docker-compose up -d

FastAPI запускаем вручную через сваггер. Для примера можно использовать id 145169

Airflow даг инициируем вручную.
