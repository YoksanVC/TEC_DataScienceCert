docker build --tag tarea3_yoksanvarela .
docker run --name tarea3-db -e POSTGRES_PASSWORD=testPassword -p 5433:5432 -d postgres
docker run -p 8888:8888 -i -t tarea3_yoksanvarela /bin/bash