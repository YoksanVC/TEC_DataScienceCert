docker build --tag proyecto_yoksanvarela .
docker network create my_network
docker run --name tarea3_db -e POSTGRES_PASSWORD=testPassword -p 5433:5432 --network my_network -d postgres
docker run --network my_network -p 8888:8888 -i -t proyecto_yoksanvarela /bin/bash