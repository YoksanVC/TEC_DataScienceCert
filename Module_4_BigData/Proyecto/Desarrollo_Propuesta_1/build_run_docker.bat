docker build --tag proyecto_yoksanvarela .
docker run --name proyecto-db -e POSTGRES_PASSWORD=testPassword -p 5433:5432 -d postgres
docker run -p 8888:8888 -i -t proyecto_yoksanvarela /bin/bash