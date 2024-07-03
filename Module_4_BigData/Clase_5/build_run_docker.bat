docker build --tag clase5_full_docker .
docker run --name bigdata-db -e POSTGRES_PASSWORD=testPassword -p 5433:5432 -d postgres
docker run -p 8888:8888 -i -t clase5_full_docker /bin/bash