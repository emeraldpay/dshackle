FROM eclipse-temurin:21

RUN apt-get update -y && apt-get install -y libcurl4-openssl-dev libcjson-dev
