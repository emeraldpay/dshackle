FROM eclipse-temurin:20

RUN apt-get update -y && apt-get install -y libcurl4-openssl-dev libjansson-dev
