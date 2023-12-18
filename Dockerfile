FROM gradle:latest as builder
COPY . /dshackle
WORKDIR /dshackle
RUN gradle distZip
RUN unzip /dshackle/build/distributions/dshackle-*-SNAPSHOT.zip -d /dshackle/app

FROM openjdk:latest
RUN microdnf install findutils
COPY --from=builder /dshackle/app/dshackle-*-SNAPSHOT /app
ENTRYPOINT [ "/app/bin/dshackle" ]