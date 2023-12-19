all: build-foundation build-main

build-foundation:
	cd foundation && ../gradlew build publishToMavenLocal

build-main:
	./gradlew build

test: build-foundation
	./gradlew check

local-docker: Dockerfile
	docker build -t drpc-dshackle .

jib: build-foundation local-docker
	./gradlew jib -Pdocker=drpcorg

jib-docker: build-foundation local-docker
	./gradlew jibDockerBuild -Pdocker=drpcorg

distZip: build-foundation
	./gradlew disZip

clean:
	./gradlew clean;
	cd foundation && ../gradlew clean
