all: build-foundation build-main

build-foundation:
	cd foundation && ../gradlew build publishToMavenLocal

build-main:
	./gradlew build

test: build-foundation
	./gradlew check


jib: build-foundation
	./gradlew jib -Pdocker=drpcorg

jib-docker: build-foundation
	./gradlew jibDockerBuild -Pdocker=drpcorg

disZip: build-foundation
	./gradlew disZip

clean:
	./gradlew clean;
	cd foundation && ../gradlew clean