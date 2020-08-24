#!/usr/bin/env sh

set -e
cd ../..
export SPRING_APPLICATION_JSON='{"configPath": "testing/dshackle/dshackle.yaml"}'
./gradlew run