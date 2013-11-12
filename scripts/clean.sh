#!/bin/bash

NAME=$(bash scripts/get-project-name.sh)

find src/ -iname \*.class -exec rm '{}' ';'
rm $NAME.jar
find . -name "worker*" | grep ":" | xargs rm -r
