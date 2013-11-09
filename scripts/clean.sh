#!/bin/bash

find src/ -iname \*.class -exec rm '{}' ';'
rm MapReduceFramework.jar
find . -name "worker*" | grep ":" | xargs rm -r
