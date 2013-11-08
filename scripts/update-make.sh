#!/bin/bash

cd ../src/

lines=$(find . -name \*.java | sed 's/\(.*java\)/\t\1 \\/g')
lines=$(echo "$lines" | sed 's/[\/&]/\\&/g')

cd ../

lines=`echo "$lines" | tr '\n' " " | tr '\r' " "`

sed 's/\(CLASSES = \\\)/\1\n'"$lines"'/' make-template > temp
sed 's/\(\.java \\\) /\1\n/g' temp > src/Makefile

sed -i 's/\nRMCLASSES/RMCLASSES/g' src/Makefile

rm temp
