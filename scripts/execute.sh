#!/bin/bash

NAME=$(bash scripts/get-project-name.sh)

if [ ! -z $1 ]
then
	DIFF=$(bash scripts/check-make.sh)
	if [ ! -n "$DIFF" ]; then
		echo "Makefile up-to-date"
	else
		echo "Makefile outdated, recompiling project..."
		bash scripts/clean.sh
		bash scripts/update-make.sh
		bash scripts/package.sh
	fi

	MAIN=$(find src -name $1".java" | sed {s/src\\///g} | sed {s/\.java//g})
	java -cp $NAME.jar $MAIN "${*:2}"
else
	echo "Sorry, must enter a valid program! (ex: Client, Registry, CapitalsServer)"
fi
