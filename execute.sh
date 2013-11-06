#!/bin/bash

if [ ! -z $1 ]
then
	DIFF=$(bash check-make.sh)
	if [ ! -n "$DIFF" ]; then
		echo "Makefile up-to-date"
	else
		echo "Makefile outdated, recompiling project..."
		bash clean.sh
		bash package.sh
	fi

	java -cp MapReduceFramework.jar $1 "${*:2}"
else
	echo "Sorry, must enter a valid program! (ex: Client, Registry, CapitalsServer)"
fi
