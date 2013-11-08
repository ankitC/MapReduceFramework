#!/bin/bash

bash scripts/package.sh

if [ ! -z $1 ] 
then 
    bash scripts/execute.sh $1 "${*:2}"
else
	echo "Sorry, must enter a valid program! (ex: Client, Registry, CapitalsServer)"
fi
