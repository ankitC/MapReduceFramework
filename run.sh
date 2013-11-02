#!/bin/bash

bash package.sh

if [ ! -z $1 ] 
then 
    bash execute.sh $1
else
	echo "Sorry, must enter a valid program! (ex: Master, Worker)"
fi