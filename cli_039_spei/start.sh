#!/bin/sh

#Change the NAME variable with the name of your script
NAME=cli_039

docker build -t $NAME --build-arg NAME=$NAME .
docker run --log-driver=syslog --log-opt syslog-address=$LOG --log-opt tag=$NAME -v "$(pwd)"/data:/opt/$NAME/data --env-file .env --rm $NAME python main.py
