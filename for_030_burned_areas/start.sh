#!/bin/sh

#Change the NAME variable with the name of your script
NAME=for_030_burned_areas

docker build -t $NAME --build-arg NAME=$NAME .
docker run --log-driver=syslog --log-opt syslog-address=$LOG --log-opt tag=$NAME --env-file .env --rm $NAME python main.py
