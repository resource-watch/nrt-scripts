#!/bin/sh

#Change the NAME variable with the name of your script
NAME=cit_004_city_aq
LOG=${LOG:-udp://localhost}

docker build -t $NAME --no-cache --build-arg NAME=$NAME .
docker run --log-driver=syslog --log-opt syslog-address=$LOG --log-opt tag=$NAME --env-file .env --rm $NAME python main.py
