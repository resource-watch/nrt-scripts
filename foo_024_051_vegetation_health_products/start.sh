#!/bin/sh

#Change the NAME variable with the name of your script
NAME=foo_024_051_vegetation_health
LOG=${LOG:-udp://localhost}

docker build -t $NAME --build-arg NAME=$NAME .
docker run --log-driver=syslog --log-opt syslog-address=$LOG --log-opt tag=$NAME --env-file .env --rm $NAME python main.py
