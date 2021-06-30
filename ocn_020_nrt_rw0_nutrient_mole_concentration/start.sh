#!/bin/sh

#Change the NAME variable with the name of your script
NAME=ocn_020_nrt_rw0_nutrient_mole_concentration
LOG=${LOG:-udp://localhost}

docker build -t $NAME --build-arg NAME=$NAME .
docker run --log-driver=syslog --log-opt syslog-address=$LOG --log-opt tag=$NAME --env-file .env --rm $NAME python main.py