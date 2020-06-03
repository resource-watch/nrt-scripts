#!/bin/sh

#Change the NAME variable with the name of your script
NAME=$(basename $(pwd))
LOG=${LOG:-udp://localhost}

docker build -t $NAME --build-arg NAME=$NAME .
docker run --log-driver=syslog --log-opt syslog-address=$LOG --log-opt tag=$NAME --env-file .env --rm $NAME python update_worldbank_data_on_carto.py python update_worldbank_layers_on_rw.py
