#!/bin/sh

#Change the NAME variable with the name of your script
NAME=$(basename $(pwd))
LOG=${LOG:-udp://localhost}

docker build -t $NAME --build-arg NAME=$NAME .
docker run -it -v data:/opt/$NAME/data \
           --log-driver=syslog \
           --log-opt syslog-address=$LOG \
           --log-opt tag=$NAME \
           --env-file .env \
           --rm $NAME \
           /bin/bash

           #python main.py



           #
