#!/bin/sh

NAME=$(basename $(pwd))
LOG=${LOG:-udp://localhost}

docker build -t $NAME --build-arg NAME=$NAME .
docker run --log-driver=syslog --log-opt syslog-address=$LOG \
        --log-opt tag=$NAME \
        --env-file .env \
        -e CARTO_KEY_WRIRW='65efbcc00e9591f334fcad66b3d9515228a7deef' \
        -e CARTO_KEY='8ab811877d79ed8238945c1c2524313daf4d6625' \
        --rm $NAME python main.py
