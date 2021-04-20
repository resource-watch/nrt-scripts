#Change the NAME variable with the name of your script
NAME=bio_007_rw2_wdpa 
LOG=${LOG:-udp://localhost}

docker build -t $NAME --no-cache --build-arg NAME=$NAME .
# --m is used to limit the memory usage of this script to prevent memory errors
docker run \
    -m 1600m \
    --log-driver=syslog \
    --log-opt syslog-address=$LOG \
    --log-opt tag=$NAME \
    --env-file .env \
    --rm $NAME \
    python main.py


  #  /bin/bash