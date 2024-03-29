#Change the NAME variable with the name of your script
NAME=bio_007b
LOG=${LOG:-udp://localhost}

docker build -t $NAME --no-cache --build-arg NAME=$NAME .

# -m limits the memory usuage of the docker to prevent memory error
docker run \
    -m 1700m \
    --log-driver=syslog \
    --log-opt syslog-address=$LOG \
    --log-opt tag=$NAME \
    --env-file .env \
    --rm $NAME \
    python main.py


  #  /bin/bash