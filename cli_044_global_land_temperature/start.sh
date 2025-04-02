#Change the NAME variable with the name of your script
NAME=$(basename $(pwd))

docker build -t $NAME --build-arg NAME=$NAME .
docker run  --network=host --log-driver=syslog --log-opt syslog-address=$LOG --log-opt tag=$NAME --env-file .env --rm $NAME python main.py
