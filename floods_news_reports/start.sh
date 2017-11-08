#Change the NAME variable with the name of your script
NAME=floods_reports
docker build -t $NAME --build-arg NAME=$NAME .
docker run --log-opt syslog-address=$LOG --log-opt tag=$NAME -v $(pwd)/data:/opt/$NAME/data --env-file .env --rm $NAME
