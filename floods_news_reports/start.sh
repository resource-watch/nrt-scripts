#Change the NAME variable with the name of your script
NAME=my-first-script
docker build -t $NAME --build-arg NAME=$NAME .
docker run -v $(pwd)/data:/opt/$NAME/data --rm $NAME