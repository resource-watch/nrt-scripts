# Automatic data toolchain

A collection of tools to automatically handle a variety of datasets.

### Develop

Each job should be in it's own folder with `time.cron` and `start.sh` files, as follows. A deploy script constructs a crontab with an entry for each folder with a `time.cron` and `start.sh`.

```
Repository
|
|-Script 1 folder
| |-time.cron  # single line containing crontab frequency
| |-start.sh   # shell script to start job in new container
| |-Dockefile  # container to build
| |-.env       # the global repo's .env file will be copied here
| +-...
|
|-Script 2 folder
| +-...
|
+-...
```

Standard `start.sh` builds and runs a docker container.

```
# name image
NAME=python-script

# build image
docker build -t $NAME --build-arg NAME=$NAME .

# run container and attach logger and environment variables
docker run --log-driver=syslog --log-opt syslog-address=$LOG --log-opt tag=$NAME -v $(pwd)/data:/opt/$NAME/data --env-file .env --rm $NAME
```

Standard `time.cron` should be one line without commands or breaks. E.g. run daily at 1:15am.

```
15 1 * * *
```

### Deploy

Run locally with http://github.com/fgassert/nrt-container .
