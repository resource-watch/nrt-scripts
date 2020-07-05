# Automatic data toolchain

A collection of tools to automatically handle a variety of datasets.

## Dependencies

Dependencies on other Microservices:
- [Dataset](https://github.com/resource-watch/dataset/)
- [Layer](https://github.com/resource-watch/layer)

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

**Run**

To run this script on your own computer: 
  1. This script is run in a Docker container. Before you can run this script, make sure you have downloaded [Docker](https://www.docker.com/).
    <br><br>
  2. You must also have a [Google Cloud Storage](https://cloud.google.com/) account/project set up.
    <br><br>
  3. [Clone the nrt-scripts repository](https://help.github.com/en/github/creating-cloning-and-archiving-repositories/cloning-a-repository) to your computer.
    <br><br>
  4. Change the environmental variable sample file in this script's root folder (`.env.sample`) to `.env`, and replace the field after each variable with the indicated Google Cloud Storage service account credentials. Alternatively, you can create one master `.env` file on your computer with these credentials and create a symbolic link to the master copy of your .env file using the following command:
    <br>`ln -s /home/path/to/.env .`
    <br><br>
  5. Navigate to the root folder for this script (`bio_005_coral_bleaching`) in the command line, and run this script:
    <br>`./start.sh`
    
If you want this script to run automatically on your computer, you must set up a crontab. Alternatively, you can run the `./start.sh` command each time you want to update the data.
