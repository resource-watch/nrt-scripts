# NRT Test

Dockerized Python3 cron script to download data and insert into CARTO table.

### Usage

Requires Docker.

`.env.sample` Copy to `.env` and add keys.

`./start.sh` Build container and run script once.

`./run-cron.sh` Build container with cron inside!.

### Modify

`Dockerfile` Define requirements here.

`time.cron` Set crontab frequency here.

`contents\` Copied into container.

`contents\src\__init__.py` Contains core logic for downloading, formatting, and uploading data.

`contents\src\carto.py` Utility library for interacting with CARTO.


