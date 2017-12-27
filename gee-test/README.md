# gee-test

Test NRT script for uploading to GEE.

# Run

Copy `.env.sample` to `.env` and enter account credentials. Copy GCS service account credential file to `credentials.json`.

`./start.sh` Build docker and run once.

# Modify

`start.sh` Edit script name / Docker image name.

`contents/` Copied into container.

`contents/src/__init__.py` Main application script.

`contents/src/eeUtil/` Utility module for interacting with GEE.

`time.cron` Edit cron freqency.

