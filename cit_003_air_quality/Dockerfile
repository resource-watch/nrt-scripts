FROM python:3.6
MAINTAINER Francis Gassert <fgassert@wri.org>

# set name
ARG NAME=nrt-script
ENV NAME ${NAME}

# install core libraries
RUN apt-get update

# install application libraries
RUN pip install requests

# Copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .
COPY time.cron .
VOLUME ./data

CMD ["python", "main.py"]
