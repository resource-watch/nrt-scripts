FROM python:3.9
LABEL maintainer="Amelia Snyder <amelia.snyder@wri.org>"
#Note this script was originally developed by Nathan Suberi <nathan.suberi@wri.org>

# set name
ARG NAME=nrt-script
ENV NAME=${NAME}

# install core libraries
RUN apt-get update
RUN apt-get install -y cron

# install application libraries
RUN pip install -e git+https://github.com/resource-watch/cartosql.py.git#egg=cartosql
RUN pip install --upgrade pip && pip install \
    requests==2.22.0 \
    xmljson==0.2.0 \
    lxml==4.5.0 \
    python-dateutil==2.8.0

# Copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .
COPY time.cron .
VOLUME ./data

# Set user for container security
RUN useradd $NAME
RUN chown -R $NAME:$NAME .
USER $NAME

# Launch script
CMD ["python", "main.py"]
