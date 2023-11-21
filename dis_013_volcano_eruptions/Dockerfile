FROM python:3.6
MAINTAINER Amelia Snyder <amelia.snyder@wri.org>
#Note this script was originally developed by Amelia Snyder <amelia.snyder@wri.org>

# set name
ARG NAME=nrt-script
ENV NAME ${NAME}


# install core libraries
RUN apt-get update
RUN apt-get install -y cron

# install application libraries
RUN pip install -e git+https://github.com/resource-watch/cartosql.py.git#egg=cartosql
RUN pip install -U pip \
    requests==2.22.0 \
    xmljson==0.2.0 \
    lxml==4.5.0

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
