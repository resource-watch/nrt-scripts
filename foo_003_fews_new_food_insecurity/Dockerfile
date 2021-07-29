FROM python:3.6
MAINTAINER Amelia Snyder <amelia.snyder@wri.org>
#Note this script was originally developed by Nathan Suberi <nathan.suberi@wri.org>

ARG NAME=foo_003
ENV NAME ${NAME}

# install core libraries
RUN apt-get update
RUN pip install --upgrade pip

# install application libraries
RUN pip install bs4==0.0.1
RUN pip install fiona==1.8.13 shapely==1.6.4 python-dateutil==2.8.0 requests==2.22.0
RUN pip install -e git+https://github.com/resource-watch/cartosql.py.git#egg=cartosql

# Copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .
COPY time.cron .

RUN groupadd -r $NAME && useradd -r -g $NAME $NAME
RUN chown -R $NAME:$NAME /opt/$NAME
USER $NAME
VOLUME ./data

# Launch script
CMD ["python", "main.py"]
