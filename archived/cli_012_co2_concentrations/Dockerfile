FROM python:3.6
MAINTAINER Amelia Snyder <amelia.snyder@wri.org>
#Note this script was originally developed by Nathan Suberi <nathan.suberi@wri.org>

ARG NAME=cli_012
ENV NAME ${NAME}

# install core libraries
RUN apt-get update
RUN pip install -U pip

# install application libraries
RUN apt-get install -y \
  gdal-bin \
  libgdal-dev \
  libnetcdf-dev \
  netcdf-bin \
  cron \
  && apt-get clean

RUN pip install --upgrade pip && pip install numpy==1.18.1
RUN pip install rasterio==1.1.2
RUN pip install -e git+https://github.com/resource-watch/eeUtil#egg=eeUtil
RUN pip install oauth2client==4.1.3
RUN pip install -e git+https://github.com/resource-watch/cartosql.py.git#egg=cartosql

# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .
COPY time.cron .
VOLUME /opt/$NAME/data

RUN groupadd -r $NAME && useradd -r -g $NAME $NAME
RUN chown -R $NAME:$NAME .
USER $NAME

# Launch script
CMD ["python", "main.py"]
