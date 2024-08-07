FROM python:3.9
LABEL maintainer="Amelia Snyder <amelia.snyder@wri.org>"
#Note this script was originally developed by Nathan Suberi <nathan.suberi@wri.org>

# set name
ARG NAME=nrt-script
ENV NAME=${NAME}

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

RUN pip install oauth2client==4.1.3
RUN pip install google-api-python-client==2.135.0
RUN pip install -e git+https://github.com/resource-watch/eeUtil#egg=eeUtil
RUN pip install numpy==1.26.4 #Install this ahead of rasterio for appropriate applications
RUN pip install --upgrade pip && pip install \
    requests==2.32.3 \
    rasterio==1.3.10 \
    netCDF4==1.7.1 \
    python-dateutil==2.9.0


# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .
COPY time.cron .

RUN groupadd -r $NAME && useradd -r -g $NAME $NAME
RUN chown -R $NAME:$NAME .
USER $NAME

VOLUME /opt/$NAME/data

# Launch script
CMD ["python", "main.py"]
