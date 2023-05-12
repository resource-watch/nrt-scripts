FROM python:3.7
MAINTAINER Weiqi Zhou <weiqi.zhou@wri.org>
#Note this script was originally developed by Nathan Suberi <nathan.suberi@wri.org>

# install core libraries
RUN apt-get update
RUN pip install -U pip

# install application libraries
RUN apt-get install -y gdal-bin libgdal-dev
RUN pip install -e git+https://github.com/resource-watch/eeUtil#egg=eeUtil
RUN pip install numpy==1.18.1
RUN pip install \
    netCDF4==1.5.3 \
    rasterio==1.1.2

# set name
ARG NAME=foo_024_051
ENV NAME ${NAME}

# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .

RUN groupadd -r $NAME && useradd -r -g $NAME $NAME
RUN chown -R $NAME:$NAME .
USER $NAME

VOLUME /opt/$NAME/data

CMD ["python", "main.py"]
