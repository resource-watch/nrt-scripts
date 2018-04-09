FROM python:3-stretch
MAINTAINER Nathan Suberi <nathan.suberi@wri.org>

# install core libraries
RUN apt-get update
RUN pip install -U pip

# install application libraries
RUN apt-get install -y \
    gdal-bin \
    libgdal-dev

RUN pip install -e git+https://github.com/fgassert/eeUtil#egg=eeUtil
RUN pip install numpy
RUN pip install \
    netCDF4 \
    rasterio

# set name
ARG NAME=nrt-script
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
