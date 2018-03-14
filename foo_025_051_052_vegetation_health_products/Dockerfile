FROM python:3.6
MAINTAINER Nathan Suberi <nathan.suberi@wri.org>

# install core libraries
RUN apt-get update
RUN pip install -U pip

# install application libraries
RUN apt-get install -y \
    gdal-bin \
    libgdal-dev \
    libnetcdf-dev \
    netcdf-bin \
    && apt-get clean

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

CMD ["python", "main.py"]
