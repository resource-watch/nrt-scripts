FROM python:3.6
MAINTAINER Nathan Suberi <nathan.suberi@wri.org>
ARG NAME=cli_039
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

RUN pip install -e git+https://github.com/fgassert/eeUtil#egg=eeUtil
RUN pip install numpy #Install this ahead of rasterio for appropriate applications
RUN pip install --upgrade pip && pip install \
    requests \
    rasterio \
    netCDF4 \
    python-dateutil


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
