FROM python:3.6
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
    rasterio \
    geopandas

# set name
ARG NAME=nrt-script
ENV NAME ${NAME}

# copy the application folder inside the container
WORKDIR /opt/$NAME/
RUN mkdir -p data
RUN mkdir -p mosaics
VOLUME /opt/$NAME/data
VOLUME /opt/$NAME/mosaics
COPY contents/ .

RUN useradd -r $NAME
RUN chown -R $NAME:$NAME /opt/$NAME
USER $NAME

CMD ["python", "main.py"]
