FROM python:3.6
MAINTAINER Nathan Suberi <nathan.suberi@wri.org>
ARG NAME=cli_035
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

# Move and copy over requirements here to speed up dev - don't need to reinstall
# Python packages when only change is to application code
COPY contents/requirements.txt .
# install application libraries
RUN pip install numpy #Install this ahead of rasterio for appropriate applications
RUN pip install --upgrade pip && pip install -r requirements.txt
RUN pip install -e git+https://github.com/fgassert/cartosql.py.git#egg=cartosql

# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .
COPY time.cron .
COPY credentials.json .
VOLUME /opt/$NAME/data

RUN useradd $NAME
RUN chown -R $NAME:$NAME .
USER $NAME

# Launch script
CMD ["python", "main.py"]
