FROM python:3.6
MAINTAINER Kristine Lister <kristine.lister@wri.org>

# install core libraries
RUN apt-get update
RUN pip install -U pip

# install application libraries
RUN apt-get install -y gdal-bin libgdal-dev
RUN pip install -e git+https://github.com/fgassert/eeUtil#egg=eeUtil
RUN pip install oauth2client
RUN pip install bs4
RUN pip install Cython
RUN pip install netCDF4

#install
RUN apt-get -y install python-gdal

# set name
ARG NAME=nrt-script
ENV NAME ${NAME}

# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .

RUN useradd -r $NAME
RUN chown -R $NAME:$NAME /opt/$NAME
USER $NAME

CMD ["python", "main.py"]
