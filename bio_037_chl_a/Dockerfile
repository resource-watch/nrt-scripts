FROM python:3-stretch
MAINTAINER Kristine Lister <kristine.lister@wri.org>
#Note this script was originally developed by Kristine Lister <kristine.lister@wri.org>

# install core libraries
RUN apt-get update
RUN pip install -U pip

# install application libraries
RUN apt-get install -y gdal-bin libgdal-dev
RUN apt-get -y install python-gdal
RUN pip install oauth2client==4.1.3
RUN pip install google-api-python-client==1.12.8
RUN pip install -e git+https://github.com/resource-watch/eeUtil#egg=eeUtil
RUN pip install netCDF4==1.5.3


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
