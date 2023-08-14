FROM python:3.9
MAINTAINER Weiqi Zhou <weiqi.zhou@wri.org>
#Note this script was originally developed by Nathan Suberi <nathan.suberi@wri.org> and maintained by Amelia Snyder

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
RUN pip install google-api-python-client==2.96.0
RUN pip install -e git+https://github.com/resource-watch/eeUtil#egg=eeUtil
RUN pip install python-dateutil==2.8.2
RUN pip install LMIPy

ARG NAME=cli_005
ENV NAME ${NAME}

# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .

RUN useradd -r $NAME
RUN chown -R $NAME:$NAME /opt/$NAME
VOLUME /opt/$NAME/data
USER $NAME

# Launch script
CMD ["python", "main.py"]
