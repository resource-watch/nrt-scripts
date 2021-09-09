FROM python:3.6
MAINTAINER Liz Saccoccia <elizabeth.saccoccia@wri.org>
#Note this script was originally developed by Liz Saccoccia <elizabeth.saccoccia@wri.org>

# install core libraries
RUN apt-get update
RUN pip install -U pip

# install application libraries
RUN apt-get install -y gdal-bin libgdal-dev
RUN pip install oauth2client==4.1.3
RUN pip install -e git+https://github.com/resource-watch/eeUtil#egg=eeUtil
RUN pip install bs4==0.0.1
RUN pip install python-dateutil==2.8.1

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
