FROM python:3.6
MAINTAINER Weiqi Zhou <weiqi.zhou@wri.org>
#Note this script was originally developed by Nathan Suberi <nathan.suberi@wri.org>

# set name
ARG NAME=nrt-script
ENV NAME ${NAME}

# install core libraries
RUN apt-get update
RUN apt-get install -y cron

# install application libraries
RUN pip install --upgrade pip && pip install \
    requests==2.22.0
RUN pip install cartoframes
RUN pip install numpy==1.18.1
RUN pip install geopandas==0.9.0

# Copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .
COPY time.cron .

# Set user for container security
RUN useradd $NAME
RUN chown -R $NAME:$NAME .
#USER $NAME

# Launch script
CMD ["python", "main.py"]
