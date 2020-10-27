FROM python:3.6
MAINTAINER Amelia Snyder <amelia.snyder@wri.org>
#Note this script was originally developed by Amelia Snyder <amelia.snyder@wri.org>

# install core libraries
RUN apt-get update && pip install -U pip

# install application libraries
RUN apt-get install -y gdal-bin libgdal-dev
RUN pip install oauth2client==4.1.3
RUN pip install -e git+https://github.com/resource-watch/eeUtil#egg=eeUtil
RUN pip install rasterio==1.1.6
RUN pip install boto3==1.9.66
RUN pip install netCDF4==1.4.2
RUN pip install earthengine-api==0.1.234

# set name
ARG NAME=loc_mcaqf
ENV NAME ${NAME}

# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .

RUN useradd -r $NAME
RUN chown -R $NAME:$NAME /opt/$NAME
VOLUME /opt/$NAME/data
USER $NAME

CMD ["python", "main.py"]
