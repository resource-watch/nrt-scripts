FROM python:3.6
MAINTAINER Amelia Snyder <amelia.snyder@wri.org>
#Note this script was originally developed by Francis Gassert <fgassert@wri.org>

# install core libraries
RUN apt-get update && pip install -U pip

# install application libraries
RUN apt-get install -y gdal-bin libgdal-dev
RUN pip install oauth2client==4.1.3
RUN pip install -e git+https://github.com/resource-watch/eeUtil/tree/fix/upgrade_version_gee#egg=eeUtil

# set name
ARG NAME=nrt-script
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
