FROM python:3.6
MAINTAINER Amelia Snyder <amelia.snyder@wri.org>

# set name
ARG NAME=nrt-script
ENV NAME ${NAME}

# install core libraries
RUN apt-get update

# install application libraries
RUN apt-get install -y gdal-bin libgdal-dev
RUN pip install requests==2.22.0
RUN pip install oauth2client==4.1.3
RUN pip install -e git+https://github.com/resource-watch/cartosql.py.git#egg=cartosql
RUN pip install fiona==1.8.13

# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .
VOLUME ./data

RUN groupadd -r $NAME && useradd -r -g $NAME $NAME
RUN chown -R $NAME:$NAME /opt/$NAME/
USER $NAME

CMD ["python", "main.py"]
