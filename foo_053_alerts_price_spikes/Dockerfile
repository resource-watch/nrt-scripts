FROM python:3.6-alpine
MAINTAINER Amelia Snyder <amelia.snyder@wri.org>
#Note this script was originally developed by Nathan Suberi <nathan.suberi@wri.org> and Amelia Snyder <amelia.snyder@wri.org>

# install core libraries
RUN echo "http://mirror.leaseweb.com/alpine/edge/testing/" >> /etc/apk/repositories
RUN apk add --no-cache geos-dev
RUN apk update && apk upgrade && \
   apk add --no-cache --update bash git openssl-dev build-base alpine-sdk \
   libffi-dev gcc python3-dev musl-dev
# install application libraries
RUN pip install --upgrade pip
RUN pip install requests==2.22.0
RUN pip install -e git+https://github.com/resource-watch/cartosql.py.git#egg=cartosql
RUN pip install shapely==1.6.4
RUN pip install numpy==1.18.1
RUN pip install pandas==0.25.3
RUN pip install backoff

# set name
ARG NAME=nrt-script
ENV NAME ${NAME}

# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .
VOLUME ./data

RUN adduser -D $NAME
RUN chown $NAME:$NAME .
USER $NAME

CMD ["python", "main.py"]
