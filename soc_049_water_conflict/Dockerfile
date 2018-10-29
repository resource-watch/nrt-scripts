FROM python:3.6
MAINTAINER Liz Saccoccia <elizabeth.saccoccia@wri.org>

# install core libraries
RUN apt-get update
RUN pip install --upgrade pip

# install application libraries
RUN pip install requests
RUN pip install -e git+https://github.com/fgassert/cartosql.py.git#egg=cartosql
RUN pip install import bs4
RUN pip install lxml

# set name
ARG NAME=soc_049_water_conflict
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
