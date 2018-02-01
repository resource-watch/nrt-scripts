FROM python:3.6
MAINTAINER Nathan Suberi <nathan.suberi@wri.org>
ARG NAME=foo_003
ENV NAME ${NAME}

# install core libraries
RUN apt-get update
RUN apt-get install -y cron

# install application libraries
RUN pip install --upgrade pip && pip install fiona shapely python-dateutil requests
RUN pip install -e git+https://github.com/fgassert/cartosql.py.git#egg=cartosql

# Copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .
COPY time.cron .
VOLUME ./data

RUN groupadd -r $NAME && useradd -r -g $NAME $NAME
RUN chown $NAME:$NAME /opt/$NAME
USER $NAME

# Launch script
CMD ["python", "main.py"]
