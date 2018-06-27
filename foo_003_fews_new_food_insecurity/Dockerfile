FROM python:3.6
MAINTAINER Nathan Suberi <nathan.suberi@wri.org>
ARG NAME=foo_003
ENV NAME ${NAME}

# install core libraries
RUN apt-get update
RUN pip install --upgrade pip

# install application libraries
RUN pip install fiona shapely python-dateutil requests
RUN pip install -e git+https://github.com/fgassert/cartosql.py.git#egg=cartosql

# Copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .
COPY time.cron .

RUN groupadd -r $NAME && useradd -r -g $NAME $NAME
RUN chown -R $NAME:$NAME /opt/$NAME
USER $NAME
VOLUME ./data

# Launch script
CMD ["python", "main.py"]
