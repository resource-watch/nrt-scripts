FROM python:3.6
MAINTAINER Nathan Suberi <nathan.suberi@wri.org>

# install core libraries
RUN apt-get update

# install application libraries
RUN apt-get install -y gdal-bin libgdal-dev
RUN pip install requests
RUN pip install -e git+https://github.com/fgassert/cartosql.py.git#egg=cartosql
RUN pip install fiona

# set name
ARG NAME=cli_037
ENV NAME ${NAME}

# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .
VOLUME ./data

# add user
RUN groupadd -r $NAME && useradd -r -g $NAME $NAME
RUN chown -R $NAME:$NAME /opt/$NAME/
USER $NAME

CMD ["python", "main.py"]
