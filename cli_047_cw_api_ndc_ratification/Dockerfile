FROM python:3.6
MAINTAINER Nathan Suberi <nathan.suberi@wri.org>

# install core libraries
RUN apt-get update
RUN apt-get install git

# install application libraries
RUN pip install --upgrade pip
RUN pip install pandas
RUN pip install requests \
    cartoframes

RUN pip install -e git+https://github.com/fgassert/cartosql.py.git#egg=cartosql

# set name
ARG NAME=climatewatchapi
ENV NAME ${NAME}

# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents/ .
VOLUME ./data

# Set user for container security
RUN useradd $NAME
RUN chown -R $NAME:$NAME .
USER $NAME

CMD ["python", "main.py"]
