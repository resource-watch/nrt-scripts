FROM python:3.6-alpine
MAINTAINER Amelia Snyder <amelia.snyder@wri.org>
#Note this script was originally developed by Amelia Snyder <amelia.snyder@wri.org>

# install core libraries
RUN apk update
RUN apk add git

# install application libraries
RUN pip install requests==2.22.0
RUN pip install -e git+https://github.com/resource-watch/cartosql.py.git#egg=cartosql

# set name
ARG NAME=nrt-script
ENV NAME ${NAME}

# copy the application folder inside the container
RUN mkdir -p /opt/$NAME/data
WORKDIR /opt/$NAME/
COPY contents .
VOLUME ./data

RUN useradd $NAME
RUN chown $NAME:$NAME .
USER $NAME

CMD ["python", "main.py"]
