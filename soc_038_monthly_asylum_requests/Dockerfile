FROM python:3.6-alpine
MAINTAINER Nathan Suberi <nathan.suberi@wri.org>

# install core libraries
RUN apk update
RUN apk add git

# install application libraries
RUN pip install requests
RUN pip install -e git+https://github.com/fgassert/cartosql.py.git#egg=cartosql

# set name
ARG NAME=soc_038
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
