FROM python:3.6-alpine
MAINTAINER Francis Gassert <fgassert@wri.org>

# install core libraries
RUN apk update
RUN apk add git

# install application libraries
RUN pip install requests
RUN pip install -e git+https://github.com/fgassert/cartosql.py.git#egg=cartosql

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
